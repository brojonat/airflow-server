#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import json
import time
import logging

import pendulum
from airflow.decorators import dag, task
from airflow.utils import timezone
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.types import DagRunType
from airflow.api.common.trigger_dag import trigger_dag
from airflow.models import DagRun
from airflow.models.baseoperator import chain
from airflow.models.param import Param

from plugins.operators import (
    RedisPublishOperator,
    RedisLockOperator,
)
from plugins.hooks import (
    PRAWHook,
    RedisHookDecodeResponses,
)

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["reddit"],
    params={
        "submission_id": Param("14fn2e0", "A visionary's post.")
    }
)
def monitor_reddit_submission():
    """This DAG will long poll Reddit for a submission's data and push the
    results onto a Redis PubSub channel.

    """
    logger = logging.getLogger("airflow.task")

    # acquire the lock to poll for this submission_id
    acquire_lock_res = RedisLockOperator(
        task_id="acquire_lock_task",
        redis_conn_id="airflow_redis",
        lock_key="bayesian:poll-lock:{{ params['submission_id'] }}",
        lock_value="({{ dag.dag_id }}, {{ run_id }})",
    )

    # always release the lock after all tasks are finished
    release_lock_res = RedisLockOperator(
        task_id="release_lock_task",
        redis_conn_id="airflow_redis",
        lock_key="bayesian:poll-lock:{{ params['submission_id'] }}",
        lock_value="({{ dag.dag_id }}, {{ run_id }})",
        release=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # exit early if we don't have the lock
    @task.branch(task_id="lock_branch")
    def lock_branch(**context):
        """Exit or continue depending on lock_task result."""
        lock_value = context["task_instance"].xcom_pull(task_ids="acquire_lock_task")
        logger.info("Lock value data: %s", lock_value)
        if not lock_value:
            return None
        return ["poll_pub_task"]
    lock_branch_res = lock_branch()

    # poll for data
    @task(task_id="poll_pub_task")
    def poll_pub_task(**context):
        """Poll Reddit for submission_id data and return the result."""
        sid = context["params"]["submission_id"]
        logger.info("Polling reddit for %s", sid)
        rc = PRAWHook(reddit_conn_id="airflow_reddit").get_conn()
        sub = rc.submission(sid)
        sub.comments.replace_more(limit=0)
        comment_data = {}
        for c in sub.comments.list():
            comment_data[c.id] = {"ups": c.ups, "downs": c.downs, "body": c.body}
        data = json.dumps({sid: comment_data})
        rdb = RedisHookDecodeResponses(redis_conn_id="airflow_redis").get_conn()
        return rdb.publish(sid, data)
    poll_pub_task_res = poll_pub_task()

    # retrigger self
    @task(task_id="trigger_self")
    def trigger_self(**context):
        """Rerun the DAG."""
        parsed_execution_date = timezone.utcnow()
        trigger_dag(
            dag_id=context["dag"].dag_id,
            run_id=DagRun.generate_run_id(DagRunType.MANUAL, parsed_execution_date),
            conf=context["params"],
            execution_date=parsed_execution_date,
            replace_microseconds=False,
        )
    trigger_self_res = trigger_self()

    # exit early if no subscribers
    @task.branch(task_id="sub_count_branch")
    def sub_count_branch(sub_count, **context):
        """Exit or rerun depending on PUBLISH result."""
        logger.info("Pub count data: %s", sub_count)
        if sub_count < 1:
            return ["release_lock_task"]
        return ["sleep"]
    sub_count_branch_res = sub_count_branch(poll_pub_task_res)

    # sleep to prevent spamming
    @task(task_id="sleep")
    def sleep():
        """Sleep."""
        time.sleep(3)
    sleep_res = sleep()

    # main DAG dependency structure; all tasks are sequentially dependent
    chain(
        acquire_lock_res,
        lock_branch_res,
        poll_pub_task_res,
        sub_count_branch_res,
        sleep_res,
        release_lock_res,
        trigger_self_res,
    )
    # block retriggering on sleep succeeding
    trigger_self_res.set_upstream(sleep_res)

# invoke DAGs
monitor_reddit_submission()
