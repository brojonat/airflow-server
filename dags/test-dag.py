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
        def author_to_dict(a):
            return {
                "comment_karma": a.comment_karma,
                "created_utc": a.created_utc,
                "has_verified_email": a.has_verified_email,
                "id": a.id,
                "is_employee": a.is_employee,
                "link_karma": a.link_karma,
                "name": a.name,
            }

        def submission_to_dict(s):
            try:
                poll_data = poll_data_to_dict(s.poll_data)
            except AttributeError:
                poll_data = None
            return {
                "author": author_to_dict(s.author),
                "created_utc": s.created_utc,
                "distinguished": s.distinguished,
                "edited": s.edited,
                "id": s.id,
                "is_self": s.is_self,
                "locked": s.locked,
                "name": s.name,
                "num_comments": s.num_comments,
                "over_18": s.over_18,
                "permalink": s.permalink,
                "poll_data": poll_data,
                "score": s.score,
                "selftext": s.selftext,
                "title": s.title,
                "upvote_ration": s.upvote_ratio,
            }

        def poll_data_to_dict(p):
            return {
                "options": poll_options_to_dict(p.options),
                "total_vote_count": p.total_vote_count,
                "voting_end_timestamp": p.voting_end_timestamp,
            }

        def poll_options_to_dict(o):
            return {
                "id": o.id,
                "text": o.text,
                "vote_count": o.vote_count,
            }

        def subreddit_to_dict(s):
             return {
                "created_utc": s.created_utc,
                "description": s.description,
                "display_name": s.display_name,
                "id": s.id,
                "name": s.name,
                "over18": s.over18, # lol, doesn't match the submission convention
                "subscribers": s.subscribers,
             }

        def comment_to_dict(c):
            data = {}
            data["author_id"] = c.author.id
            data["body"] = c.body
            data["created_utc"] = c.created_utc
            data["distinguished"] = c.distinguished
            data["edited"] = c.edited
            data["id"] = c.id
            data["is_submitter"] = c.is_submitter
            data["link_id"] = c.link_id
            data["parent_id"] = c.parent_id
            data["permalink"] = c.permalink
            data["score"] = c.score
            data["stickied"] = c.stickied
            data["submission_id"] = c.submission.id
            data["subreddit"] = c.subreddit.id
            data["replies"] = [] # skip this since it requires more network requests
            return data

        comment_data = {}
        author_data = {}
        submission_data = {}
        subreddit_data = {}
        for c in sub.comments.list():
            comment_data[c.id] = comment_to_dict(c)
            if comment_data["author_id"] not in author_data:
                author_data[c.author.id] = author_to_dict(c.author)
            if comment_data["submission_id"] not in submission_data:
                submission_data[c.submission.id] = submission_to_dict(c.submission)
            if comment_data["subreddit_id"] not in subreddit_data:
                subreddit_data[c.subreddit.id] = subreddit_to_dict(c.subreddit)
        data = json.dumps({
            sid: {
                "comment_data": comment_data,
                "author_data": author_data,
                "submission_data": submission_data,
                "subreddit_data": subreddit_data
            }
        })
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
