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
import datetime

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
    start_time = pendulum.now()
    max_rps = 0.5 # requests per second. 0.5 is high, but fine while testing
    soonest_next_poll = start_time + datetime.timedelta(seconds=1./max_rps)

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
                "comment_karma": getattr(a, "comment_karma", None),
                "created_utc": getattr(a, "created_utc", None),
                "has_verified_email": getattr(a, "has_verified_email", None),
                "id": getattr(a, "id", None),
                "is_employee": getattr(a, "is_employee", None),
                "link_karma": getattr(a, "link_karma", None),
                "name": getattr(a, "name", None),
            }

        def submission_to_dict(s):
            try:
                poll_data = poll_data_to_dict(s.poll_data)
            except AttributeError:
                poll_data = None
            return {
                "author": author_to_dict(s.author),
                "created_utc": getattr(s, "created_utc", None),
                "distinguished": getattr(s, "distinguished", None),
                "edited": getattr(s, "edited", None),
                "id": getattr(s, "id", None),
                "is_self": getattr(s, "is_self", None),
                "locked": getattr(s, "locked", None),
                "name": getattr(s, "name", None),
                "num_comments": getattr(s, "num_comments", None),
                "over_18": getattr(s, "over_18", None),
                "permalink": getattr(s, "permalink", None),
                "poll_data": poll_data,
                "score": getattr(s, "score", None),
                "selftext": getattr(s, "selftext", None),
                "title": getattr(s, "title", None),
                "upvote_ration": getattr(s, "upvote_ratio", None),
            }

        def poll_data_to_dict(p):
            return {
                "options": poll_options_to_dict(p.options),
                "total_vote_count": getattr(p, "total_vote_count", None),
                "voting_end_timestamp": getattr(p, "voting_end_timestamp", None),
            }

        def poll_options_to_dict(o):
            return {
                "id": getattr(o, "id", None),
                "text": getattr(o, "text", None),
                "vote_count": getattr(o, "vote_count", None),
            }

        def subreddit_to_dict(s):
             return {
                "created_utc": getattr(s, "created_utc", None),
                "description": getattr(s, "description", None),
                "display_name": getattr(s, "display_name", None),
                "id": getattr(s, "id", None),
                "name": getattr(s, "name", None),
                "over18": getattr(s, "over18", None), # lol, doesn't match the submission convention
                "subscribers": getattr(s, "subscribers", None),
             }

        def comment_to_dict(c):
            data = {}
            # data["author_id"] = getattr(c.author, "id", None) if c.author else None
            data["body"] = getattr(c, "body", None)
            data["created_utc"] = getattr(c, "created_utc", None)
            data["distinguished"] = getattr(c, "distinguished", None)
            data["edited"] = getattr(c, "edited", None)
            data["id"] = getattr(c, "id", None)
            data["is_submitter"] = getattr(c, "is_submitter", None)
            data["link_id"] = getattr(c, "link_id", None)
            data["parent_id"] = getattr(c, "parent_id", None)
            data["permalink"] = getattr(c, "permalink", None)
            data["score"] = getattr(c, "score", None)
            data["stickied"] = getattr(c, "stickied", None)
            # data["submission_id"] = getattr(c.submission, "id", None) if c.submission else None
            # data["subreddit"] = getattr(c.subreddit, "id", None) if c.subreddit else None
            data["replies"] = [] # skip this since it requires more network requests
            return data

        comment_data = {}
        author_data = {}
        submission_data = {}
        subreddit_data = {}

        comments = sub.comments.list()
        if (
            comments[0].submission and
            getattr(comments[0].submission, "id", None) and
            comments[0].submission.id not in submission_data
        ):
            submission_data[comments[0].submission.id] = submission_to_dict(comments[0].submission)
        if (
            comments[0].subreddit and
            getattr(comments[0].subreddit, "id", None) and
            comments[0].subreddit.id not in subreddit_data
        ):
            subreddit_data[comments[0].subreddit.id] = subreddit_to_dict(comments[0].subreddit)

        for c in comments:
            comment_data[c.id] = comment_to_dict(c)
            # don't do this as it creates a ton of network requests
            # if (
            #     c.author and
            #     getattr(c.author, "id", None) and
            #     c.author.id not in author_data
            # ):
            #     author_data[c.author.id] = author_to_dict(c.author)

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
        sleep_time = (pendulum.now() - soonest_next_poll).seconds
        if sleep_time > 0:
            time.sleep(sleep_time)
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
