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

from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from plugins.hooks import RedisHookDecodeResponses

if TYPE_CHECKING:
    from airflow.utils.context import Context


class RedisPublishOperator(BaseOperator):
    """
    Publish a message to Redis.

    NOTE: this is like the standard Redis provider operator, but
    returns the value of the PUBLISH command.

    :param channel: redis channel to which the message is published (templated)
    :param message: the message to publish (templated)
    :param redis_conn_id: redis connection to use
    """

    template_fields: Sequence[str] = ("channel", "message")

    def __init__(self, *, channel: str, message: str, redis_conn_id: str = "redis_default", **kwargs) -> None:

        super().__init__(**kwargs)
        self.redis_conn_id = redis_conn_id
        self.channel = channel
        self.message = message

    def execute(self, context: Context) -> None:
        """
        Publish the message to Redis channel.

        :param context: the context object
        """
        redis_hook = RedisHookDecodeResponses(redis_conn_id=self.redis_conn_id)
        self.log.info("Sending message %s to Redis on channel %s", self.message, self.channel)
        result = redis_hook.get_conn().publish(channel=self.channel, message=self.message)
        self.log.info("Result of publishing %s", result)
        return result


class RedisLockOperator(BaseOperator):
    """
    Attempt to acquire a Redis lock.

    This operator is used by a DAG to acquire a lock for an arbitrary resource
    corresponding to `lock_key`. The lock owner is indicated by `lock_value`.
    The core assumption is that the DAG supplies `lock_value` by joining it's
    dag_id and run_id; since Airflow enforces uniqueness on these fields, we can
    be certain that only one process can hold the lock.

    :param lock_key: redis key for the lock (templated)
    :param lock_value: the value for the lock (templated)
    :param release: if True, release the lock
    :param redis_conn_id: redis connection to use
    """

    template_fields: Sequence[str] = ("lock_key", "lock_value")

    def __init__(
        self,
        *,
        lock_key: str,
        lock_value: str = "",
        release: bool = False,
        redis_conn_id: str = "redis_default",
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.redis_conn_id = redis_conn_id
        self.lock_key = lock_key
        self.lock_value = lock_value
        self.release = release

    def execute(self, context: Context) -> None:
        """Modify a Redis lock.

        If self.lock_value equals the the value stored under self.lock_key in
        Redis, then the instance is considered to hold the lock. If `release` is
        truthy and the instance owns the lock, then the lock will be released.

        If `release` is falsey, then this will attempt to acquire the lock by
        setting the value self.lock_key to self.lock_value in Redis. Finally,
        the value under self.lock_key in Redis is checked; if it equals
        self.lock_value, then this returns True to indicate we hold the lock.

        :param context: the context object
        """
        redis_hook = RedisHookDecodeResponses(redis_conn_id=self.redis_conn_id)
        rdb = redis_hook.get_conn()

        if self.release:
            result = rdb.get(self.lock_key)
            if result != self.lock_value:
                self.log.info("Lock release skipped: %s holds lock %s.", result, self.lock_key)
                return
            self.log.info(
                "Lock release success: %s released lock %s", self.lock_value, self.lock_key)
            return rdb.delete(self.lock_key)

        rdb.set(self.lock_key, self.lock_value, nx=True, ex=120)
        result = rdb.get(self.lock_key)
        if result != self.lock_value:
            self.log.info("%s holds lock instead of %s, returning None", result, self.lock_value)
            return
        self.log.info("Lock acquisition success: %s holds lock %s", result, self.lock_key)
        return True
