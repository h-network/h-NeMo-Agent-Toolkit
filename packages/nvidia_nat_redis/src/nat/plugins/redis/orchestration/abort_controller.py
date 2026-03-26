# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""External abort controller backed by Redis Pub/Sub."""

from __future__ import annotations

import asyncio
import json
import logging
import time

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


class AbortController:
    """Listens for (or sends) abort signals on per-task Redis Pub/Sub channels.

    The orchestration layer creates one listener per active task.  External callers
    (UI, API, another agent) publish to the same channel to request cancellation.
    """

    def __init__(self, client: aioredis.Redis, key_prefix: str) -> None:
        """Initialize the abort controller.

        Args:
            client: Async Redis client.
            key_prefix: Namespace prefix for abort channel keys.
        """
        self._client = client
        self._key_prefix = key_prefix

    def abort_channel(self, task_id: str) -> str:
        """Return the Pub/Sub channel name for *task_id*."""
        return f"{self._key_prefix}:abort:{task_id}"

    async def listen_for_abort(self, task_id: str, abort_event: asyncio.Event) -> None:
        """Subscribe to the abort channel and set *abort_event* on first message.

        Designed to run as a background ``asyncio.Task``.  The caller is
        responsible for cancelling this task when execution completes.
        """
        pubsub = self._client.pubsub()
        try:
            await pubsub.subscribe(self.abort_channel(task_id))
            async for message in pubsub.listen():
                if message["type"] == "message":
                    logger.info("Abort signal received for task %s: %s", task_id, message["data"])
                    abort_event.set()
                    break
        except asyncio.CancelledError:
            pass
        finally:
            await pubsub.unsubscribe(self.abort_channel(task_id))
            await pubsub.close()

    async def send_abort(self, task_id: str, reason: str = "External abort") -> int:
        """Publish an abort message to the task's channel.

        Returns:
            The number of subscribers that received the message.
            ``0`` means the task was not actively listening (already finished or never started).
        """
        payload = json.dumps({"reason": reason, "timestamp": time.time()})
        count = await self._client.publish(self.abort_channel(task_id), payload)
        logger.info("Abort sent for task %s (receivers=%d)", task_id, count)
        return count
