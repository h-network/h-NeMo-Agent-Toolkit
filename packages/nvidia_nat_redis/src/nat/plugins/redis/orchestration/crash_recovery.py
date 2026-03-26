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
"""SCAN-based crash recovery for orphaned running tasks."""

from __future__ import annotations

import logging

import redis.asyncio as aioredis

from .models import TaskState
from .state_tracker import TaskStateTracker

logger = logging.getLogger(__name__)


class CrashRecovery:
    """Detects and recovers orphaned tasks left in ``running`` state after a crash.

    On startup, scans all state keys and transitions any that are still
    ``running`` to ``failed`` with an explanatory error message.
    """

    def __init__(self, client: aioredis.Redis, key_prefix: str, state_ttl: int, instance_id: str) -> None:
        """Initialize crash recovery.

        Args:
            client: Async Redis client.
            key_prefix: Namespace prefix for state keys.
            state_ttl: TTL in seconds for state entries.
            instance_id: Identifier for this recovery instance.
        """
        self._client = client
        self._key_prefix = key_prefix
        self._tracker = TaskStateTracker(client, key_prefix, state_ttl, instance_id)
        self._instance_id = instance_id

    async def recover_orphaned_tasks(self) -> list[str]:
        """Scan for running tasks and mark them as failed.

        Returns:
            List of task IDs that were recovered.
        """
        recovered: list[str] = []
        pattern = f"{self._key_prefix}:state:*"
        cursor: int | bytes = 0

        while True:
            cursor, keys = await self._client.scan(cursor=cursor, match=pattern, count=100)
            for key in keys:
                key_str = key if isinstance(key, str) else key.decode()
                raw = await self._client.get(key_str)
                if raw is None:
                    continue

                info = self._tracker.deserialize(raw)
                if info.state == TaskState.RUNNING:
                    error_msg = f"Orphaned: process crashed (recovered by instance {self._instance_id})"
                    await self._tracker.set_failed(info.task_id, error_msg)
                    recovered.append(info.task_id)
                    logger.warning("Recovered orphaned task %s (function=%s)", info.task_id, info.function_name)

            if cursor == 0:
                break

        if recovered:
            logger.info("Crash recovery completed: %d orphaned task(s) recovered", len(recovered))
        else:
            logger.debug("Crash recovery completed: no orphaned tasks found")

        return recovered
