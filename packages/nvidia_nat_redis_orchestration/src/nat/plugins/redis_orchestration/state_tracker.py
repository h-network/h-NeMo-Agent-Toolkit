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
"""Task state tracker backed by Redis strings and Pub/Sub."""

from __future__ import annotations

import dataclasses
import json
import logging
import time

import redis.asyncio as aioredis

from .models import TERMINAL_STATES
from .models import TaskState
from .models import TaskStateInfo

logger = logging.getLogger(__name__)


class TaskStateTracker:
    """Manages task lifecycle state in Redis.

    Each task's state is stored as a JSON string with a TTL.
    State transitions are published to a shared Pub/Sub channel
    so external observers can react in real time.
    """

    def __init__(
        self,
        client: aioredis.Redis,
        key_prefix: str,
        state_ttl: int,
        instance_id: str,
    ) -> None:
        self._client = client
        self._key_prefix = key_prefix
        self._state_ttl = state_ttl
        self._instance_id = instance_id

    # ---- Key / channel helpers ----

    def state_key(self, task_id: str) -> str:
        return f"{self._key_prefix}:state:{task_id}"

    @property
    def state_channel(self) -> str:
        return f"{self._key_prefix}:state_events"

    # ---- State transitions ----

    async def set_running(self, task_id: str, function_name: str) -> None:
        """Mark a task as running. Publishes a state-transition event."""
        now = time.time()
        info = TaskStateInfo(
            task_id=task_id,
            function_name=function_name,
            state=TaskState.RUNNING,
            instance_id=self._instance_id,
            started_at=now,
            updated_at=now,
        )
        await self._write_state(info)

    async def set_completed(self, task_id: str) -> None:
        """Transition a running task to completed (idempotent for terminal states)."""
        await self._transition(task_id, TaskState.COMPLETED)

    async def set_failed(self, task_id: str, error: str) -> None:
        """Transition a running task to failed."""
        await self._transition(task_id, TaskState.FAILED, error=error)

    async def set_timed_out(self, task_id: str) -> None:
        """Transition a running task to timed_out."""
        await self._transition(task_id, TaskState.TIMED_OUT)

    async def set_aborted(self, task_id: str) -> None:
        """Transition a running task to aborted."""
        await self._transition(task_id, TaskState.ABORTED)

    # ---- Queries ----

    async def get_state(self, task_id: str) -> TaskStateInfo | None:
        """Retrieve the current state for *task_id*, or ``None`` if expired / missing."""
        raw = await self._client.get(self.state_key(task_id))
        if raw is None:
            return None
        return self._deserialize(raw)

    # ---- Internals ----

    async def _transition(self, task_id: str, new_state: TaskState, *, error: str | None = None) -> None:
        """Conditionally move a task to *new_state*.

        If the task is already in a terminal state the call is a no-op,
        preventing races between abort and completion.
        """
        current = await self.get_state(task_id)
        if current is None:
            logger.warning("Cannot transition task %s to %s: state key missing", task_id, new_state)
            return
        if current.state in TERMINAL_STATES:
            logger.debug("Task %s already in terminal state %s; ignoring transition to %s",
                         task_id, current.state, new_state)
            return

        updated = TaskStateInfo(
            task_id=current.task_id,
            function_name=current.function_name,
            state=new_state,
            instance_id=current.instance_id,
            started_at=current.started_at,
            updated_at=time.time(),
            error=error,
        )
        await self._write_state(updated)

    async def _write_state(self, info: TaskStateInfo) -> None:
        """Persist state to Redis and publish a transition event."""
        payload = self._serialize(info)
        await self._client.set(self.state_key(info.task_id), payload, ex=self._state_ttl)
        event = json.dumps({
            "task_id": info.task_id,
            "state": info.state.value,
            "function_name": info.function_name,
            "timestamp": info.updated_at,
        })
        await self._client.publish(self.state_channel, event)
        logger.debug("Task %s -> %s", info.task_id, info.state.value)

    # ---- Serialization ----

    @staticmethod
    def _serialize(info: TaskStateInfo) -> str:
        return json.dumps(dataclasses.asdict(info))

    @staticmethod
    def _deserialize(raw: str) -> TaskStateInfo:
        data = json.loads(raw)
        data["state"] = TaskState(data["state"])
        return TaskStateInfo(**data)
