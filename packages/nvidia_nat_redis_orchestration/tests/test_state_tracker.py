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
"""Tests for TaskStateTracker."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from nat.plugins.redis_orchestration.models import TaskState
from nat.plugins.redis_orchestration.state_tracker import TaskStateTracker


@pytest.fixture(name="mock_redis")
def fixture_mock_redis():
    client = AsyncMock()
    client.set = AsyncMock()
    client.get = AsyncMock(return_value=None)
    client.publish = AsyncMock(return_value=1)
    return client


@pytest.fixture(name="tracker")
def fixture_tracker(mock_redis):
    return TaskStateTracker(client=mock_redis, key_prefix="test", state_ttl=600, instance_id="inst-1")


class TestStateKey:

    def test_state_key_format(self, tracker):
        assert tracker.state_key("abc123") == "test:state:abc123"

    def test_state_channel(self, tracker):
        assert tracker.state_channel == "test:state_events"


class TestSetRunning:

    async def test_set_running_writes_state(self, tracker, mock_redis):
        await tracker.set_running("task-1", "my_function")

        mock_redis.set.assert_called_once()
        call_args = mock_redis.set.call_args
        assert call_args[0][0] == "test:state:task-1"
        payload = json.loads(call_args[0][1])
        assert payload["task_id"] == "task-1"
        assert payload["function_name"] == "my_function"
        assert payload["state"] == "running"
        assert payload["instance_id"] == "inst-1"
        assert call_args[1]["ex"] == 600

    async def test_set_running_publishes_event(self, tracker, mock_redis):
        await tracker.set_running("task-1", "my_function")

        mock_redis.publish.assert_called_once()
        channel, event_str = mock_redis.publish.call_args[0]
        assert channel == "test:state_events"
        event = json.loads(event_str)
        assert event["task_id"] == "task-1"
        assert event["state"] == "running"


class TestTransitions:

    async def _setup_running_state(self, mock_redis):
        """Configure mock_redis.get to return a valid running state."""
        running_payload = json.dumps({
            "task_id": "task-1",
            "function_name": "fn",
            "state": "running",
            "instance_id": "inst-1",
            "started_at": 1000.0,
            "updated_at": 1000.0,
            "error": None,
        })
        mock_redis.get = AsyncMock(return_value=running_payload)

    async def test_set_completed(self, tracker, mock_redis):
        await self._setup_running_state(mock_redis)
        await tracker.set_completed("task-1")

        payload = json.loads(mock_redis.set.call_args[0][1])
        assert payload["state"] == "completed"

    async def test_set_failed(self, tracker, mock_redis):
        await self._setup_running_state(mock_redis)
        await tracker.set_failed("task-1", "something broke")

        payload = json.loads(mock_redis.set.call_args[0][1])
        assert payload["state"] == "failed"
        assert payload["error"] == "something broke"

    async def test_set_timed_out(self, tracker, mock_redis):
        await self._setup_running_state(mock_redis)
        await tracker.set_timed_out("task-1")

        payload = json.loads(mock_redis.set.call_args[0][1])
        assert payload["state"] == "timed_out"

    async def test_set_aborted(self, tracker, mock_redis):
        await self._setup_running_state(mock_redis)
        await tracker.set_aborted("task-1")

        payload = json.loads(mock_redis.set.call_args[0][1])
        assert payload["state"] == "aborted"

    async def test_terminal_state_is_noop(self, tracker, mock_redis):
        """If the task is already in a terminal state, transition does nothing."""
        completed_payload = json.dumps({
            "task_id": "task-1",
            "function_name": "fn",
            "state": "completed",
            "instance_id": "inst-1",
            "started_at": 1000.0,
            "updated_at": 1001.0,
            "error": None,
        })
        mock_redis.get = AsyncMock(return_value=completed_payload)

        await tracker.set_failed("task-1", "late failure")

        # set() should not have been called (no write)
        mock_redis.set.assert_not_called()

    async def test_missing_state_is_noop(self, tracker, mock_redis):
        """If the state key is missing, transition does nothing."""
        mock_redis.get = AsyncMock(return_value=None)

        await tracker.set_completed("task-1")
        mock_redis.set.assert_not_called()


class TestGetState:

    async def test_get_state_returns_none_for_missing(self, tracker, mock_redis):
        mock_redis.get = AsyncMock(return_value=None)
        result = await tracker.get_state("nonexistent")
        assert result is None

    async def test_get_state_deserializes(self, tracker, mock_redis):
        payload = json.dumps({
            "task_id": "task-1",
            "function_name": "fn",
            "state": "running",
            "instance_id": "inst-1",
            "started_at": 1000.0,
            "updated_at": 1000.0,
            "error": None,
        })
        mock_redis.get = AsyncMock(return_value=payload)

        info = await tracker.get_state("task-1")
        assert info is not None
        assert info.task_id == "task-1"
        assert info.state == TaskState.RUNNING
        assert info.function_name == "fn"
