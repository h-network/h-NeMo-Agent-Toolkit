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
"""Tests for CrashRecovery."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from nat.plugins.redis_orchestration.crash_recovery import CrashRecovery


def _make_state(task_id: str, state: str = "running") -> str:
    return json.dumps({
        "task_id": task_id,
        "function_name": "some_func",
        "state": state,
        "instance_id": "old-inst",
        "started_at": 1000.0,
        "updated_at": 1000.0,
        "error": None,
    })


@pytest.fixture(name="mock_redis")
def fixture_mock_redis():
    client = AsyncMock()
    client.set = AsyncMock()
    client.publish = AsyncMock(return_value=0)
    return client


class TestCrashRecovery:

    async def test_recovers_running_tasks(self, mock_redis):
        # SCAN returns two keys: one running, one completed.
        # set_failed internally calls get_state (extra get per recovered task),
        # so use a key-based side_effect to handle interleaved calls.
        mock_redis.scan = AsyncMock(return_value=(0, ["test:state:t1", "test:state:t2"]))
        state_store = {
            "test:state:t1": _make_state("t1", "running"),
            "test:state:t2": _make_state("t2", "completed"),
        }
        mock_redis.get = AsyncMock(side_effect=lambda key: state_store.get(key))

        recovery = CrashRecovery(mock_redis, "test", state_ttl=600, instance_id="new-inst")
        recovered = await recovery.recover_orphaned_tasks()

        assert recovered == ["t1"]
        # set() should have been called to transition t1 to failed
        assert mock_redis.set.call_count == 1
        payload = json.loads(mock_redis.set.call_args[0][1])
        assert payload["state"] == "failed"
        assert "Orphaned" in payload["error"]

    async def test_no_orphans(self, mock_redis):
        mock_redis.scan = AsyncMock(return_value=(0, ["test:state:t1"]))
        mock_redis.get = AsyncMock(return_value=_make_state("t1", "completed"))

        recovery = CrashRecovery(mock_redis, "test", state_ttl=600, instance_id="new-inst")
        recovered = await recovery.recover_orphaned_tasks()

        assert recovered == []
        mock_redis.set.assert_not_called()

    async def test_empty_keyspace(self, mock_redis):
        mock_redis.scan = AsyncMock(return_value=(0, []))

        recovery = CrashRecovery(mock_redis, "test", state_ttl=600, instance_id="new-inst")
        recovered = await recovery.recover_orphaned_tasks()

        assert recovered == []

    async def test_handles_expired_key(self, mock_redis):
        """If a key is found by SCAN but GET returns None (TTL expired), skip it."""
        mock_redis.scan = AsyncMock(return_value=(0, ["test:state:t1"]))
        mock_redis.get = AsyncMock(return_value=None)

        recovery = CrashRecovery(mock_redis, "test", state_ttl=600, instance_id="new-inst")
        recovered = await recovery.recover_orphaned_tasks()

        assert recovered == []

    async def test_multi_page_scan(self, mock_redis):
        """Verify cursor-based pagination."""
        mock_redis.scan = AsyncMock(side_effect=[
            (42, ["test:state:t1"]),   # First page, cursor=42
            (0, ["test:state:t2"]),    # Second page, cursor=0 (done)
        ])
        state_store = {
            "test:state:t1": _make_state("t1", "running"),
            "test:state:t2": _make_state("t2", "running"),
        }
        mock_redis.get = AsyncMock(side_effect=lambda key: state_store.get(key))

        recovery = CrashRecovery(mock_redis, "test", state_ttl=600, instance_id="new-inst")
        recovered = await recovery.recover_orphaned_tasks()

        assert set(recovered) == {"t1", "t2"}
        assert mock_redis.scan.call_count == 2
