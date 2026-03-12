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
"""Integration tests against a live Redis instance.

Requires: REDIS_URL env var (e.g. redis://h-oracle:6379).
Run with: pytest tests/test_integration.py -v -m integration
"""

from __future__ import annotations

import asyncio
import os

import pytest
import redis.asyncio as aioredis

from nat.plugins.redis_orchestration.abort_controller import AbortController
from nat.plugins.redis_orchestration.crash_recovery import CrashRecovery
from nat.plugins.redis_orchestration.models import TaskState
from nat.plugins.redis_orchestration.state_tracker import TaskStateTracker

REDIS_URL = os.environ.get("REDIS_URL", "redis://h-oracle:6379")


@pytest.fixture(name="redis_client")
async def fixture_redis_client():
    client = aioredis.from_url(REDIS_URL, decode_responses=True, socket_timeout=5.0)
    await client.ping()
    yield client
    # Cleanup: delete all test keys
    cursor = 0
    while True:
        cursor, keys = await client.scan(cursor=cursor, match="inttest:*", count=100)
        if keys:
            await client.delete(*keys)
        if cursor == 0:
            break
    await client.close()


@pytest.mark.integration
class TestStateTrackerIntegration:

    async def test_full_lifecycle(self, redis_client):
        tracker = TaskStateTracker(redis_client, "inttest", state_ttl=30, instance_id="test-node")

        # Set running
        await tracker.set_running("task-1", "my_function")
        info = await tracker.get_state("task-1")
        assert info is not None
        assert info.state == TaskState.RUNNING
        assert info.function_name == "my_function"
        assert info.instance_id == "test-node"

        # Transition to completed
        await tracker.set_completed("task-1")
        info = await tracker.get_state("task-1")
        assert info is not None
        assert info.state == TaskState.COMPLETED

        # Terminal state is idempotent
        await tracker.set_failed("task-1", "late error")
        info = await tracker.get_state("task-1")
        assert info.state == TaskState.COMPLETED  # unchanged

    async def test_failed_state(self, redis_client):
        tracker = TaskStateTracker(redis_client, "inttest", state_ttl=30, instance_id="test-node")

        await tracker.set_running("task-fail", "bad_func")
        await tracker.set_failed("task-fail", "something broke")
        info = await tracker.get_state("task-fail")
        assert info.state == TaskState.FAILED
        assert info.error == "something broke"

    async def test_missing_key_returns_none(self, redis_client):
        tracker = TaskStateTracker(redis_client, "inttest", state_ttl=30, instance_id="test-node")
        assert await tracker.get_state("nonexistent") is None


@pytest.mark.integration
class TestAbortControllerIntegration:

    async def test_abort_round_trip(self, redis_client):
        controller = AbortController(redis_client, "inttest")
        event = asyncio.Event()

        # Start listener
        listener = asyncio.create_task(controller.listen_for_abort("abort-task", event))
        await asyncio.sleep(0.1)  # Let subscription establish

        # Send abort
        receivers = await controller.send_abort("abort-task", reason="test kill")
        assert receivers >= 1

        # Wait for event
        await asyncio.wait_for(event.wait(), timeout=2.0)
        assert event.is_set()

        # Cleanup
        if not listener.done():
            listener.cancel()
            try:
                await listener
            except asyncio.CancelledError:
                pass

    async def test_abort_no_listener(self, redis_client):
        controller = AbortController(redis_client, "inttest")
        receivers = await controller.send_abort("nobody-listening", reason="void")
        assert receivers == 0


@pytest.mark.integration
class TestCrashRecoveryIntegration:

    async def test_recovers_orphaned_tasks(self, redis_client):
        tracker = TaskStateTracker(redis_client, "inttest", state_ttl=30, instance_id="old-instance")

        # Simulate two tasks left in running state (orphans from a crash)
        await tracker.set_running("orphan-1", "func_a")
        await tracker.set_running("orphan-2", "func_b")

        # Also one completed task (should not be recovered)
        await tracker.set_running("done-1", "func_c")
        await tracker.set_completed("done-1")

        # Run crash recovery
        recovery = CrashRecovery(redis_client, "inttest", state_ttl=30, instance_id="new-instance")
        recovered = await recovery.recover_orphaned_tasks()

        assert set(recovered) == {"orphan-1", "orphan-2"}

        # Verify they're now failed
        info1 = await tracker.get_state("orphan-1")
        assert info1.state == TaskState.FAILED
        assert "Orphaned" in info1.error

        info2 = await tracker.get_state("orphan-2")
        assert info2.state == TaskState.FAILED

        # Completed task untouched
        done = await tracker.get_state("done-1")
        assert done.state == TaskState.COMPLETED


@pytest.mark.integration
class TestPubSubStateEvents:

    async def test_state_events_published(self, redis_client):
        """Verify that state transitions are published to the events channel."""
        tracker = TaskStateTracker(redis_client, "inttest", state_ttl=30, instance_id="test-node")

        # Create a separate client for subscribing (Pub/Sub requires dedicated connection)
        sub_client = aioredis.from_url(REDIS_URL, decode_responses=True)
        pubsub = sub_client.pubsub()
        await pubsub.subscribe(tracker.state_channel)

        # Consume the subscription confirmation
        msg = await pubsub.get_message(timeout=2.0)
        assert msg["type"] == "subscribe"

        # Trigger state transitions
        await tracker.set_running("events-task", "my_func")

        # Read the published event
        msg = await pubsub.get_message(timeout=2.0)
        assert msg is not None
        assert msg["type"] == "message"
        import json
        event = json.loads(msg["data"])
        assert event["task_id"] == "events-task"
        assert event["state"] == "running"

        await pubsub.unsubscribe()
        await pubsub.close()
        await sub_client.close()
