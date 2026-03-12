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
"""Tests for AbortController."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from nat.plugins.redis_orchestration.abort_controller import AbortController


@pytest.fixture(name="mock_redis")
def fixture_mock_redis():
    client = AsyncMock()
    client.publish = AsyncMock(return_value=1)
    return client


@pytest.fixture(name="controller")
def fixture_controller(mock_redis):
    return AbortController(client=mock_redis, key_prefix="test")


class TestAbortChannel:

    def test_channel_format(self, controller):
        assert controller.abort_channel("task-1") == "test:abort:task-1"


class TestSendAbort:

    async def test_send_abort_publishes(self, controller, mock_redis):
        count = await controller.send_abort("task-1", reason="User cancelled")

        assert count == 1
        mock_redis.publish.assert_called_once()
        channel, payload_str = mock_redis.publish.call_args[0]
        assert channel == "test:abort:task-1"
        payload = json.loads(payload_str)
        assert payload["reason"] == "User cancelled"
        assert "timestamp" in payload

    async def test_send_abort_returns_zero_when_no_subscribers(self, controller, mock_redis):
        mock_redis.publish = AsyncMock(return_value=0)
        count = await controller.send_abort("task-1")
        assert count == 0


class TestListenForAbort:

    async def test_listen_sets_event_on_message(self, mock_redis):
        """Simulate a Pub/Sub message arriving and verify the event is set."""
        # Build a mock pubsub that yields one message then stops
        mock_pubsub = AsyncMock()
        mock_pubsub.subscribe = AsyncMock()
        mock_pubsub.unsubscribe = AsyncMock()
        mock_pubsub.close = AsyncMock()

        async def mock_listen():
            yield {"type": "subscribe", "data": 1}
            yield {"type": "message", "data": '{"reason": "abort"}'}

        mock_pubsub.listen = mock_listen
        mock_redis.pubsub = lambda: mock_pubsub

        controller = AbortController(client=mock_redis, key_prefix="test")
        event = asyncio.Event()

        await controller.listen_for_abort("task-1", event)

        assert event.is_set()
        mock_pubsub.unsubscribe.assert_called_once_with("test:abort:task-1")
        mock_pubsub.close.assert_called_once()

    async def test_listen_handles_cancellation(self, mock_redis):
        """Listener should clean up gracefully when cancelled."""
        mock_pubsub = AsyncMock()
        mock_pubsub.subscribe = AsyncMock()
        mock_pubsub.unsubscribe = AsyncMock()
        mock_pubsub.close = AsyncMock()

        async def mock_listen():
            yield {"type": "subscribe", "data": 1}
            # Simulate a long wait that will be cancelled
            await asyncio.sleep(999)
            yield {"type": "message", "data": "never"}  # pragma: no cover

        mock_pubsub.listen = mock_listen
        mock_redis.pubsub = lambda: mock_pubsub

        controller = AbortController(client=mock_redis, key_prefix="test")
        event = asyncio.Event()

        task = asyncio.create_task(controller.listen_for_abort("task-1", event))
        await asyncio.sleep(0.01)
        task.cancel()
        await task  # Should not raise

        assert not event.is_set()
        mock_pubsub.unsubscribe.assert_called_once()
        mock_pubsub.close.assert_called_once()
