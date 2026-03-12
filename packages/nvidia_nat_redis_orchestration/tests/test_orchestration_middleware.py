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
"""Tests for RedisOrchestrationMiddleware."""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest

from nat.middleware.middleware import FunctionMiddlewareContext
from nat.plugins.redis_orchestration.exceptions import TaskAbortedError
from nat.plugins.redis_orchestration.orchestration_middleware import RedisOrchestrationMiddleware
from nat.plugins.redis_orchestration.orchestration_middleware_config import RedisOrchestrationConfig

# ==================== Fixtures ====================


@pytest.fixture(name="mock_builder")
def fixture_mock_builder():
    builder = Mock()
    builder._functions = {}
    builder.get_llm = AsyncMock()
    builder.get_embedder = AsyncMock()
    builder.get_retriever = AsyncMock()
    builder.get_memory_client = AsyncMock()
    builder.get_object_store_client = AsyncMock()
    builder.get_auth_provider = AsyncMock()
    builder.get_function = AsyncMock()
    builder.get_function_config = Mock()
    return builder


@pytest.fixture(name="function_context")
def fixture_function_context():
    return FunctionMiddlewareContext(
        name="test_function",
        config=Mock(),
        description="A test function",
        input_schema=None,
        single_output_schema=type(None),
        stream_output_schema=type(None),
    )


@pytest.fixture(name="mock_redis")
def fixture_mock_redis():
    client = AsyncMock()
    client.set = AsyncMock()
    client.get = AsyncMock(return_value=None)
    client.publish = AsyncMock(return_value=1)
    return client


def _make_middleware(
    mock_builder: Mock,
    mock_redis: AsyncMock,
    *,
    enable_state_tracking: bool = True,
    enable_abort: bool = False,
) -> RedisOrchestrationMiddleware:
    config = RedisOrchestrationConfig(
        redis_url="redis://localhost:6379",
        enable_state_tracking=enable_state_tracking,
        enable_abort=enable_abort,
        state_ttl=600,
        key_prefix="test",
    )
    return RedisOrchestrationMiddleware(
        config=config,
        builder=mock_builder,
        client=mock_redis,
        instance_id="test-inst",
    )


def _setup_running_get(mock_redis: AsyncMock) -> None:
    """Make mock_redis.get return a running state so transitions succeed."""
    def get_side_effect(key):
        return json.dumps({
            "task_id": key.split(":")[-1],
            "function_name": "test_function",
            "state": "running",
            "instance_id": "test-inst",
            "started_at": 1000.0,
            "updated_at": 1000.0,
            "error": None,
        })

    mock_redis.get = AsyncMock(side_effect=get_side_effect)


# ==================== Invoke Tests ====================


class TestOrchestrationMiddlewareInvoke:

    async def test_successful_invoke_tracks_state(self, mock_builder, function_context, mock_redis):
        _setup_running_get(mock_redis)
        middleware = _make_middleware(mock_builder, mock_redis)

        async def fast_function(*args, **kwargs):
            return "result"

        call_next = AsyncMock(side_effect=fast_function)

        result = await middleware.function_middleware_invoke(
            "input", call_next=call_next, context=function_context,
        )

        assert result == "result"
        # Should have: set_running (SET+PUBLISH), set_completed (GET+SET+PUBLISH)
        assert mock_redis.set.call_count == 2
        # First call: running
        first_payload = json.loads(mock_redis.set.call_args_list[0][0][1])
        assert first_payload["state"] == "running"
        # Second call: completed
        second_payload = json.loads(mock_redis.set.call_args_list[1][0][1])
        assert second_payload["state"] == "completed"

    async def test_failed_invoke_tracks_failure(self, mock_builder, function_context, mock_redis):
        _setup_running_get(mock_redis)
        middleware = _make_middleware(mock_builder, mock_redis)

        call_next = AsyncMock(side_effect=ValueError("bad input"))

        with pytest.raises(ValueError, match="bad input"):
            await middleware.function_middleware_invoke(
                "input", call_next=call_next, context=function_context,
            )

        assert mock_redis.set.call_count == 2
        second_payload = json.loads(mock_redis.set.call_args_list[1][0][1])
        assert second_payload["state"] == "failed"
        assert second_payload["error"] == "bad input"

    async def test_timeout_tracks_timed_out(self, mock_builder, function_context, mock_redis):
        _setup_running_get(mock_redis)
        middleware = _make_middleware(mock_builder, mock_redis)

        call_next = AsyncMock(side_effect=TimeoutError("too slow"))

        with pytest.raises(TimeoutError):
            await middleware.function_middleware_invoke(
                "input", call_next=call_next, context=function_context,
            )

        second_payload = json.loads(mock_redis.set.call_args_list[1][0][1])
        assert second_payload["state"] == "timed_out"

    async def test_state_tracking_disabled(self, mock_builder, function_context, mock_redis):
        middleware = _make_middleware(mock_builder, mock_redis, enable_state_tracking=False)

        call_next = AsyncMock(return_value="ok")

        result = await middleware.function_middleware_invoke(
            "input", call_next=call_next, context=function_context,
        )

        assert result == "ok"
        mock_redis.set.assert_not_called()


# ==================== Streaming Tests ====================


class TestOrchestrationMiddlewareStream:

    async def test_stream_tracks_state(self, mock_builder, function_context, mock_redis):
        _setup_running_get(mock_redis)
        middleware = _make_middleware(mock_builder, mock_redis)

        async def fast_stream(*args, **kwargs):
            for i in range(3):
                yield f"chunk_{i}"

        collected = []
        async for chunk in middleware.function_middleware_stream(
            "input", call_next=fast_stream, context=function_context,
        ):
            collected.append(chunk)

        assert collected == ["chunk_0", "chunk_1", "chunk_2"]
        assert mock_redis.set.call_count == 2
        first_payload = json.loads(mock_redis.set.call_args_list[0][0][1])
        assert first_payload["state"] == "running"
        second_payload = json.loads(mock_redis.set.call_args_list[1][0][1])
        assert second_payload["state"] == "completed"

    async def test_stream_failure_tracks_state(self, mock_builder, function_context, mock_redis):
        _setup_running_get(mock_redis)
        middleware = _make_middleware(mock_builder, mock_redis)

        async def error_stream(*args, **kwargs):
            yield "chunk_0"
            raise RuntimeError("stream broke")

        with pytest.raises(RuntimeError, match="stream broke"):
            async for _ in middleware.function_middleware_stream(
                "input", call_next=error_stream, context=function_context,
            ):
                pass

        second_payload = json.loads(mock_redis.set.call_args_list[1][0][1])
        assert second_payload["state"] == "failed"


# ==================== Abort Tests ====================


class TestOrchestrationMiddlewareAbort:

    async def test_abort_cancels_execution(self, mock_builder, function_context, mock_redis):
        _setup_running_get(mock_redis)
        middleware = _make_middleware(mock_builder, mock_redis, enable_abort=True, enable_state_tracking=True)

        # Mock the pubsub so listen_for_abort immediately signals abort
        mock_pubsub = AsyncMock()
        mock_pubsub.subscribe = AsyncMock()
        mock_pubsub.unsubscribe = AsyncMock()
        mock_pubsub.close = AsyncMock()

        async def immediate_abort_listen():
            yield {"type": "subscribe", "data": 1}
            yield {"type": "message", "data": '{"reason": "test abort"}'}

        mock_pubsub.listen = immediate_abort_listen
        mock_redis.pubsub = lambda: mock_pubsub

        async def slow_function(*args, **kwargs):
            await asyncio.sleep(10)
            return "never"  # pragma: no cover

        call_next = AsyncMock(side_effect=slow_function)

        with pytest.raises(TaskAbortedError):
            await middleware.function_middleware_invoke(
                "input", call_next=call_next, context=function_context,
            )

        # Verify state was set to aborted
        aborted_calls = [
            c for c in mock_redis.set.call_args_list
            if "aborted" in json.loads(c[0][1]).get("state", "")
        ]
        assert len(aborted_calls) == 1

    async def test_stream_abort_between_chunks(self, mock_builder, function_context, mock_redis):
        _setup_running_get(mock_redis)
        middleware = _make_middleware(mock_builder, mock_redis, enable_abort=True, enable_state_tracking=True)

        # Mock pubsub that signals abort after a short delay
        mock_pubsub = AsyncMock()
        mock_pubsub.subscribe = AsyncMock()
        mock_pubsub.unsubscribe = AsyncMock()
        mock_pubsub.close = AsyncMock()

        async def delayed_abort_listen():
            yield {"type": "subscribe", "data": 1}
            await asyncio.sleep(0.05)
            yield {"type": "message", "data": '{"reason": "abort during stream"}'}

        mock_pubsub.listen = delayed_abort_listen
        mock_redis.pubsub = lambda: mock_pubsub

        async def slow_stream(*args, **kwargs):
            yield "chunk_0"
            await asyncio.sleep(0.1)  # Give time for abort to arrive
            yield "chunk_1"

        with pytest.raises(TaskAbortedError):
            async for _ in middleware.function_middleware_stream(
                "input", call_next=slow_stream, context=function_context,
            ):
                pass
