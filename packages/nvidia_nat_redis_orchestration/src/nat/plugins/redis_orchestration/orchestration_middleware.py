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
"""Redis orchestration middleware — state tracking + external abort."""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator
from typing import Any

import redis.asyncio as aioredis

from nat.builder.builder import Builder
from nat.middleware.dynamic.dynamic_function_middleware import DynamicFunctionMiddleware
from nat.middleware.middleware import CallNext
from nat.middleware.middleware import CallNextStream
from nat.middleware.middleware import FunctionMiddlewareContext

from .abort_controller import AbortController
from .exceptions import TaskAbortedError
from .orchestration_middleware_config import RedisOrchestrationConfig
from .state_tracker import TaskStateTracker

logger = logging.getLogger(__name__)


class RedisOrchestrationMiddleware(DynamicFunctionMiddleware):
    """Middleware that tracks task execution state in Redis and supports external abort.

    Wraps each intercepted function call with:
    * **State tracking** — sets ``running`` before execution, then ``completed`` /
      ``failed`` / ``timed_out`` / ``aborted`` on exit.  State is stored as a
      Redis string with a configurable TTL and published to a Pub/Sub channel.
    * **External abort** — subscribes to a per-task Pub/Sub channel so that
      external callers can cancel execution in flight.
    """

    def __init__(
        self,
        config: RedisOrchestrationConfig,
        builder: Builder,
        client: aioredis.Redis,
        instance_id: str,
    ) -> None:
        super().__init__(config=config, builder=builder)
        self._orch_config = config
        self._client = client
        self._instance_id = instance_id

        self._state_tracker: TaskStateTracker | None = None
        self._abort_controller: AbortController | None = None

        if config.enable_state_tracking:
            self._state_tracker = TaskStateTracker(client, config.key_prefix, config.state_ttl, instance_id)
        if config.enable_abort:
            self._abort_controller = AbortController(client, config.key_prefix)

    # ------------------------------------------------------------------
    # Single invocation
    # ------------------------------------------------------------------

    async def function_middleware_invoke(
        self,
        *args: Any,
        call_next: CallNext,
        context: FunctionMiddlewareContext,
        **kwargs: Any,
    ) -> Any:
        task_id = uuid.uuid4().hex

        if self._state_tracker:
            await self._state_tracker.set_running(task_id, context.name)

        abort_event = asyncio.Event()
        listener_task: asyncio.Task[None] | None = None
        if self._abort_controller:
            listener_task = asyncio.create_task(
                self._abort_controller.listen_for_abort(task_id, abort_event),
            )

        try:
            if self._abort_controller:
                result = await self._invoke_with_abort(args, kwargs, call_next, context, task_id, abort_event)
            else:
                result = await super().function_middleware_invoke(
                    *args, call_next=call_next, context=context, **kwargs,
                )

            if self._state_tracker:
                await self._state_tracker.set_completed(task_id)
            return result

        except TaskAbortedError:
            if self._state_tracker:
                await self._state_tracker.set_aborted(task_id)
            raise
        except TimeoutError:
            if self._state_tracker:
                await self._state_tracker.set_timed_out(task_id)
            raise
        except Exception as exc:
            if self._state_tracker:
                await self._state_tracker.set_failed(task_id, str(exc))
            raise
        finally:
            if listener_task is not None and not listener_task.done():
                listener_task.cancel()
                try:
                    await listener_task
                except asyncio.CancelledError:
                    pass

    # ------------------------------------------------------------------
    # Streaming invocation
    # ------------------------------------------------------------------

    async def function_middleware_stream(
        self,
        *args: Any,
        call_next: CallNextStream,
        context: FunctionMiddlewareContext,
        **kwargs: Any,
    ) -> AsyncIterator[Any]:
        task_id = uuid.uuid4().hex

        if self._state_tracker:
            await self._state_tracker.set_running(task_id, context.name)

        abort_event = asyncio.Event()
        listener_task: asyncio.Task[None] | None = None
        if self._abort_controller:
            listener_task = asyncio.create_task(
                self._abort_controller.listen_for_abort(task_id, abort_event),
            )

        try:
            async for chunk in super().function_middleware_stream(
                *args, call_next=call_next, context=context, **kwargs,
            ):
                if abort_event.is_set():
                    raise TaskAbortedError(task_id)
                yield chunk

            if self._state_tracker:
                await self._state_tracker.set_completed(task_id)

        except TaskAbortedError:
            if self._state_tracker:
                await self._state_tracker.set_aborted(task_id)
            raise
        except TimeoutError:
            if self._state_tracker:
                await self._state_tracker.set_timed_out(task_id)
            raise
        except Exception as exc:
            if self._state_tracker:
                await self._state_tracker.set_failed(task_id, str(exc))
            raise
        finally:
            if listener_task is not None and not listener_task.done():
                listener_task.cancel()
                try:
                    await listener_task
                except asyncio.CancelledError:
                    pass

    # ------------------------------------------------------------------
    # Abort-aware invocation helper
    # ------------------------------------------------------------------

    async def _invoke_with_abort(
        self,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        call_next: CallNext,
        context: FunctionMiddlewareContext,
        task_id: str,
        abort_event: asyncio.Event,
    ) -> Any:
        """Run the downstream call while racing against an abort signal."""
        execution_task = asyncio.create_task(
            super().function_middleware_invoke(*args, call_next=call_next, context=context, **kwargs),
        )

        abort_wait = asyncio.create_task(abort_event.wait())

        done, _pending = await asyncio.wait(
            {execution_task, abort_wait},
            return_when=asyncio.FIRST_COMPLETED,
        )

        if abort_event.is_set():
            execution_task.cancel()
            try:
                await execution_task
            except asyncio.CancelledError:
                pass
            raise TaskAbortedError(task_id)

        # Execution completed normally — clean up the abort waiter.
        abort_wait.cancel()
        return execution_task.result()


__all__ = ["RedisOrchestrationMiddleware"]
