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
"""Redis orchestration middleware — state tracking, external abort, session continuity."""

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
from .session_store import SessionStore
from .state_tracker import TaskStateTracker

logger = logging.getLogger(__name__)


class RedisOrchestrationMiddleware(DynamicFunctionMiddleware):
    """Middleware that tracks task execution state in Redis, supports external abort,
    and provides session continuity for conversation history.

    Wraps each intercepted function call with:
    * **State tracking** — sets ``running`` before execution, then ``completed`` /
      ``failed`` / ``timed_out`` / ``aborted`` on exit.
    * **External abort** — subscribes to a per-task Pub/Sub channel so that
      external callers can cancel execution in flight.
    * **Session continuity** — persists conversation turns (user input + assistant
      response) to Redis lists with TTL and size-based rotation.
    """

    # LLM method names from COMPONENT_FUNCTION_ALLOWLISTS — unique to LLM components.
    _LLM_METHODS: frozenset[str] = frozenset({"invoke", "ainvoke", "stream", "astream"})

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
        self._session_store: SessionStore | None = None

        if config.enable_state_tracking:
            self._state_tracker = TaskStateTracker(client, config.key_prefix, config.state_ttl, instance_id)
        if config.enable_abort:
            self._abort_controller = AbortController(client, config.key_prefix)
        if config.enable_session_continuity:
            self._session_store = SessionStore(
                client, config.key_prefix,
                session_ttl=config.session_ttl,
                max_turns=config.session_max_turns,
                max_bytes=config.session_max_bytes,
            )

    @property
    def session_store(self) -> SessionStore | None:
        """Access the session store for direct programmatic use."""
        return self._session_store

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

            # Session capture — persist LLM turns
            await self._capture_session_turn(context, args, result)

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

        accumulated_content: list[str] = []

        try:
            async for chunk in super().function_middleware_stream(
                *args, call_next=call_next, context=context, **kwargs,
            ):
                if abort_event.is_set():
                    raise TaskAbortedError(task_id)
                # Accumulate content for session capture
                if self._session_store:
                    accumulated_content.append(self._extract_chunk_text(chunk))
                yield chunk

            if self._state_tracker:
                await self._state_tracker.set_completed(task_id)

            # Session capture — persist streamed response
            if accumulated_content:
                full_response = "".join(accumulated_content)
                await self._capture_session_turn(context, args, full_response)

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

    # ------------------------------------------------------------------
    # Session continuity helpers
    # ------------------------------------------------------------------

    def _get_session_id(self) -> str | None:
        """Derive session ID from the current execution context."""
        if not self._session_store:
            return None
        try:
            from nat.builder.context import Context
            ctx = Context.get()
            return SessionStore.derive_session_id(
                conversation_id=getattr(ctx, "conversation_id", None),
                user_id=getattr(ctx, "user_id", None),
            )
        except Exception:
            return None

    def _is_llm_call(self, context: FunctionMiddlewareContext) -> bool:
        """Check if the intercepted call is an LLM invocation."""
        return context.name in self._LLM_METHODS

    async def _capture_session_turn(
        self,
        context: FunctionMiddlewareContext,
        args: tuple[Any, ...],
        result: Any,
    ) -> None:
        """Capture user input + assistant response as session turns.

        Only activates for LLM calls (invoke/ainvoke/stream/astream).
        """
        if not self._session_store or not self._is_llm_call(context):
            return

        session_id = self._get_session_id()
        if not session_id:
            return

        try:
            # Extract user message from LLM input args
            user_text = self._extract_user_message(args)
            if user_text:
                await self._session_store.add_user_turn(session_id, user_text)

            # Extract assistant response
            assistant_text = self._extract_assistant_response(result)
            if assistant_text:
                await self._session_store.add_assistant_turn(session_id, assistant_text)
        except Exception:
            logger.debug("Session capture failed for session %s", session_id, exc_info=True)

    @staticmethod
    def _extract_user_message(args: tuple[Any, ...]) -> str | None:
        """Extract the last user message from LLM input args.

        LLM ainvoke receives args[0] as a list of message objects
        (LangChain BaseMessage) or a list of dicts with role/content.
        We find the last user/human message.
        """
        if not args:
            return None
        messages = args[0]
        if not isinstance(messages, list) or not messages:
            # Could be a string prompt
            if isinstance(messages, str):
                return messages
            return None

        # Walk backwards to find the last user message
        for msg in reversed(messages):
            # LangChain BaseMessage — has .type and .content
            if hasattr(msg, "type") and hasattr(msg, "content"):
                if msg.type in ("human", "user"):
                    return str(msg.content)
            # Dict format — {"role": "user", "content": "..."}
            elif isinstance(msg, dict):
                if msg.get("role") in ("user", "human"):
                    return str(msg.get("content", ""))
        return None

    @staticmethod
    def _extract_assistant_response(result: Any) -> str | None:
        """Extract text content from an LLM response.

        Handles LangChain AIMessage objects and plain strings.
        """
        if result is None:
            return None
        if isinstance(result, str):
            return result if result.strip() else None
        # LangChain AIMessage — has .content attribute
        if hasattr(result, "content"):
            content = result.content
            return str(content) if content else None
        return str(result) if result else None

    @staticmethod
    def _extract_chunk_text(chunk: Any) -> str:
        """Extract text from a streaming chunk."""
        if isinstance(chunk, str):
            return chunk
        if hasattr(chunk, "content"):
            return str(chunk.content) if chunk.content else ""
        return str(chunk) if chunk else ""


__all__ = ["RedisOrchestrationMiddleware"]
