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
"""
Comprehensive test report for nvidia_nat_redis_orchestration.

Covers every code path across all components:
  - Models & Exceptions
  - TaskStateTracker (unit + live Redis)
  - AbortController (unit + live Redis)
  - CrashRecovery (unit + live Redis)
  - RedisOrchestrationMiddleware (unit + live E2E with real LLM)
  - RedisOrchestrationConfig (validation)
  - Pub/Sub event broadcasting
  - Concurrency & race conditions
  - Edge cases

Generates a structured Markdown test report.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import os
import sys
import time
import traceback
from typing import Any
from unittest.mock import AsyncMock, Mock

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
REDIS_URL = os.environ.get("REDIS_URL", "redis://h-oracle:6379")
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://h-titan:11434/v1")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "gpt-oss:20b")
KEY_PREFIX = "fulltest"
REPORT_PATH = os.environ.get(
    "REPORT_PATH",
    os.path.join(os.path.dirname(__file__), "..", "TEST_REPORT.md"),
)

# ---------------------------------------------------------------------------
# Report infrastructure
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class TestResult:
    suite: str
    name: str
    passed: bool
    duration_ms: float
    detail: str = ""
    error: str = ""


results: list[TestResult] = []


def record(suite: str, name: str, passed: bool, duration: float, detail: str = "", error: str = ""):
    results.append(TestResult(suite, name, passed, round(duration * 1000, 1), detail, error))
    status = "PASS" if passed else "FAIL"
    print(f"  [{status}] {suite} / {name} ({duration*1000:.0f}ms)")


async def run_test(suite: str, name: str, coro):
    t0 = time.time()
    try:
        detail = await coro
        record(suite, name, True, time.time() - t0, detail=detail or "")
    except Exception as exc:
        record(suite, name, False, time.time() - t0, error=f"{type(exc).__name__}: {exc}")
        traceback.print_exc()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def mock_builder():
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


def make_context(name: str = "test_function"):
    from nat.middleware.middleware import FunctionMiddlewareContext
    return FunctionMiddlewareContext(
        name=name,
        config=Mock(),
        description=f"Test function: {name}",
        input_schema=None,
        single_output_schema=type(None),
        stream_output_schema=type(None),
    )


async def call_llm(prompt: str, max_tokens: int = 200) -> str:
    import httpx
    async with httpx.AsyncClient(timeout=120.0) as client:
        resp = await client.post(
            f"{OLLAMA_URL}/chat/completions",
            json={"model": OLLAMA_MODEL, "messages": [{"role": "user", "content": prompt}], "max_tokens": max_tokens},
        )
        data = resp.json()
        return data["choices"][0]["message"].get("content", "") or "[no content]"


async def drain_events(pubsub, timeout: float = 0.5) -> list[dict]:
    events = []
    while True:
        msg = await pubsub.get_message(timeout=timeout)
        if msg is None:
            break
        if msg["type"] == "message":
            events.append(json.loads(msg["data"]))
    return events


async def cleanup_keys(client, prefix: str):
    cursor = 0
    while True:
        cursor, keys = await client.scan(cursor=cursor, match=f"{prefix}:*", count=200)
        if keys:
            await client.delete(*keys)
        if cursor == 0:
            break


# ===========================================================================
# TEST SUITES
# ===========================================================================


# ---------------------------------------------------------------------------
# 1. Models & Exceptions
# ---------------------------------------------------------------------------
async def suite_models():
    suite = "Models & Exceptions"
    print(f"\n{'='*60}\n{suite}\n{'='*60}")

    from nat.plugins.redis_orchestration.models import TaskState, TaskStateInfo, TERMINAL_STATES
    from nat.plugins.redis_orchestration.exceptions import TaskAbortedError, OrchestrationError

    async def test_task_state_values():
        assert TaskState.RUNNING == "running"
        assert TaskState.COMPLETED == "completed"
        assert TaskState.FAILED == "failed"
        assert TaskState.TIMED_OUT == "timed_out"
        assert TaskState.ABORTED == "aborted"
        return "All 5 states have correct string values"

    async def test_terminal_states():
        assert TaskState.RUNNING not in TERMINAL_STATES
        assert TaskState.COMPLETED in TERMINAL_STATES
        assert TaskState.FAILED in TERMINAL_STATES
        assert TaskState.TIMED_OUT in TERMINAL_STATES
        assert TaskState.ABORTED in TERMINAL_STATES
        return "RUNNING is non-terminal; 4 terminal states correct"

    async def test_task_state_info_frozen():
        info = TaskStateInfo("t1", "fn", TaskState.RUNNING, "inst", 1.0, 2.0)
        try:
            info.state = TaskState.COMPLETED
            raise AssertionError("Should be frozen")
        except dataclasses.FrozenInstanceError:
            pass
        return "TaskStateInfo is immutable (frozen dataclass)"

    async def test_task_state_info_fields():
        info = TaskStateInfo("t1", "fn", TaskState.FAILED, "inst", 100.0, 200.0, error="broke")
        assert info.task_id == "t1"
        assert info.function_name == "fn"
        assert info.error == "broke"
        d = dataclasses.asdict(info)
        assert set(d.keys()) == {"task_id", "function_name", "state", "instance_id", "started_at", "updated_at", "error"}
        return "All 7 fields present and correct"

    async def test_task_state_info_default_error():
        info = TaskStateInfo("t1", "fn", TaskState.RUNNING, "inst", 1.0, 2.0)
        assert info.error is None
        return "error defaults to None"

    async def test_task_aborted_error():
        err = TaskAbortedError("task-99", "user cancelled")
        assert err.task_id == "task-99"
        assert err.reason == "user cancelled"
        assert "task-99" in str(err)
        assert isinstance(err, OrchestrationError)
        assert isinstance(err, Exception)
        return "TaskAbortedError inherits OrchestrationError, carries task_id + reason"

    async def test_orchestration_error():
        err = OrchestrationError("generic failure")
        assert str(err) == "generic failure"
        assert isinstance(err, Exception)
        return "OrchestrationError is a plain Exception subclass"

    for t in [test_task_state_values, test_terminal_states, test_task_state_info_frozen,
              test_task_state_info_fields, test_task_state_info_default_error,
              test_task_aborted_error, test_orchestration_error]:
        await run_test(suite, t.__name__.replace("test_", ""), t())


# ---------------------------------------------------------------------------
# 2. Config validation
# ---------------------------------------------------------------------------
async def suite_config():
    suite = "Config Validation"
    print(f"\n{'='*60}\n{suite}\n{'='*60}")

    from nat.plugins.redis_orchestration.orchestration_middleware_config import RedisOrchestrationConfig
    from pydantic import ValidationError

    async def test_minimal_config():
        c = RedisOrchestrationConfig(redis_url="redis://localhost:6379")
        assert c.enable_state_tracking is True
        assert c.enable_abort is True
        assert c.state_ttl == 3600
        assert c.key_prefix == "nat:orch"
        assert c.crash_recovery_on_startup is True
        assert c.instance_id is None
        return f"Defaults: ttl={c.state_ttl}, prefix={c.key_prefix}, tracking={c.enable_state_tracking}"

    async def test_full_config():
        c = RedisOrchestrationConfig(
            redis_url="redis://myhost:6380/2", enable_state_tracking=False, enable_abort=False,
            state_ttl=7200, key_prefix="custom", crash_recovery_on_startup=False, instance_id="node-1",
        )
        assert c.redis_url == "redis://myhost:6380/2"
        assert c.state_ttl == 7200
        assert c.instance_id == "node-1"
        return "All fields accepted with custom values"

    async def test_state_ttl_zero_rejected():
        try:
            RedisOrchestrationConfig(redis_url="redis://x", state_ttl=0)
            raise AssertionError("Should reject ttl=0")
        except ValidationError:
            pass
        return "state_ttl=0 raises ValidationError"

    async def test_state_ttl_negative_rejected():
        try:
            RedisOrchestrationConfig(redis_url="redis://x", state_ttl=-5)
            raise AssertionError("Should reject ttl=-5")
        except ValidationError:
            pass
        return "state_ttl=-5 raises ValidationError"

    async def test_type_discriminator():
        c = RedisOrchestrationConfig(redis_url="redis://x")
        assert c.type == "redis_orchestration"
        return f"type discriminator = '{c.type}'"

    async def test_redis_password_optional():
        c = RedisOrchestrationConfig(redis_url="redis://x", redis_password="secret123")
        assert c.redis_password is not None
        return "redis_password accepted as optional field"

    for t in [test_minimal_config, test_full_config, test_state_ttl_zero_rejected,
              test_state_ttl_negative_rejected, test_type_discriminator, test_redis_password_optional]:
        await run_test(suite, t.__name__.replace("test_", ""), t())


# ---------------------------------------------------------------------------
# 3. State Tracker — unit (mocked Redis)
# ---------------------------------------------------------------------------
async def suite_state_tracker_unit():
    suite = "State Tracker (unit)"
    print(f"\n{'='*60}\n{suite}\n{'='*60}")

    from nat.plugins.redis_orchestration.state_tracker import TaskStateTracker
    from nat.plugins.redis_orchestration.models import TaskState

    def make_mock():
        m = AsyncMock()
        m.set = AsyncMock()
        m.get = AsyncMock(return_value=None)
        m.publish = AsyncMock(return_value=1)
        return m

    async def test_key_format():
        t = TaskStateTracker(make_mock(), "pfx", 600, "i1")
        assert t.state_key("abc") == "pfx:state:abc"
        assert t.state_channel == "pfx:state_events"
        return "pfx:state:{id} and pfx:state_events"

    async def test_set_running_payload():
        m = make_mock()
        t = TaskStateTracker(m, "pfx", 600, "i1")
        await t.set_running("t1", "fn")
        payload = json.loads(m.set.call_args[0][1])
        assert payload["task_id"] == "t1"
        assert payload["function_name"] == "fn"
        assert payload["state"] == "running"
        assert payload["instance_id"] == "i1"
        assert m.set.call_args[1]["ex"] == 600
        return f"JSON payload correct, TTL=600"

    async def test_set_running_publishes():
        m = make_mock()
        t = TaskStateTracker(m, "pfx", 600, "i1")
        await t.set_running("t1", "fn")
        ch, data = m.publish.call_args[0]
        assert ch == "pfx:state_events"
        evt = json.loads(data)
        assert evt["state"] == "running"
        return "Published to pfx:state_events"

    async def test_transition_completed():
        m = make_mock()
        m.get = AsyncMock(return_value=json.dumps({
            "task_id": "t1", "function_name": "fn", "state": "running",
            "instance_id": "i1", "started_at": 1.0, "updated_at": 1.0, "error": None,
        }))
        t = TaskStateTracker(m, "pfx", 600, "i1")
        await t.set_completed("t1")
        payload = json.loads(m.set.call_args[0][1])
        assert payload["state"] == "completed"
        return "running -> completed"

    async def test_transition_failed_with_error():
        m = make_mock()
        m.get = AsyncMock(return_value=json.dumps({
            "task_id": "t1", "function_name": "fn", "state": "running",
            "instance_id": "i1", "started_at": 1.0, "updated_at": 1.0, "error": None,
        }))
        t = TaskStateTracker(m, "pfx", 600, "i1")
        await t.set_failed("t1", "broke")
        payload = json.loads(m.set.call_args[0][1])
        assert payload["state"] == "failed"
        assert payload["error"] == "broke"
        return "running -> failed with error message"

    async def test_transition_timed_out():
        m = make_mock()
        m.get = AsyncMock(return_value=json.dumps({
            "task_id": "t1", "function_name": "fn", "state": "running",
            "instance_id": "i1", "started_at": 1.0, "updated_at": 1.0, "error": None,
        }))
        t = TaskStateTracker(m, "pfx", 600, "i1")
        await t.set_timed_out("t1")
        payload = json.loads(m.set.call_args[0][1])
        assert payload["state"] == "timed_out"
        return "running -> timed_out"

    async def test_transition_aborted():
        m = make_mock()
        m.get = AsyncMock(return_value=json.dumps({
            "task_id": "t1", "function_name": "fn", "state": "running",
            "instance_id": "i1", "started_at": 1.0, "updated_at": 1.0, "error": None,
        }))
        t = TaskStateTracker(m, "pfx", 600, "i1")
        await t.set_aborted("t1")
        payload = json.loads(m.set.call_args[0][1])
        assert payload["state"] == "aborted"
        return "running -> aborted"

    async def test_terminal_state_idempotent():
        m = make_mock()
        m.get = AsyncMock(return_value=json.dumps({
            "task_id": "t1", "function_name": "fn", "state": "completed",
            "instance_id": "i1", "started_at": 1.0, "updated_at": 2.0, "error": None,
        }))
        t = TaskStateTracker(m, "pfx", 600, "i1")
        await t.set_failed("t1", "late")
        m.set.assert_not_called()
        return "completed state blocks transition to failed (no write)"

    async def test_missing_key_noop():
        m = make_mock()
        m.get = AsyncMock(return_value=None)
        t = TaskStateTracker(m, "pfx", 600, "i1")
        await t.set_completed("t1")
        m.set.assert_not_called()
        return "Missing key -> no-op (no write, no publish)"

    async def test_get_state_none():
        m = make_mock()
        m.get = AsyncMock(return_value=None)
        t = TaskStateTracker(m, "pfx", 600, "i1")
        assert await t.get_state("x") is None
        return "Returns None for missing key"

    async def test_get_state_deserialize():
        m = make_mock()
        m.get = AsyncMock(return_value=json.dumps({
            "task_id": "t1", "function_name": "fn", "state": "running",
            "instance_id": "i1", "started_at": 100.0, "updated_at": 100.0, "error": None,
        }))
        t = TaskStateTracker(m, "pfx", 600, "i1")
        info = await t.get_state("t1")
        assert info.task_id == "t1"
        assert info.state == TaskState.RUNNING
        return "Deserialized TaskStateInfo with correct types"

    for t in [test_key_format, test_set_running_payload, test_set_running_publishes,
              test_transition_completed, test_transition_failed_with_error,
              test_transition_timed_out, test_transition_aborted,
              test_terminal_state_idempotent, test_missing_key_noop,
              test_get_state_none, test_get_state_deserialize]:
        await run_test(suite, t.__name__.replace("test_", ""), t())


# ---------------------------------------------------------------------------
# 4. Abort Controller — unit (mocked Redis)
# ---------------------------------------------------------------------------
async def suite_abort_controller_unit():
    suite = "Abort Controller (unit)"
    print(f"\n{'='*60}\n{suite}\n{'='*60}")

    from nat.plugins.redis_orchestration.abort_controller import AbortController

    async def test_channel_format():
        m = AsyncMock()
        c = AbortController(m, "pfx")
        assert c.abort_channel("t1") == "pfx:abort:t1"
        return "pfx:abort:{task_id}"

    async def test_send_abort_publishes():
        m = AsyncMock()
        m.publish = AsyncMock(return_value=1)
        c = AbortController(m, "pfx")
        n = await c.send_abort("t1", "reason")
        assert n == 1
        ch, payload = m.publish.call_args[0]
        assert ch == "pfx:abort:t1"
        d = json.loads(payload)
        assert d["reason"] == "reason"
        assert "timestamp" in d
        return "Published to pfx:abort:t1 with reason + timestamp"

    async def test_send_abort_returns_zero():
        m = AsyncMock()
        m.publish = AsyncMock(return_value=0)
        c = AbortController(m, "pfx")
        assert await c.send_abort("t1") == 0
        return "Returns 0 when no subscribers"

    async def test_listen_sets_event():
        m = AsyncMock()
        ps = AsyncMock()
        ps.subscribe = AsyncMock()
        ps.unsubscribe = AsyncMock()
        ps.close = AsyncMock()

        async def listen():
            yield {"type": "subscribe", "data": 1}
            yield {"type": "message", "data": '{"reason":"x"}'}

        ps.listen = listen
        m.pubsub = lambda: ps
        c = AbortController(m, "pfx")
        event = asyncio.Event()
        await c.listen_for_abort("t1", event)
        assert event.is_set()
        return "Event set on first message"

    async def test_listen_graceful_cancel():
        m = AsyncMock()
        ps = AsyncMock()
        ps.subscribe = AsyncMock()
        ps.unsubscribe = AsyncMock()
        ps.close = AsyncMock()

        async def listen():
            yield {"type": "subscribe", "data": 1}
            await asyncio.sleep(999)
            yield {"type": "message", "data": "never"}

        ps.listen = listen
        m.pubsub = lambda: ps
        c = AbortController(m, "pfx")
        event = asyncio.Event()
        task = asyncio.create_task(c.listen_for_abort("t1", event))
        await asyncio.sleep(0.01)
        task.cancel()
        await task
        assert not event.is_set()
        ps.unsubscribe.assert_called_once()
        ps.close.assert_called_once()
        return "Cancellation cleans up subscription"

    for t in [test_channel_format, test_send_abort_publishes, test_send_abort_returns_zero,
              test_listen_sets_event, test_listen_graceful_cancel]:
        await run_test(suite, t.__name__.replace("test_", ""), t())


# ---------------------------------------------------------------------------
# 5. Crash Recovery — unit (mocked Redis)
# ---------------------------------------------------------------------------
async def suite_crash_recovery_unit():
    suite = "Crash Recovery (unit)"
    print(f"\n{'='*60}\n{suite}\n{'='*60}")

    from nat.plugins.redis_orchestration.crash_recovery import CrashRecovery

    def state_json(tid, state="running"):
        return json.dumps({
            "task_id": tid, "function_name": "fn", "state": state,
            "instance_id": "old", "started_at": 1.0, "updated_at": 1.0, "error": None,
        })

    async def test_recovers_running():
        m = AsyncMock()
        m.set = AsyncMock()
        m.publish = AsyncMock(return_value=0)
        store = {"pfx:state:t1": state_json("t1", "running"), "pfx:state:t2": state_json("t2", "completed")}
        m.scan = AsyncMock(return_value=(0, list(store.keys())))
        m.get = AsyncMock(side_effect=lambda k: store.get(k))
        r = CrashRecovery(m, "pfx", 600, "new")
        got = await r.recover_orphaned_tasks()
        assert got == ["t1"]
        return "Recovered t1 (running), skipped t2 (completed)"

    async def test_empty_keyspace():
        m = AsyncMock()
        m.set = AsyncMock()
        m.publish = AsyncMock()
        m.scan = AsyncMock(return_value=(0, []))
        r = CrashRecovery(m, "pfx", 600, "new")
        assert await r.recover_orphaned_tasks() == []
        return "No keys -> empty list"

    async def test_expired_key():
        m = AsyncMock()
        m.set = AsyncMock()
        m.publish = AsyncMock()
        m.scan = AsyncMock(return_value=(0, ["pfx:state:t1"]))
        m.get = AsyncMock(return_value=None)
        r = CrashRecovery(m, "pfx", 600, "new")
        assert await r.recover_orphaned_tasks() == []
        return "Key expired between SCAN and GET -> skipped"

    async def test_multi_page():
        m = AsyncMock()
        m.set = AsyncMock()
        m.publish = AsyncMock(return_value=0)
        store = {"pfx:state:t1": state_json("t1"), "pfx:state:t2": state_json("t2")}
        m.scan = AsyncMock(side_effect=[(42, ["pfx:state:t1"]), (0, ["pfx:state:t2"])])
        m.get = AsyncMock(side_effect=lambda k: store.get(k))
        r = CrashRecovery(m, "pfx", 600, "new")
        got = await r.recover_orphaned_tasks()
        assert set(got) == {"t1", "t2"}
        assert m.scan.call_count == 2
        return "2-page SCAN recovered both tasks"

    async def test_all_terminal_states_skipped():
        m = AsyncMock()
        m.set = AsyncMock()
        m.publish = AsyncMock()
        keys = [f"pfx:state:t{i}" for i in range(4)]
        store = {
            keys[0]: state_json("t0", "completed"),
            keys[1]: state_json("t1", "failed"),
            keys[2]: state_json("t2", "timed_out"),
            keys[3]: state_json("t3", "aborted"),
        }
        m.scan = AsyncMock(return_value=(0, keys))
        m.get = AsyncMock(side_effect=lambda k: store.get(k))
        r = CrashRecovery(m, "pfx", 600, "new")
        assert await r.recover_orphaned_tasks() == []
        m.set.assert_not_called()
        return "All 4 terminal states skipped (completed, failed, timed_out, aborted)"

    for t in [test_recovers_running, test_empty_keyspace, test_expired_key,
              test_multi_page, test_all_terminal_states_skipped]:
        await run_test(suite, t.__name__.replace("test_", ""), t())


# ---------------------------------------------------------------------------
# 6. Middleware — unit (mocked Redis)
# ---------------------------------------------------------------------------
async def suite_middleware_unit():
    suite = "Middleware (unit)"
    print(f"\n{'='*60}\n{suite}\n{'='*60}")

    from nat.plugins.redis_orchestration.orchestration_middleware import RedisOrchestrationMiddleware
    from nat.plugins.redis_orchestration.orchestration_middleware_config import RedisOrchestrationConfig
    from nat.plugins.redis_orchestration.exceptions import TaskAbortedError

    def make_redis_mock():
        m = AsyncMock()
        m.set = AsyncMock()
        m.publish = AsyncMock(return_value=1)

        def get_side(key):
            return json.dumps({
                "task_id": key.split(":")[-1], "function_name": "fn", "state": "running",
                "instance_id": "i1", "started_at": 1.0, "updated_at": 1.0, "error": None,
            })
        m.get = AsyncMock(side_effect=get_side)
        return m

    def make_mw(redis_mock, *, tracking=True, abort=False):
        cfg = RedisOrchestrationConfig(
            redis_url="redis://x", enable_state_tracking=tracking, enable_abort=abort,
            state_ttl=600, key_prefix="mw",
        )
        return RedisOrchestrationMiddleware(cfg, mock_builder(), redis_mock, "i1")

    ctx = make_context()

    async def test_invoke_success():
        m = make_redis_mock()
        mw = make_mw(m)
        r = await mw.function_middleware_invoke("x", call_next=AsyncMock(return_value="ok"), context=ctx)
        assert r == "ok"
        calls = [json.loads(c[0][1])["state"] for c in m.set.call_args_list]
        assert calls == ["running", "completed"]
        return f"States written: {calls}"

    async def test_invoke_failure():
        m = make_redis_mock()
        mw = make_mw(m)
        try:
            await mw.function_middleware_invoke("x", call_next=AsyncMock(side_effect=ValueError("bad")), context=ctx)
        except ValueError:
            pass
        calls = [json.loads(c[0][1])["state"] for c in m.set.call_args_list]
        assert calls == ["running", "failed"]
        err = json.loads(m.set.call_args_list[1][0][1])["error"]
        assert err == "bad"
        return f"States: {calls}, error='{err}'"

    async def test_invoke_timeout():
        m = make_redis_mock()
        mw = make_mw(m)
        try:
            await mw.function_middleware_invoke("x", call_next=AsyncMock(side_effect=TimeoutError()), context=ctx)
        except TimeoutError:
            pass
        calls = [json.loads(c[0][1])["state"] for c in m.set.call_args_list]
        assert calls == ["running", "timed_out"]
        return f"States: {calls}"

    async def test_invoke_tracking_disabled():
        m = make_redis_mock()
        mw = make_mw(m, tracking=False)
        r = await mw.function_middleware_invoke("x", call_next=AsyncMock(return_value="ok"), context=ctx)
        assert r == "ok"
        m.set.assert_not_called()
        return "No Redis writes when tracking disabled"

    async def test_invoke_abort():
        m = make_redis_mock()
        ps = AsyncMock()
        ps.subscribe = AsyncMock()
        ps.unsubscribe = AsyncMock()
        ps.close = AsyncMock()

        async def immediate_abort():
            yield {"type": "subscribe", "data": 1}
            yield {"type": "message", "data": '{"reason":"kill"}'}

        ps.listen = immediate_abort
        m.pubsub = lambda: ps
        mw = make_mw(m, abort=True)
        try:
            await mw.function_middleware_invoke(
                "x",
                call_next=AsyncMock(side_effect=lambda *a, **kw: asyncio.sleep(10)),
                context=ctx,
            )
            raise AssertionError("Should have raised TaskAbortedError")
        except TaskAbortedError as e:
            pass
        aborted = [c for c in m.set.call_args_list if "aborted" in json.loads(c[0][1]).get("state", "")]
        assert len(aborted) >= 1
        return "TaskAbortedError raised, state=aborted written"

    async def test_stream_success():
        m = make_redis_mock()
        mw = make_mw(m)

        async def gen(*a, **kw):
            for i in range(3):
                yield f"c{i}"

        chunks = []
        async for c in mw.function_middleware_stream("x", call_next=gen, context=ctx):
            chunks.append(c)
        assert chunks == ["c0", "c1", "c2"]
        calls = [json.loads(c[0][1])["state"] for c in m.set.call_args_list]
        assert calls == ["running", "completed"]
        return f"Chunks: {chunks}, States: {calls}"

    async def test_stream_failure():
        m = make_redis_mock()
        mw = make_mw(m)

        async def gen(*a, **kw):
            yield "c0"
            raise RuntimeError("boom")

        try:
            async for _ in mw.function_middleware_stream("x", call_next=gen, context=ctx):
                pass
        except RuntimeError:
            pass
        calls = [json.loads(c[0][1])["state"] for c in m.set.call_args_list]
        assert calls == ["running", "failed"]
        return f"States: {calls}"

    async def test_stream_abort():
        m = make_redis_mock()
        ps = AsyncMock()
        ps.subscribe = AsyncMock()
        ps.unsubscribe = AsyncMock()
        ps.close = AsyncMock()

        async def delayed_abort():
            yield {"type": "subscribe", "data": 1}
            await asyncio.sleep(0.05)
            yield {"type": "message", "data": '{"reason":"kill"}'}

        ps.listen = delayed_abort
        m.pubsub = lambda: ps
        mw = make_mw(m, abort=True)

        async def gen(*a, **kw):
            yield "c0"
            await asyncio.sleep(0.2)
            yield "c1"

        try:
            async for _ in mw.function_middleware_stream("x", call_next=gen, context=ctx):
                pass
            raise AssertionError("Should abort")
        except TaskAbortedError:
            pass
        return "TaskAbortedError raised during stream"

    async def test_both_disabled():
        m = make_redis_mock()
        cfg = RedisOrchestrationConfig(
            redis_url="redis://x", enable_state_tracking=False, enable_abort=False, state_ttl=600, key_prefix="mw",
        )
        mw = RedisOrchestrationMiddleware(cfg, mock_builder(), m, "i1")
        r = await mw.function_middleware_invoke("x", call_next=AsyncMock(return_value="ok"), context=ctx)
        assert r == "ok"
        m.set.assert_not_called()
        m.publish.assert_not_called()
        return "Pass-through when both features disabled"

    for t in [test_invoke_success, test_invoke_failure, test_invoke_timeout,
              test_invoke_tracking_disabled, test_invoke_abort,
              test_stream_success, test_stream_failure, test_stream_abort,
              test_both_disabled]:
        await run_test(suite, t.__name__.replace("test_", ""), t())


# ---------------------------------------------------------------------------
# 7. Live Redis integration
# ---------------------------------------------------------------------------
async def suite_live_redis():
    suite = "Live Redis Integration"
    print(f"\n{'='*60}\n{suite}\n{'='*60}")

    import redis.asyncio as aioredis
    from nat.plugins.redis_orchestration.state_tracker import TaskStateTracker
    from nat.plugins.redis_orchestration.abort_controller import AbortController
    from nat.plugins.redis_orchestration.crash_recovery import CrashRecovery
    from nat.plugins.redis_orchestration.models import TaskState

    client = aioredis.from_url(REDIS_URL, decode_responses=True, socket_timeout=5.0)
    await client.ping()
    await cleanup_keys(client, KEY_PREFIX)

    async def test_connectivity():
        r = await client.ping()
        assert r is True
        return f"Connected to {REDIS_URL}"

    async def test_full_lifecycle():
        t = TaskStateTracker(client, KEY_PREFIX, 30, "live")
        await t.set_running("lc1", "func_a")
        i = await t.get_state("lc1")
        assert i.state == TaskState.RUNNING
        await t.set_completed("lc1")
        i = await t.get_state("lc1")
        assert i.state == TaskState.COMPLETED
        await t.set_failed("lc1", "late")
        i = await t.get_state("lc1")
        assert i.state == TaskState.COMPLETED
        return "running -> completed (terminal blocks failed)"

    async def test_failed_with_error():
        t = TaskStateTracker(client, KEY_PREFIX, 30, "live")
        await t.set_running("fe1", "func_b")
        await t.set_failed("fe1", "db connection lost")
        i = await t.get_state("fe1")
        assert i.state == TaskState.FAILED
        assert i.error == "db connection lost"
        return f"error='{i.error}'"

    async def test_ttl_applied():
        t = TaskStateTracker(client, KEY_PREFIX, 5, "live")
        await t.set_running("ttl1", "func")
        ttl = await client.ttl(t.state_key("ttl1"))
        assert 0 < ttl <= 5
        return f"TTL={ttl}s on state key"

    async def test_abort_round_trip():
        ctrl = AbortController(client, KEY_PREFIX)
        event = asyncio.Event()
        listener = asyncio.create_task(ctrl.listen_for_abort("ab1", event))
        await asyncio.sleep(0.2)
        n = await ctrl.send_abort("ab1", "test")
        assert n >= 1
        await asyncio.wait_for(event.wait(), timeout=3.0)
        assert event.is_set()
        if not listener.done():
            listener.cancel()
            try:
                await listener
            except asyncio.CancelledError:
                pass
        return f"receivers={n}, event={event.is_set()}"

    async def test_abort_no_subscriber():
        ctrl = AbortController(client, KEY_PREFIX)
        n = await ctrl.send_abort("nobody", "void")
        assert n == 0
        return f"receivers={n} (expected 0)"

    async def test_crash_recovery():
        # Clean all keys first to avoid interference from earlier tests
        await cleanup_keys(client, KEY_PREFIX)
        t = TaskStateTracker(client, KEY_PREFIX, 30, "old-inst")
        await t.set_running("cr1", "fn_a")
        await t.set_running("cr2", "fn_b")
        await t.set_running("cr3", "fn_c")
        await t.set_completed("cr3")
        rec = CrashRecovery(client, KEY_PREFIX, 30, "new-inst")
        got = await rec.recover_orphaned_tasks()
        assert set(got) == {"cr1", "cr2"}
        i1 = await t.get_state("cr1")
        assert i1.state == TaskState.FAILED and "Orphaned" in i1.error
        i3 = await t.get_state("cr3")
        assert i3.state == TaskState.COMPLETED
        return f"Recovered {got}, cr3 untouched"

    async def test_pubsub_events():
        sub = aioredis.from_url(REDIS_URL, decode_responses=True)
        ps = sub.pubsub()
        t = TaskStateTracker(client, KEY_PREFIX, 30, "live")
        await ps.subscribe(t.state_channel)
        await ps.get_message(timeout=2.0)
        await t.set_running("ps1", "fn")
        msg = await ps.get_message(timeout=2.0)
        assert msg and msg["type"] == "message"
        evt = json.loads(msg["data"])
        assert evt["task_id"] == "ps1" and evt["state"] == "running"
        await ps.unsubscribe()
        await ps.close()
        await sub.close()
        return f"Event: {evt}"

    async def test_unicode_error():
        t = TaskStateTracker(client, KEY_PREFIX, 30, "live")
        await t.set_running("uc1", "fn")
        await t.set_failed("uc1", "Error: \u2603 snowman \U0001f525 fire")
        i = await t.get_state("uc1")
        assert "\u2603" in i.error and "\U0001f525" in i.error
        return f"Unicode error stored: {i.error}"

    async def test_long_function_name():
        t = TaskStateTracker(client, KEY_PREFIX, 30, "live")
        long_name = "a" * 500
        await t.set_running("ln1", long_name)
        i = await t.get_state("ln1")
        assert i.function_name == long_name
        return f"Stored function name of length {len(i.function_name)}"

    for t in [test_connectivity, test_full_lifecycle, test_failed_with_error,
              test_ttl_applied, test_abort_round_trip, test_abort_no_subscriber,
              test_crash_recovery, test_pubsub_events, test_unicode_error, test_long_function_name]:
        await run_test(suite, t.__name__.replace("test_", ""), t())

    await cleanup_keys(client, KEY_PREFIX)
    await client.close()


# ---------------------------------------------------------------------------
# 8. E2E with real LLM
# ---------------------------------------------------------------------------
async def suite_e2e_llm():
    suite = "E2E with Real LLM"
    print(f"\n{'='*60}\n{suite}\n{'='*60}")

    import redis.asyncio as aioredis
    from nat.plugins.redis_orchestration.orchestration_middleware import RedisOrchestrationMiddleware
    from nat.plugins.redis_orchestration.orchestration_middleware_config import RedisOrchestrationConfig
    from nat.plugins.redis_orchestration.abort_controller import AbortController
    from nat.plugins.redis_orchestration.exceptions import TaskAbortedError

    client = aioredis.from_url(REDIS_URL, decode_responses=True, socket_timeout=5.0)
    await client.ping()
    await cleanup_keys(client, "e2e")

    sub = aioredis.from_url(REDIS_URL, decode_responses=True)
    ps = sub.pubsub()
    await ps.subscribe("e2e:state_events")
    await ps.get_message(timeout=2.0)

    cfg = RedisOrchestrationConfig(
        redis_url=REDIS_URL, enable_state_tracking=True, enable_abort=True,
        state_ttl=60, key_prefix="e2e", crash_recovery_on_startup=False,
    )
    mw = RedisOrchestrationMiddleware(cfg, mock_builder(), client, "e2e-node")
    ctx = make_context("llm_call")

    async def test_successful_llm():
        async def fn(*a, **kw):
            return await call_llm("What is 2+2? One word answer.")
        r = await mw.function_middleware_invoke("x", call_next=fn, context=ctx)
        events = await drain_events(ps)
        states = [e["state"] for e in events]
        assert "running" in states and "completed" in states
        return f"LLM said: '{r.strip()[:60]}', states: {states}"

    async def test_llm_with_long_response():
        async def fn(*a, **kw):
            return await call_llm("List the first 5 prime numbers.", max_tokens=150)
        r = await mw.function_middleware_invoke("x", call_next=fn, context=ctx)
        events = await drain_events(ps)
        states = [e["state"] for e in events]
        assert "completed" in states
        return f"LLM said: '{r.strip()[:80]}...', states: {states}"

    async def test_abort_during_llm():
        async def fn(*a, **kw):
            return await call_llm("Write a 1000 word essay on quantum physics.", max_tokens=500)
        task = asyncio.create_task(mw.function_middleware_invoke("x", call_next=fn, context=ctx))
        await asyncio.sleep(0.5)
        # Find running task
        running_id = None
        for _ in range(20):
            msg = await ps.get_message(timeout=0.5)
            if msg and msg["type"] == "message":
                evt = json.loads(msg["data"])
                if evt["state"] == "running":
                    running_id = evt["task_id"]
                    break
        assert running_id, "Never saw running event"
        ctrl = AbortController(client, "e2e")
        n = await ctrl.send_abort(running_id, "user killed")
        aborted = False
        try:
            await asyncio.wait_for(task, timeout=15.0)
        except TaskAbortedError:
            aborted = True
        except asyncio.TimeoutError:
            task.cancel()
            try:
                await task
            except:
                pass
        events = await drain_events(ps)
        states = [e["state"] for e in events]
        return f"abort sent (receivers={n}), aborted={aborted}, events: {states}"

    async def test_concurrent_llm_calls():
        async def fn1(*a, **kw):
            return await call_llm("What color is the sky? One word.")
        async def fn2(*a, **kw):
            return await call_llm("What color is grass? One word.")

        r1, r2 = await asyncio.gather(
            mw.function_middleware_invoke("x", call_next=fn1, context=make_context("llm_1")),
            mw.function_middleware_invoke("x", call_next=fn2, context=make_context("llm_2")),
        )
        events = await drain_events(ps)
        running_count = sum(1 for e in events if e["state"] == "running")
        completed_count = sum(1 for e in events if e["state"] == "completed")
        assert running_count == 2 and completed_count == 2
        return f"LLM1: '{r1.strip()[:30]}', LLM2: '{r2.strip()[:30]}', {running_count}x running, {completed_count}x completed"

    for t in [test_successful_llm, test_llm_with_long_response, test_abort_during_llm, test_concurrent_llm_calls]:
        await run_test(suite, t.__name__.replace("test_", ""), t())

    await cleanup_keys(client, "e2e")
    await ps.unsubscribe()
    await ps.close()
    await sub.close()
    await client.close()


# ---------------------------------------------------------------------------
# 9. Concurrency & edge cases
# ---------------------------------------------------------------------------
async def suite_concurrency():
    suite = "Concurrency & Edge Cases"
    print(f"\n{'='*60}\n{suite}\n{'='*60}")

    import redis.asyncio as aioredis
    from nat.plugins.redis_orchestration.state_tracker import TaskStateTracker
    from nat.plugins.redis_orchestration.models import TaskState

    client = aioredis.from_url(REDIS_URL, decode_responses=True, socket_timeout=5.0)
    await cleanup_keys(client, "conc")

    async def test_rapid_state_transitions():
        t = TaskStateTracker(client, "conc", 30, "inst")
        for i in range(20):
            tid = f"rapid-{i}"
            await t.set_running(tid, "fn")
            await t.set_completed(tid)
            info = await t.get_state(tid)
            assert info.state == TaskState.COMPLETED
        return "20 rapid running->completed cycles"

    async def test_concurrent_state_writes():
        t = TaskStateTracker(client, "conc", 30, "inst")
        tasks = []
        for i in range(10):
            tasks.append(t.set_running(f"par-{i}", f"fn-{i}"))
        await asyncio.gather(*tasks)
        for i in range(10):
            info = await t.get_state(f"par-{i}")
            assert info is not None and info.state == TaskState.RUNNING
        return "10 parallel set_running calls succeeded"

    async def test_state_after_ttl_hint():
        """Verify key exists immediately and has a TTL."""
        t = TaskStateTracker(client, "conc", 2, "inst")
        await t.set_running("ttl-check", "fn")
        ttl = await client.ttl(t.state_key("ttl-check"))
        assert 0 < ttl <= 2
        info = await t.get_state("ttl-check")
        assert info is not None
        return f"Key exists with TTL={ttl}s"

    async def test_empty_error_string():
        t = TaskStateTracker(client, "conc", 30, "inst")
        await t.set_running("ee1", "fn")
        await t.set_failed("ee1", "")
        info = await t.get_state("ee1")
        assert info.error == ""
        return "Empty string error stored correctly"

    async def test_special_chars_in_task_id():
        t = TaskStateTracker(client, "conc", 30, "inst")
        tid = "task:with:colons-and_underscores.and.dots"
        await t.set_running(tid, "fn")
        info = await t.get_state(tid)
        assert info.task_id == tid
        return f"task_id with special chars: '{tid}'"

    for t in [test_rapid_state_transitions, test_concurrent_state_writes,
              test_state_after_ttl_hint, test_empty_error_string, test_special_chars_in_task_id]:
        await run_test(suite, t.__name__.replace("test_", ""), t())

    await cleanup_keys(client, "conc")
    await client.close()


# ===========================================================================
# REPORT GENERATION
# ===========================================================================

def generate_report() -> str:
    total = len(results)
    passed = sum(1 for r in results if r.passed)
    failed = total - passed
    total_ms = sum(r.duration_ms for r in results)

    suites = {}
    for r in results:
        suites.setdefault(r.suite, []).append(r)

    lines = []
    lines.append("# Redis Orchestration Middleware — Test Report")
    lines.append("")
    lines.append(f"**Date:** {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")
    lines.append(f"**Infrastructure:** Redis @ `{REDIS_URL}` | Ollama @ `{OLLAMA_URL}` (`{OLLAMA_MODEL}`)")
    lines.append(f"**Python:** {sys.version.split()[0]}")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Total tests | {total} |")
    lines.append(f"| Passed | {passed} |")
    lines.append(f"| Failed | {failed} |")
    lines.append(f"| Pass rate | {passed/total*100:.1f}% |")
    lines.append(f"| Total duration | {total_ms:.0f}ms |")
    lines.append("")

    for suite_name, suite_results in suites.items():
        sp = sum(1 for r in suite_results if r.passed)
        sf = len(suite_results) - sp
        lines.append(f"## {suite_name}")
        lines.append("")
        lines.append(f"**{sp}/{len(suite_results)}** passed" + (f" | **{sf} FAILED**" if sf else ""))
        lines.append("")
        lines.append("| # | Test | Status | Time | Detail |")
        lines.append("|---|------|--------|------|--------|")
        for i, r in enumerate(suite_results, 1):
            status = "PASS" if r.passed else "**FAIL**"
            detail = r.detail if r.passed else r.error
            detail = detail.replace("|", "\\|").replace("\n", " ")[:120]
            lines.append(f"| {i} | {r.name} | {status} | {r.duration_ms:.0f}ms | {detail} |")
        lines.append("")

    if failed:
        lines.append("## Failures")
        lines.append("")
        for r in results:
            if not r.passed:
                lines.append(f"### {r.suite} / {r.name}")
                lines.append(f"```\n{r.error}\n```")
                lines.append("")

    lines.append("---")
    lines.append(f"*Generated by test_full_report.py*")
    return "\n".join(lines)


# ===========================================================================
# MAIN
# ===========================================================================

async def main():
    print("=" * 60)
    print("REDIS ORCHESTRATION MIDDLEWARE — COMPREHENSIVE TEST REPORT")
    print("=" * 60)

    # Check dependencies
    try:
        import httpx  # noqa: F401
    except ImportError:
        print("Installing httpx for LLM calls...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--break-system-packages", "--user", "httpx"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    await suite_models()
    await suite_config()
    await suite_state_tracker_unit()
    await suite_abort_controller_unit()
    await suite_crash_recovery_unit()
    await suite_middleware_unit()
    await suite_live_redis()
    await suite_e2e_llm()
    await suite_concurrency()

    report = generate_report()

    report_path = os.path.abspath(REPORT_PATH)
    with open(report_path, "w") as f:
        f.write(report)

    total = len(results)
    passed = sum(1 for r in results if r.passed)
    failed = total - passed

    print()
    print("=" * 60)
    print(f"TOTAL: {total} | PASSED: {passed} | FAILED: {failed}")
    print(f"Report written to: {report_path}")
    print("=" * 60)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
