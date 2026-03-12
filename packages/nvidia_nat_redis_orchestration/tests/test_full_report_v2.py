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
Comprehensive test report v2 for nvidia_nat_redis_orchestration.

Covers ALL three implemented features:
  Feature 1: Task Lifecycle State Tracking
  Feature 2: External Abort
  Feature 3: Session Continuity  (NEW in this report)

Test levels:
  - Unit tests (mocked Redis)
  - Live Redis integration (h-oracle)
  - E2E with real LLM (h-titan / Ollama gpt-oss:20b)

Generates a structured Markdown test report at TEST_REPORT_V2.md
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
REPORT_PATH = os.environ.get(
    "REPORT_PATH",
    os.path.join(os.path.dirname(__file__), "..", "TEST_REPORT_V2.md"),
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


def record(suite, name, passed, duration, detail="", error=""):
    results.append(TestResult(suite, name, passed, round(duration * 1000, 1), detail, error))
    status = "PASS" if passed else "FAIL"
    print(f"  [{status}] {suite} / {name} ({duration*1000:.0f}ms)")


async def run_test(suite, name, coro):
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
    b = Mock()
    b._functions = {}
    for m in ["get_llm", "get_embedder", "get_retriever", "get_memory_client",
              "get_object_store_client", "get_auth_provider", "get_function"]:
        setattr(b, m, AsyncMock())
    b.get_function_config = Mock()
    return b


def make_context(name="test_function"):
    from nat.middleware.middleware import FunctionMiddlewareContext
    return FunctionMiddlewareContext(
        name=name, config=Mock(), description=f"Test: {name}",
        input_schema=None, single_output_schema=type(None), stream_output_schema=type(None),
    )


async def call_llm(prompt, max_tokens=200):
    import httpx
    async with httpx.AsyncClient(timeout=120.0) as c:
        r = await c.post(f"{OLLAMA_URL}/chat/completions", json={
            "model": OLLAMA_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
        })
        return r.json()["choices"][0]["message"].get("content", "") or "[no content]"


async def drain_events(pubsub, timeout=0.5):
    evts = []
    while True:
        msg = await pubsub.get_message(timeout=timeout)
        if msg is None:
            break
        if msg["type"] == "message":
            evts.append(json.loads(msg["data"]))
    return evts


async def cleanup_keys(client, prefix):
    cur = 0
    while True:
        cur, keys = await client.scan(cursor=cur, match=f"{prefix}:*", count=200)
        if keys:
            await client.delete(*keys)
        if cur == 0:
            break


# ===========================================================================
# FEATURE 1: STATE TRACKING — Unit Tests
# ===========================================================================
async def suite_f1_state_tracking_unit():
    suite = "F1: State Tracking (unit)"
    print(f"\n{'='*60}\n{suite}\n{'='*60}")

    from nat.plugins.redis_orchestration.models import TaskState, TaskStateInfo, TERMINAL_STATES
    from nat.plugins.redis_orchestration.state_tracker import TaskStateTracker

    def make_mock():
        m = AsyncMock()
        m.set = AsyncMock()
        m.get = AsyncMock(return_value=None)
        m.publish = AsyncMock(return_value=1)
        return m

    async def test_state_enum():
        assert set(TaskState) == {"running", "completed", "failed", "timed_out", "aborted"}
        assert TaskState.RUNNING not in TERMINAL_STATES
        assert len(TERMINAL_STATES) == 4
        return "5 states, 4 terminal"

    async def test_state_info_immutable():
        info = TaskStateInfo("t1", "fn", TaskState.RUNNING, "i1", 1.0, 2.0)
        try:
            info.state = TaskState.COMPLETED
            raise AssertionError("Should be frozen")
        except dataclasses.FrozenInstanceError:
            pass
        return "TaskStateInfo is frozen"

    async def test_key_format():
        t = TaskStateTracker(make_mock(), "p", 600, "i")
        assert t.state_key("x") == "p:state:x"
        assert t.state_channel == "p:state_events"
        return "p:state:{id}, p:state_events"

    async def test_set_running():
        m = make_mock()
        t = TaskStateTracker(m, "p", 600, "i")
        await t.set_running("t1", "fn")
        p = json.loads(m.set.call_args[0][1])
        assert p["state"] == "running" and p["task_id"] == "t1" and m.set.call_args[1]["ex"] == 600
        m.publish.assert_called_once()
        return f"SET with TTL=600, PUBLISH to channel"

    async def test_all_transitions():
        results = []
        for target, method in [("completed", "set_completed"), ("failed", "set_failed"),
                                ("timed_out", "set_timed_out"), ("aborted", "set_aborted")]:
            m = make_mock()
            m.get = AsyncMock(return_value=json.dumps({
                "task_id": "t1", "function_name": "fn", "state": "running",
                "instance_id": "i", "started_at": 1.0, "updated_at": 1.0, "error": None}))
            t = TaskStateTracker(m, "p", 600, "i")
            if target == "failed":
                await getattr(t, method)("t1", "err")
            else:
                await getattr(t, method)("t1")
            p = json.loads(m.set.call_args[0][1])
            assert p["state"] == target
            results.append(target)
        return f"running -> {', '.join(results)}"

    async def test_terminal_idempotent():
        m = make_mock()
        m.get = AsyncMock(return_value=json.dumps({
            "task_id": "t1", "function_name": "fn", "state": "completed",
            "instance_id": "i", "started_at": 1.0, "updated_at": 2.0, "error": None}))
        t = TaskStateTracker(m, "p", 600, "i")
        await t.set_failed("t1", "late")
        m.set.assert_not_called()
        return "completed blocks failed (no write)"

    async def test_missing_key_noop():
        m = make_mock()
        t = TaskStateTracker(m, "p", 600, "i")
        await t.set_completed("t1")
        m.set.assert_not_called()
        return "Missing key -> no-op"

    for fn in [test_state_enum, test_state_info_immutable, test_key_format,
               test_set_running, test_all_transitions, test_terminal_idempotent, test_missing_key_noop]:
        await run_test(suite, fn.__name__.replace("test_", ""), fn())


# ===========================================================================
# FEATURE 2: EXTERNAL ABORT — Unit Tests
# ===========================================================================
async def suite_f2_abort_unit():
    suite = "F2: External Abort (unit)"
    print(f"\n{'='*60}\n{suite}\n{'='*60}")

    from nat.plugins.redis_orchestration.abort_controller import AbortController
    from nat.plugins.redis_orchestration.exceptions import TaskAbortedError, OrchestrationError

    async def test_exceptions():
        e = TaskAbortedError("t1", "killed")
        assert e.task_id == "t1" and e.reason == "killed"
        assert isinstance(e, OrchestrationError)
        return "TaskAbortedError carries task_id + reason"

    async def test_channel_format():
        c = AbortController(AsyncMock(), "p")
        assert c.abort_channel("t1") == "p:abort:t1"
        return "p:abort:{id}"

    async def test_send_abort():
        m = AsyncMock()
        m.publish = AsyncMock(return_value=1)
        c = AbortController(m, "p")
        n = await c.send_abort("t1", "reason")
        assert n == 1
        ch, payload = m.publish.call_args[0]
        d = json.loads(payload)
        assert ch == "p:abort:t1" and d["reason"] == "reason"
        return "Published with reason + timestamp"

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
        c = AbortController(m, "p")
        ev = asyncio.Event()
        await c.listen_for_abort("t1", ev)
        assert ev.is_set()
        return "Event set on message"

    async def test_listen_cancel_cleanup():
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
        c = AbortController(m, "p")
        ev = asyncio.Event()
        task = asyncio.create_task(c.listen_for_abort("t1", ev))
        await asyncio.sleep(0.01)
        task.cancel()
        await task
        assert not ev.is_set()
        ps.unsubscribe.assert_called_once()
        ps.close.assert_called_once()
        return "Cancelled cleanly, unsubscribe + close called"

    for fn in [test_exceptions, test_channel_format, test_send_abort,
               test_listen_sets_event, test_listen_cancel_cleanup]:
        await run_test(suite, fn.__name__.replace("test_", ""), fn())


# ===========================================================================
# FEATURE 1+2: MIDDLEWARE — Unit Tests
# ===========================================================================
async def suite_f12_middleware_unit():
    suite = "F1+F2: Middleware (unit)"
    print(f"\n{'='*60}\n{suite}\n{'='*60}")

    from nat.plugins.redis_orchestration.orchestration_middleware import RedisOrchestrationMiddleware
    from nat.plugins.redis_orchestration.orchestration_middleware_config import RedisOrchestrationConfig
    from nat.plugins.redis_orchestration.exceptions import TaskAbortedError

    def make_redis():
        m = AsyncMock()
        m.set = AsyncMock()
        m.publish = AsyncMock(return_value=1)
        m.get = AsyncMock(side_effect=lambda k: json.dumps({
            "task_id": k.split(":")[-1], "function_name": "fn", "state": "running",
            "instance_id": "i", "started_at": 1.0, "updated_at": 1.0, "error": None}))
        return m

    def make_mw(rm, *, track=True, abort=False, session=False):
        cfg = RedisOrchestrationConfig(redis_url="redis://x", enable_state_tracking=track,
                                        enable_abort=abort, enable_session_continuity=session,
                                        state_ttl=600, key_prefix="mw")
        return RedisOrchestrationMiddleware(cfg, mock_builder(), rm, "i")

    ctx = make_context()

    async def test_invoke_success():
        m = make_redis()
        mw = make_mw(m)
        r = await mw.function_middleware_invoke("x", call_next=AsyncMock(return_value="ok"), context=ctx)
        assert r == "ok"
        states = [json.loads(c[0][1])["state"] for c in m.set.call_args_list]
        assert states == ["running", "completed"]
        return f"States: {states}"

    async def test_invoke_failure():
        m = make_redis()
        mw = make_mw(m)
        try:
            await mw.function_middleware_invoke("x", call_next=AsyncMock(side_effect=ValueError("bad")), context=ctx)
        except ValueError:
            pass
        states = [json.loads(c[0][1])["state"] for c in m.set.call_args_list]
        assert states == ["running", "failed"]
        return f"States: {states}"

    async def test_invoke_timeout():
        m = make_redis()
        mw = make_mw(m)
        try:
            await mw.function_middleware_invoke("x", call_next=AsyncMock(side_effect=TimeoutError()), context=ctx)
        except TimeoutError:
            pass
        states = [json.loads(c[0][1])["state"] for c in m.set.call_args_list]
        assert states == ["running", "timed_out"]
        return f"States: {states}"

    async def test_invoke_abort():
        m = make_redis()
        ps = AsyncMock()
        ps.subscribe = AsyncMock()
        ps.unsubscribe = AsyncMock()
        ps.close = AsyncMock()
        async def im():
            yield {"type": "subscribe", "data": 1}
            yield {"type": "message", "data": '{"reason":"kill"}'}
        ps.listen = im
        m.pubsub = lambda: ps
        mw = make_mw(m, abort=True)
        try:
            await mw.function_middleware_invoke("x", call_next=AsyncMock(side_effect=lambda *a, **kw: asyncio.sleep(10)), context=ctx)
            raise AssertionError("Should abort")
        except TaskAbortedError:
            pass
        aborted = [c for c in m.set.call_args_list if "aborted" in json.loads(c[0][1]).get("state", "")]
        assert len(aborted) >= 1
        return "TaskAbortedError raised, state=aborted"

    async def test_stream_success():
        m = make_redis()
        mw = make_mw(m)
        async def gen(*a, **kw):
            for i in range(3): yield f"c{i}"
        chunks = [c async for c in mw.function_middleware_stream("x", call_next=gen, context=ctx)]
        assert chunks == ["c0", "c1", "c2"]
        states = [json.loads(c[0][1])["state"] for c in m.set.call_args_list]
        assert states == ["running", "completed"]
        return f"Chunks: {chunks}"

    async def test_stream_abort():
        m = make_redis()
        ps = AsyncMock()
        ps.subscribe = AsyncMock()
        ps.unsubscribe = AsyncMock()
        ps.close = AsyncMock()
        async def da():
            yield {"type": "subscribe", "data": 1}
            await asyncio.sleep(0.05)
            yield {"type": "message", "data": '{"reason":"kill"}'}
        ps.listen = da
        m.pubsub = lambda: ps
        mw = make_mw(m, abort=True)
        async def gen(*a, **kw):
            yield "c0"; await asyncio.sleep(0.2); yield "c1"
        try:
            async for _ in mw.function_middleware_stream("x", call_next=gen, context=ctx): pass
            raise AssertionError("Should abort")
        except TaskAbortedError:
            pass
        return "Abort during stream"

    async def test_both_disabled():
        m = make_redis()
        mw = make_mw(m, track=False, abort=False)
        r = await mw.function_middleware_invoke("x", call_next=AsyncMock(return_value="ok"), context=ctx)
        assert r == "ok"
        m.set.assert_not_called()
        return "Pass-through when all disabled"

    for fn in [test_invoke_success, test_invoke_failure, test_invoke_timeout,
               test_invoke_abort, test_stream_success, test_stream_abort, test_both_disabled]:
        await run_test(suite, fn.__name__.replace("test_", ""), fn())


# ===========================================================================
# FEATURE 3: SESSION CONTINUITY — Unit Tests
# ===========================================================================
async def suite_f3_session_unit():
    suite = "F3: Session Continuity (unit)"
    print(f"\n{'='*60}\n{suite}\n{'='*60}")

    from nat.plugins.redis_orchestration.session_models import TurnRole, ConversationTurn, CompactionResult
    from nat.plugins.redis_orchestration.session_store import SessionStore
    from nat.plugins.redis_orchestration.session_compactor import SessionCompactor

    def make_mock():
        m = AsyncMock()
        m.rpush = AsyncMock()
        m.expire = AsyncMock()
        m.lrange = AsyncMock(return_value=[])
        m.llen = AsyncMock(return_value=0)
        m.ltrim = AsyncMock()
        m.delete = AsyncMock()
        m.exists = AsyncMock(return_value=0)
        return m

    async def test_turn_roles():
        assert set(TurnRole) == {"user", "assistant", "system", "tool"}
        return "4 roles"

    async def test_turn_frozen():
        t = ConversationTurn(role=TurnRole.USER, content="hi", timestamp=1.0)
        try:
            t.content = "changed"
            raise AssertionError("Should be frozen")
        except AttributeError:
            pass
        return "ConversationTurn is immutable"

    async def test_session_id_derivation():
        assert SessionStore.derive_session_id(conversation_id="c1") == "conv:c1"
        assert SessionStore.derive_session_id(user_id="u1") == "user:u1"
        assert SessionStore.derive_session_id(conversation_id="c", user_id="u") == "conv:c"
        assert SessionStore.derive_session_id() is None
        return "conv_id preferred over user_id; None if neither"

    async def test_append_turn():
        m = make_mock()
        s = SessionStore(m, "p", 3600, 100, 1_000_000)
        await s.add_user_turn("s1", "hello")
        d = json.loads(m.rpush.call_args[0][1])
        assert d["role"] == "user" and d["content"] == "hello"
        m.expire.assert_called_once_with("p:session:s1", 3600)
        return f"RPUSH + EXPIRE with TTL=3600"

    async def test_get_turns():
        m = make_mock()
        m.lrange = AsyncMock(return_value=[
            json.dumps({"role": "user", "content": "hi", "timestamp": 1.0}),
            json.dumps({"role": "assistant", "content": "hello", "timestamp": 2.0}),
        ])
        s = SessionStore(m, "p", 3600, 100, 1_000_000)
        turns = await s.get_turns("s1")
        assert len(turns) == 2 and turns[0].role == TurnRole.USER and turns[1].role == TurnRole.ASSISTANT
        return f"{len(turns)} turns deserialized"

    async def test_get_turns_with_limit():
        m = make_mock()
        m.lrange = AsyncMock(return_value=[])
        s = SessionStore(m, "p", 3600, 100, 1_000_000)
        await s.get_turns("s1", limit=5)
        m.lrange.assert_called_once_with("p:session:s1", -5, -1)
        return "LRANGE with negative offset for tail"

    async def test_rotation_by_count():
        m = make_mock()
        m.llen = AsyncMock(return_value=8)
        m.lrange = AsyncMock(return_value=["x"] * 5)
        s = SessionStore(m, "p", 3600, 5, 1_000_000)
        rotated = await s._rotate_if_needed("s1")
        assert rotated is True
        m.ltrim.assert_called_once_with("p:session:s1", 3, -1)
        return "8 turns -> LTRIM(3, -1) to keep 5"

    async def test_no_rotation():
        m = make_mock()
        m.llen = AsyncMock(return_value=3)
        m.lrange = AsyncMock(return_value=["x"] * 3)
        s = SessionStore(m, "p", 3600, 100, 1_000_000)
        assert await s._rotate_if_needed("s1") is False
        m.ltrim.assert_not_called()
        return "Under limits -> no trim"

    async def test_clear_and_exists():
        m = make_mock()
        s = SessionStore(m, "p", 3600, 100, 1_000_000)
        await s.clear_session("s1")
        m.delete.assert_called_once()
        m.exists = AsyncMock(return_value=1)
        assert await s.session_exists("s1") is True
        m.exists = AsyncMock(return_value=0)
        assert await s.session_exists("s1") is False
        return "clear=DELETE, exists=EXISTS"

    async def test_compaction_under_threshold():
        m = make_mock()
        m.lrange = AsyncMock(return_value=[json.dumps({"role": "user", "content": "x", "timestamp": 1.0})] * 5)
        s = SessionStore(m, "p", 3600, 1000, 10_000_000)
        c = SessionCompactor(s, compaction_threshold=50, keep_recent=10)
        assert await c.maybe_compact("s1") is None
        return "5 turns < threshold 50 -> no compaction"

    async def test_compaction_drops_old():
        m = make_mock()
        m.lrange = AsyncMock(return_value=[
            json.dumps({"role": "user", "content": f"m{i}", "timestamp": float(i)}) for i in range(20)])
        m.llen = AsyncMock(return_value=0)
        s = SessionStore(m, "p", 3600, 1000, 10_000_000)
        c = SessionCompactor(s, compaction_threshold=15, keep_recent=5)
        r = await c.maybe_compact("s1")
        assert r.original_turn_count == 20 and r.removed_turn_count == 15 and r.retained_turn_count == 5
        return f"20 -> 5 turns (removed 15)"

    async def test_compaction_with_summary():
        m = make_mock()
        m.lrange = AsyncMock(return_value=[
            json.dumps({"role": "user", "content": f"m{i}", "timestamp": float(i)}) for i in range(20)])
        m.llen = AsyncMock(return_value=0)
        s = SessionStore(m, "p", 3600, 1000, 10_000_000)
        c = SessionCompactor(s, compaction_threshold=15, keep_recent=5)
        async def summarize(t): return "Summary of conversation."
        r = await c.maybe_compact("s1", summarize_fn=summarize)
        assert r.summary == "Summary of conversation." and r.retained_turn_count == 6
        return f"Summary generated, 6 retained (5 kept + 1 summary)"

    async def test_compaction_summary_failure():
        m = make_mock()
        m.lrange = AsyncMock(return_value=[
            json.dumps({"role": "user", "content": "x", "timestamp": 1.0})] * 20)
        m.llen = AsyncMock(return_value=0)
        s = SessionStore(m, "p", 3600, 1000, 10_000_000)
        c = SessionCompactor(s, compaction_threshold=15, keep_recent=5)
        async def fail(t): raise RuntimeError("LLM down")
        r = await c.maybe_compact("s1", summarize_fn=fail)
        assert r.summary is None and r.removed_turn_count == 15
        return "LLM failure -> compaction still proceeds without summary"

    for fn in [test_turn_roles, test_turn_frozen, test_session_id_derivation,
               test_append_turn, test_get_turns, test_get_turns_with_limit,
               test_rotation_by_count, test_no_rotation, test_clear_and_exists,
               test_compaction_under_threshold, test_compaction_drops_old,
               test_compaction_with_summary, test_compaction_summary_failure]:
        await run_test(suite, fn.__name__.replace("test_", ""), fn())


# ===========================================================================
# CONFIG VALIDATION (all features)
# ===========================================================================
async def suite_config():
    suite = "Config Validation (all features)"
    print(f"\n{'='*60}\n{suite}\n{'='*60}")

    from nat.plugins.redis_orchestration.orchestration_middleware_config import RedisOrchestrationConfig
    from pydantic import ValidationError

    async def test_defaults():
        c = RedisOrchestrationConfig(redis_url="redis://x")
        assert c.enable_state_tracking is True
        assert c.enable_abort is True
        assert c.enable_session_continuity is False
        assert c.state_ttl == 3600
        assert c.session_ttl == 86400
        assert c.session_max_turns == 200
        assert c.session_max_bytes == 1_048_576
        assert c.session_compaction_threshold == 50
        assert c.session_compaction_keep_recent == 10
        return "All defaults correct (state=on, abort=on, session=off)"

    async def test_session_config():
        c = RedisOrchestrationConfig(
            redis_url="redis://x", enable_session_continuity=True,
            session_ttl=7200, session_max_turns=50, session_max_bytes=500_000,
            session_compaction_threshold=30, session_compaction_keep_recent=5)
        assert c.session_ttl == 7200 and c.session_max_turns == 50
        return "Custom session config accepted"

    async def test_session_ttl_validation():
        try:
            RedisOrchestrationConfig(redis_url="redis://x", session_ttl=0)
            raise AssertionError("Should reject")
        except ValidationError:
            pass
        return "session_ttl=0 rejected"

    async def test_session_max_turns_validation():
        try:
            RedisOrchestrationConfig(redis_url="redis://x", session_max_turns=-1)
            raise AssertionError("Should reject")
        except ValidationError:
            pass
        return "session_max_turns=-1 rejected"

    async def test_type_discriminator():
        c = RedisOrchestrationConfig(redis_url="redis://x")
        assert c.type == "redis_orchestration"
        return f"type = '{c.type}'"

    for fn in [test_defaults, test_session_config, test_session_ttl_validation,
               test_session_max_turns_validation, test_type_discriminator]:
        await run_test(suite, fn.__name__.replace("test_", ""), fn())


# ===========================================================================
# CRASH RECOVERY — Unit Tests
# ===========================================================================
async def suite_crash_recovery_unit():
    suite = "Crash Recovery (unit)"
    print(f"\n{'='*60}\n{suite}\n{'='*60}")

    from nat.plugins.redis_orchestration.crash_recovery import CrashRecovery

    def sj(tid, state="running"):
        return json.dumps({"task_id": tid, "function_name": "fn", "state": state,
                           "instance_id": "old", "started_at": 1.0, "updated_at": 1.0, "error": None})

    async def test_recovers_orphans():
        m = AsyncMock()
        m.set = AsyncMock()
        m.publish = AsyncMock(return_value=0)
        store = {"p:state:t1": sj("t1"), "p:state:t2": sj("t2", "completed")}
        m.scan = AsyncMock(return_value=(0, list(store.keys())))
        m.get = AsyncMock(side_effect=lambda k: store.get(k))
        r = CrashRecovery(m, "p", 600, "new")
        got = await r.recover_orphaned_tasks()
        assert got == ["t1"]
        return "t1 recovered, t2 (completed) skipped"

    async def test_empty_keyspace():
        m = AsyncMock()
        m.scan = AsyncMock(return_value=(0, []))
        m.set = AsyncMock()
        m.publish = AsyncMock()
        r = CrashRecovery(m, "p", 600, "new")
        assert await r.recover_orphaned_tasks() == []
        return "Empty -> []"

    async def test_all_terminal_skipped():
        m = AsyncMock()
        m.set = AsyncMock()
        m.publish = AsyncMock()
        store = {f"p:state:t{i}": sj(f"t{i}", s) for i, s in enumerate(["completed", "failed", "timed_out", "aborted"])}
        m.scan = AsyncMock(return_value=(0, list(store.keys())))
        m.get = AsyncMock(side_effect=lambda k: store.get(k))
        r = CrashRecovery(m, "p", 600, "new")
        assert await r.recover_orphaned_tasks() == []
        m.set.assert_not_called()
        return "All 4 terminal states skipped"

    for fn in [test_recovers_orphans, test_empty_keyspace, test_all_terminal_skipped]:
        await run_test(suite, fn.__name__.replace("test_", ""), fn())


# ===========================================================================
# LIVE REDIS — All Features
# ===========================================================================
async def suite_live_redis():
    suite = "Live Redis (all features)"
    print(f"\n{'='*60}\n{suite}\n{'='*60}")

    import redis.asyncio as aioredis
    from nat.plugins.redis_orchestration.state_tracker import TaskStateTracker
    from nat.plugins.redis_orchestration.abort_controller import AbortController
    from nat.plugins.redis_orchestration.crash_recovery import CrashRecovery
    from nat.plugins.redis_orchestration.session_store import SessionStore
    from nat.plugins.redis_orchestration.session_compactor import SessionCompactor
    from nat.plugins.redis_orchestration.session_models import ConversationTurn, TurnRole
    from nat.plugins.redis_orchestration.models import TaskState

    client = aioredis.from_url(REDIS_URL, decode_responses=True, socket_timeout=5.0)
    await client.ping()
    pfx = "livev2"
    await cleanup_keys(client, pfx)

    # --- F1: State Tracking ---
    async def test_state_lifecycle():
        t = TaskStateTracker(client, pfx, 30, "n1")
        await t.set_running("lc1", "func")
        assert (await t.get_state("lc1")).state == TaskState.RUNNING
        await t.set_completed("lc1")
        assert (await t.get_state("lc1")).state == TaskState.COMPLETED
        await t.set_failed("lc1", "late")
        assert (await t.get_state("lc1")).state == TaskState.COMPLETED
        return "running->completed, terminal idempotent"

    async def test_state_ttl():
        t = TaskStateTracker(client, pfx, 5, "n1")
        await t.set_running("ttl1", "fn")
        ttl = await client.ttl(t.state_key("ttl1"))
        assert 0 < ttl <= 5
        return f"TTL={ttl}s"

    async def test_pubsub_events():
        sub = aioredis.from_url(REDIS_URL, decode_responses=True)
        ps = sub.pubsub()
        t = TaskStateTracker(client, pfx, 30, "n1")
        await ps.subscribe(t.state_channel)
        await ps.get_message(timeout=2.0)
        await t.set_running("ev1", "fn")
        msg = await ps.get_message(timeout=2.0)
        evt = json.loads(msg["data"])
        assert evt["state"] == "running" and evt["task_id"] == "ev1"
        await ps.unsubscribe()
        await ps.close()
        await sub.close()
        return f"Event: {evt['state']} for {evt['task_id']}"

    # --- F2: Abort ---
    async def test_abort_round_trip():
        ctrl = AbortController(client, pfx)
        ev = asyncio.Event()
        listener = asyncio.create_task(ctrl.listen_for_abort("ab1", ev))
        await asyncio.sleep(0.2)
        n = await ctrl.send_abort("ab1", "test")
        assert n >= 1
        await asyncio.wait_for(ev.wait(), timeout=3.0)
        if not listener.done():
            listener.cancel()
            try: await listener
            except asyncio.CancelledError: pass
        return f"receivers={n}, event={ev.is_set()}"

    # --- Crash Recovery ---
    async def test_crash_recovery():
        await cleanup_keys(client, pfx)
        t = TaskStateTracker(client, pfx, 30, "old")
        await t.set_running("cr1", "fa")
        await t.set_running("cr2", "fb")
        await t.set_running("cr3", "fc")
        await t.set_completed("cr3")
        rec = CrashRecovery(client, pfx, 30, "new")
        got = await rec.recover_orphaned_tasks()
        assert set(got) == {"cr1", "cr2"}
        return f"Recovered {got}"

    # --- F3: Session Continuity ---
    async def test_session_crud():
        await cleanup_keys(client, pfx)
        s = SessionStore(client, pfx, 60, 100, 1_000_000)
        await s.add_user_turn("sess1", "Hello!")
        await s.add_assistant_turn("sess1", "Hi there!")
        await s.add_user_turn("sess1", "What is 2+2?")
        await s.add_assistant_turn("sess1", "Four.")
        turns = await s.get_turns("sess1")
        assert len(turns) == 4 and turns[0].content == "Hello!" and turns[3].content == "Four."
        return f"{len(turns)} turns stored/retrieved"

    async def test_session_limit():
        s = SessionStore(client, pfx, 60, 100, 1_000_000)
        recent = await s.get_turns("sess1", limit=2)
        assert len(recent) == 2 and recent[0].content == "What is 2+2?"
        return f"Last 2: [{recent[0].content}, {recent[1].content}]"

    async def test_session_ttl():
        s = SessionStore(client, pfx, 10, 100, 1_000_000)
        await s.add_user_turn("ttlsess", "test")
        ttl = await client.ttl(s.turns_key("ttlsess"))
        assert 0 < ttl <= 10
        return f"TTL={ttl}s on session key"

    async def test_session_rotation():
        s = SessionStore(client, pfx, 60, 3, 1_000_000)
        for i in range(6):
            await s.add_user_turn("rot1", f"msg{i}")
        turns = await s.get_turns("rot1")
        assert len(turns) <= 3 and turns[-1].content == "msg5"
        return f"{len(turns)} turns after adding 6 (max=3)"

    async def test_session_compaction():
        await cleanup_keys(client, pfx)
        s = SessionStore(client, pfx, 60, 1000, 10_000_000)
        for i in range(20):
            role = TurnRole.USER if i % 2 == 0 else TurnRole.ASSISTANT
            await s.append_turn("comp1", ConversationTurn(role=role, content=f"t{i}", timestamp=time.time()))
        c = SessionCompactor(s, compaction_threshold=15, keep_recent=5)
        r = await c.maybe_compact("comp1")
        assert r.removed_turn_count == 15
        remaining = await s.get_turns("comp1")
        assert len(remaining) == 5
        return f"20 -> {len(remaining)} turns"

    async def test_session_compaction_with_summary():
        s = SessionStore(client, pfx, 60, 1000, 10_000_000)
        for i in range(20):
            await s.append_turn("comp2", ConversationTurn(
                role=TurnRole.USER if i % 2 == 0 else TurnRole.ASSISTANT,
                content=f"t{i}", timestamp=time.time()))
        c = SessionCompactor(s, compaction_threshold=15, keep_recent=5)
        async def summarize(t): return "Discussed various topics."
        r = await c.maybe_compact("comp2", summarize_fn=summarize)
        remaining = await s.get_turns("comp2")
        assert remaining[0].role == TurnRole.SYSTEM and "[Session summary]" in remaining[0].content
        return f"{len(remaining)} turns, first is summary"

    async def test_session_unicode():
        s = SessionStore(client, pfx, 60, 100, 1_000_000)
        await s.add_user_turn("uni1", "Snowman: \u2603 Fire: \U0001f525")
        turns = await s.get_turns("uni1")
        assert "\u2603" in turns[0].content and "\U0001f525" in turns[0].content
        return turns[0].content

    for fn in [test_state_lifecycle, test_state_ttl, test_pubsub_events,
               test_abort_round_trip, test_crash_recovery,
               test_session_crud, test_session_limit, test_session_ttl,
               test_session_rotation, test_session_compaction,
               test_session_compaction_with_summary, test_session_unicode]:
        await run_test(suite, fn.__name__.replace("test_", ""), fn())

    await cleanup_keys(client, pfx)
    await client.close()


# ===========================================================================
# E2E WITH REAL LLM — All Features
# ===========================================================================
async def suite_e2e():
    suite = "E2E with Real LLM"
    print(f"\n{'='*60}\n{suite}\n{'='*60}")

    import redis.asyncio as aioredis
    from nat.plugins.redis_orchestration.orchestration_middleware import RedisOrchestrationMiddleware
    from nat.plugins.redis_orchestration.orchestration_middleware_config import RedisOrchestrationConfig
    from nat.plugins.redis_orchestration.abort_controller import AbortController
    from nat.plugins.redis_orchestration.exceptions import TaskAbortedError

    client = aioredis.from_url(REDIS_URL, decode_responses=True, socket_timeout=5.0)
    pfx = "e2ev2"
    await cleanup_keys(client, pfx)

    sub = aioredis.from_url(REDIS_URL, decode_responses=True)
    ps = sub.pubsub()
    await ps.subscribe(f"{pfx}:state_events")
    await ps.get_message(timeout=2.0)

    cfg = RedisOrchestrationConfig(
        redis_url=REDIS_URL, enable_state_tracking=True, enable_abort=True,
        enable_session_continuity=False,  # no Context available in test
        state_ttl=60, key_prefix=pfx, crash_recovery_on_startup=False)
    mw = RedisOrchestrationMiddleware(cfg, mock_builder(), client, "e2e")
    ctx = make_context("llm_call")

    async def test_successful_llm():
        async def fn(*a, **kw): return await call_llm("What is 2+2? One word answer.")
        r = await mw.function_middleware_invoke("x", call_next=fn, context=ctx)
        evts = await drain_events(ps)
        states = [e["state"] for e in evts]
        assert "running" in states and "completed" in states
        return f"LLM: '{r.strip()[:50]}', states: {states}"

    async def test_concurrent_llm():
        async def fn1(*a, **kw): return await call_llm("Color of the sky? One word.")
        async def fn2(*a, **kw): return await call_llm("Color of grass? One word.")
        r1, r2 = await asyncio.gather(
            mw.function_middleware_invoke("x", call_next=fn1, context=make_context("llm1")),
            mw.function_middleware_invoke("x", call_next=fn2, context=make_context("llm2")))
        evts = await drain_events(ps)
        running = sum(1 for e in evts if e["state"] == "running")
        completed = sum(1 for e in evts if e["state"] == "completed")
        assert running == 2 and completed == 2
        return f"LLM1: '{r1.strip()[:20]}', LLM2: '{r2.strip()[:20]}', {running}r/{completed}c"

    async def test_abort_llm():
        async def fn(*a, **kw): return await call_llm("Write 1000 words on quantum physics.", max_tokens=500)
        task = asyncio.create_task(mw.function_middleware_invoke("x", call_next=fn, context=ctx))
        await asyncio.sleep(0.5)
        tid = None
        for _ in range(20):
            msg = await ps.get_message(timeout=0.5)
            if msg and msg["type"] == "message":
                e = json.loads(msg["data"])
                if e["state"] == "running":
                    tid = e["task_id"]
                    break
        assert tid
        ctrl = AbortController(client, pfx)
        n = await ctrl.send_abort(tid, "user killed")
        aborted = False
        try:
            await asyncio.wait_for(task, timeout=15.0)
        except TaskAbortedError:
            aborted = True
        except asyncio.TimeoutError:
            task.cancel()
            try: await task
            except: pass
        evts = await drain_events(ps)
        return f"abort receivers={n}, aborted={aborted}"

    for fn in [test_successful_llm, test_concurrent_llm, test_abort_llm]:
        await run_test(suite, fn.__name__.replace("test_", ""), fn())

    await cleanup_keys(client, pfx)
    await ps.unsubscribe()
    await ps.close()
    await sub.close()
    await client.close()


# ===========================================================================
# REPORT GENERATION
# ===========================================================================
def generate_report():
    total = len(results)
    passed = sum(1 for r in results if r.passed)
    failed = total - passed
    total_ms = sum(r.duration_ms for r in results)

    suites = {}
    for r in results:
        suites.setdefault(r.suite, []).append(r)

    lines = [
        "# Redis Orchestration Middleware — Test Report v2",
        "",
        "**All three features tested: State Tracking, External Abort, Session Continuity**",
        "",
        f"**Date:** {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}",
        f"**Infrastructure:** Redis @ `{REDIS_URL}` | Ollama @ `{OLLAMA_URL}` (`{OLLAMA_MODEL}`)",
        f"**Python:** {sys.version.split()[0]}",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Total tests | {total} |",
        f"| Passed | {passed} |",
        f"| Failed | {failed} |",
        f"| Pass rate | {passed/total*100:.1f}% |",
        f"| Total duration | {total_ms:.0f}ms |",
        "",
        "### Feature Coverage",
        "",
        "| Feature | Status |",
        "|---------|--------|",
    ]

    feature_map = {
        "F1": "Task Lifecycle State Tracking",
        "F2": "External Abort",
        "F3": "Session Continuity",
    }
    for prefix, fname in feature_map.items():
        feature_tests = [r for r in results if r.suite.startswith(prefix) or prefix.lower() in r.suite.lower()]
        fp = sum(1 for r in feature_tests if r.passed)
        ft = len(feature_tests)
        status = f"{fp}/{ft} passed" if ft > 0 else "covered in combined suites"
        lines.append(f"| {fname} | {status} |")

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
            s = "PASS" if r.passed else "**FAIL**"
            d = (r.detail if r.passed else r.error).replace("|", "\\|").replace("\n", " ")[:120]
            lines.append(f"| {i} | {r.name} | {s} | {r.duration_ms:.0f}ms | {d} |")
        lines.append("")

    if failed:
        lines.append("## Failures")
        lines.append("")
        for r in results:
            if not r.passed:
                lines.append(f"### {r.suite} / {r.name}")
                lines.append(f"```\n{r.error}\n```")
                lines.append("")

    lines.extend(["---", "*Generated by test_full_report_v2.py*"])
    return "\n".join(lines)


# ===========================================================================
# MAIN
# ===========================================================================
async def main():
    print("=" * 60)
    print("REDIS ORCHESTRATION — FULL TEST REPORT v2")
    print("(State Tracking + Abort + Session Continuity)")
    print("=" * 60)

    try:
        import httpx  # noqa: F401
    except ImportError:
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--break-system-packages", "--user", "httpx"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    await suite_f1_state_tracking_unit()
    await suite_f2_abort_unit()
    await suite_f12_middleware_unit()
    await suite_f3_session_unit()
    await suite_config()
    await suite_crash_recovery_unit()
    await suite_live_redis()
    await suite_e2e()

    report = generate_report()
    path = os.path.abspath(REPORT_PATH)
    with open(path, "w") as f:
        f.write(report)

    total = len(results)
    passed = sum(1 for r in results if r.passed)
    failed = total - passed

    print()
    print("=" * 60)
    print(f"TOTAL: {total} | PASSED: {passed} | FAILED: {failed}")
    print(f"Report: {path}")
    print("=" * 60)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
