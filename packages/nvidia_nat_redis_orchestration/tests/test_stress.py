#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""
Intensive stress test for nvidia_nat_redis_orchestration.

Runs 1000+ operations across all features concurrently:
  - 500 state tracking cycles (running -> terminal)
  - 200 abort scenarios (fire, race, no-op)
  - 300 session turns across 10 sessions with rotation + compaction
  - 50 real LLM calls through the middleware
  - Durability checks (data persists, nothing lost)

Designed to run for extended periods on h-oracle (2x5090, local Redis).
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import string
import sys
import time
import traceback

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434/v1")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen3:32b")
PREFIX = "stress"


class Stats:
    def __init__(self):
        self.total = 0
        self.passed = 0
        self.failed = 0
        self.errors: list[str] = []
        self.timings: dict[str, list[float]] = {}
        self.start_time = time.time()

    def ok(self, category: str, duration: float = 0):
        self.total += 1
        self.passed += 1
        self.timings.setdefault(category, []).append(duration)

    def fail(self, category: str, error: str):
        self.total += 1
        self.failed += 1
        self.errors.append(f"[{category}] {error}")
        if len(self.errors) <= 20:
            print(f"  FAIL [{category}]: {error[:200]}")

    def summary(self) -> str:
        elapsed = time.time() - self.start_time
        lines = [
            "=" * 70,
            "STRESS TEST RESULTS",
            "=" * 70,
            f"Duration: {elapsed:.1f}s",
            f"Total operations: {self.total}",
            f"Passed: {self.passed}",
            f"Failed: {self.failed}",
            f"Pass rate: {self.passed/self.total*100:.1f}%" if self.total else "N/A",
            "",
        ]
        for cat, times in sorted(self.timings.items()):
            avg = sum(times) / len(times) * 1000
            mx = max(times) * 1000
            lines.append(f"  {cat}: {len(times)} ops, avg={avg:.1f}ms, max={mx:.1f}ms")
        if self.errors:
            lines.append(f"\nFirst {min(20, len(self.errors))} errors:")
            for e in self.errors[:20]:
                lines.append(f"  {e[:200]}")
        lines.append("=" * 70)
        return "\n".join(lines)


stats = Stats()


async def call_llm(prompt: str, max_tokens: int = 100) -> str:
    import httpx
    async with httpx.AsyncClient(timeout=120.0) as c:
        r = await c.post(f"{OLLAMA_URL}/chat/completions", json={
            "model": OLLAMA_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
        })
        return r.json()["choices"][0]["message"].get("content", "") or "[no content]"


# ===========================================================================
# 1. STATE TRACKING STRESS — 500 cycles
# ===========================================================================
async def stress_state_tracking(client):
    from nat.plugins.redis_orchestration.state_tracker import TaskStateTracker
    from nat.plugins.redis_orchestration.models import TaskState

    print("\n[1/5] State Tracking Stress: 500 concurrent state cycles...")
    tracker = TaskStateTracker(client, PREFIX, state_ttl=300, instance_id="stress-node")

    terminals = [
        ("completed", tracker.set_completed),
        ("failed", lambda tid: tracker.set_failed(tid, "stress error")),
        ("timed_out", tracker.set_timed_out),
        ("aborted", tracker.set_aborted),
    ]

    async def one_cycle(i: int):
        tid = f"state-{i:04d}"
        target_name, target_fn = random.choice(terminals)
        t0 = time.time()
        try:
            await tracker.set_running(tid, f"func_{i}")
            info = await tracker.get_state(tid)
            if info is None or info.state != TaskState.RUNNING:
                stats.fail("state_running", f"{tid}: expected RUNNING, got {info}")
                return

            await target_fn(tid)
            info = await tracker.get_state(tid)
            if info is None:
                stats.fail("state_terminal", f"{tid}: state disappeared after transition")
                return
            if info.state.value != target_name:
                stats.fail("state_terminal", f"{tid}: expected {target_name}, got {info.state}")
                return

            # Verify idempotency — try another transition, should be no-op
            await tracker.set_failed(tid, "late failure")
            info2 = await tracker.get_state(tid)
            if info2.state.value != target_name:
                stats.fail("state_idempotent", f"{tid}: state changed from {target_name} to {info2.state}")
                return

            stats.ok("state_cycle", time.time() - t0)
        except Exception as e:
            stats.fail("state_cycle", f"{tid}: {type(e).__name__}: {e}")

    # Run in batches of 50 to avoid overwhelming Redis
    for batch_start in range(0, 500, 50):
        await asyncio.gather(*[one_cycle(i) for i in range(batch_start, min(batch_start + 50, 500))])
    print(f"  Done: {stats.passed} passed so far")


# ===========================================================================
# 2. ABORT STRESS — 200 scenarios
# ===========================================================================
async def stress_abort(client):
    from nat.plugins.redis_orchestration.abort_controller import AbortController

    print("\n[2/5] Abort Stress: 200 abort scenarios...")
    ctrl = AbortController(client, PREFIX)

    # 2a: 100 successful abort round-trips
    async def abort_round_trip(i: int):
        tid = f"abort-{i:04d}"
        event = asyncio.Event()
        t0 = time.time()
        try:
            listener = asyncio.create_task(ctrl.listen_for_abort(tid, event))
            await asyncio.sleep(0.05)  # Let subscription establish
            n = await ctrl.send_abort(tid, f"stress abort {i}")
            if n < 1:
                stats.fail("abort_roundtrip", f"{tid}: no receivers (got {n})")
                listener.cancel()
                try:
                    await listener
                except asyncio.CancelledError:
                    pass
                return
            await asyncio.wait_for(event.wait(), timeout=5.0)
            if not event.is_set():
                stats.fail("abort_roundtrip", f"{tid}: event not set")
            else:
                stats.ok("abort_roundtrip", time.time() - t0)
        except asyncio.TimeoutError:
            stats.fail("abort_roundtrip", f"{tid}: timed out waiting for event")
        except Exception as e:
            stats.fail("abort_roundtrip", f"{tid}: {type(e).__name__}: {e}")
        finally:
            if not listener.done():
                listener.cancel()
                try:
                    await listener
                except asyncio.CancelledError:
                    pass

    # Run round-trips in batches of 20
    for batch_start in range(0, 100, 20):
        await asyncio.gather(*[abort_round_trip(i) for i in range(batch_start, min(batch_start + 20, 100))])

    # 2b: 100 aborts with no listener (should return 0 receivers)
    async def abort_no_listener(i: int):
        t0 = time.time()
        try:
            n = await ctrl.send_abort(f"nobody-{i:04d}", "void")
            if n != 0:
                stats.fail("abort_no_listener", f"expected 0 receivers, got {n}")
            else:
                stats.ok("abort_no_listener", time.time() - t0)
        except Exception as e:
            stats.fail("abort_no_listener", f"{type(e).__name__}: {e}")

    await asyncio.gather(*[abort_no_listener(i) for i in range(100)])
    print(f"  Done: {stats.passed} passed so far")


# ===========================================================================
# 3. SESSION CONTINUITY STRESS — 10 sessions x 100 turns + rotation + compaction
# ===========================================================================
async def stress_sessions(client):
    from nat.plugins.redis_orchestration.session_store import SessionStore
    from nat.plugins.redis_orchestration.session_compactor import SessionCompactor
    from nat.plugins.redis_orchestration.session_models import TurnRole

    print("\n[3/5] Session Continuity Stress: 10 sessions x 100 turns...")
    store = SessionStore(client, PREFIX, session_ttl=600, max_turns=200, max_bytes=5_000_000)

    # 3a: Write 100 turns to each of 10 sessions
    async def fill_session(session_idx: int):
        sid = f"sess-{session_idx:02d}"
        t0 = time.time()
        try:
            for turn_idx in range(100):
                content = f"Turn {turn_idx} in session {session_idx}: {''.join(random.choices(string.ascii_letters, k=50))}"
                if turn_idx % 2 == 0:
                    await store.add_user_turn(sid, content)
                else:
                    await store.add_assistant_turn(sid, content)

            # Verify all 100 turns are there
            turns = await store.get_turns(sid)
            if len(turns) != 100:
                stats.fail("session_fill", f"{sid}: expected 100 turns, got {len(turns)}")
                return

            # Verify order preserved
            for j, turn in enumerate(turns):
                expected_role = TurnRole.USER if j % 2 == 0 else TurnRole.ASSISTANT
                if turn.role != expected_role:
                    stats.fail("session_order", f"{sid} turn {j}: expected {expected_role}, got {turn.role}")
                    return

            stats.ok("session_fill", time.time() - t0)
        except Exception as e:
            stats.fail("session_fill", f"{sid}: {type(e).__name__}: {e}")

    await asyncio.gather(*[fill_session(i) for i in range(10)])

    # 3b: Verify data persists — re-read all sessions
    print("  Verifying persistence...")
    for i in range(10):
        sid = f"sess-{i:02d}"
        t0 = time.time()
        try:
            turns = await store.get_turns(sid)
            if len(turns) != 100:
                stats.fail("session_persist", f"{sid}: expected 100, got {len(turns)}")
            else:
                stats.ok("session_persist", time.time() - t0)
        except Exception as e:
            stats.fail("session_persist", f"{sid}: {type(e).__name__}: {e}")

    # 3c: Verify get_turns with limit
    for i in range(10):
        sid = f"sess-{i:02d}"
        t0 = time.time()
        try:
            recent = await store.get_turns(sid, limit=10)
            if len(recent) != 10:
                stats.fail("session_limit", f"{sid}: expected 10, got {len(recent)}")
            else:
                stats.ok("session_limit", time.time() - t0)
        except Exception as e:
            stats.fail("session_limit", f"{sid}: {type(e).__name__}: {e}")

    # 3d: Rotation stress — small max_turns
    print("  Testing rotation under load...")
    small_store = SessionStore(client, PREFIX, session_ttl=600, max_turns=20, max_bytes=5_000_000)
    for i in range(5):
        sid = f"rot-{i:02d}"
        t0 = time.time()
        try:
            for j in range(100):
                await small_store.add_user_turn(sid, f"rot turn {j}")
            turns = await small_store.get_turns(sid)
            if len(turns) > 20:
                stats.fail("session_rotation", f"{sid}: {len(turns)} turns exceeds max 20")
            elif turns[-1].content != "rot turn 99":
                stats.fail("session_rotation", f"{sid}: last turn wrong: {turns[-1].content}")
            else:
                stats.ok("session_rotation", time.time() - t0)
        except Exception as e:
            stats.fail("session_rotation", f"{sid}: {type(e).__name__}: {e}")

    # 3e: Compaction stress
    print("  Testing compaction...")
    comp_store = SessionStore(client, PREFIX, session_ttl=600, max_turns=500, max_bytes=50_000_000)
    compactor = SessionCompactor(comp_store, compaction_threshold=30, keep_recent=10)
    for i in range(5):
        sid = f"comp-{i:02d}"
        t0 = time.time()
        try:
            for j in range(50):
                await comp_store.add_user_turn(sid, f"comp turn {j} with some padding: {'x' * 100}")
            result = await compactor.maybe_compact(sid)
            if result is None:
                stats.fail("session_compaction", f"{sid}: compaction didn't trigger")
            else:
                remaining = await comp_store.get_turns(sid)
                if len(remaining) != 10:
                    stats.fail("session_compaction", f"{sid}: expected 10 after compaction, got {len(remaining)}")
                else:
                    stats.ok("session_compaction", time.time() - t0)
        except Exception as e:
            stats.fail("session_compaction", f"{sid}: {type(e).__name__}: {e}")

    # 3f: Compaction with summary
    async def fake_summarize(transcript):
        return f"Summary of {len(transcript)} chars of conversation."

    for i in range(5):
        sid = f"comps-{i:02d}"
        t0 = time.time()
        try:
            for j in range(50):
                await comp_store.add_user_turn(sid, f"summary turn {j}")
            result = await compactor.maybe_compact(sid, summarize_fn=fake_summarize)
            remaining = await comp_store.get_turns(sid)
            if remaining[0].role != TurnRole.SYSTEM or "[Session summary]" not in remaining[0].content:
                stats.fail("session_compact_summary", f"{sid}: first turn not a summary")
            elif len(remaining) != 11:  # 10 kept + 1 summary
                stats.fail("session_compact_summary", f"{sid}: expected 11, got {len(remaining)}")
            else:
                stats.ok("session_compact_summary", time.time() - t0)
        except Exception as e:
            stats.fail("session_compact_summary", f"{sid}: {type(e).__name__}: {e}")

    # 3g: Large message stress
    print("  Testing large messages...")
    for i in range(10):
        sid = f"large-{i:02d}"
        t0 = time.time()
        try:
            big_content = "A" * 10_000  # 10KB message
            await store.add_user_turn(sid, big_content)
            turns = await store.get_turns(sid)
            if len(turns[0].content) != 10_000:
                stats.fail("session_large", f"{sid}: content truncated to {len(turns[0].content)}")
            else:
                stats.ok("session_large", time.time() - t0)
        except Exception as e:
            stats.fail("session_large", f"{sid}: {type(e).__name__}: {e}")

    # 3h: Unicode stress
    for i in range(10):
        sid = f"unicode-{i:02d}"
        t0 = time.time()
        try:
            content = f"Emoji: \U0001f525\U0001f4a5\u2603 CJK: \u4f60\u597d Japanese: \u3053\u3093\u306b\u3061\u306f Arabic: \u0645\u0631\u062d\u0628\u0627 #{i}"
            await store.add_user_turn(sid, content)
            turns = await store.get_turns(sid)
            if turns[0].content != content:
                stats.fail("session_unicode", f"{sid}: content mismatch")
            else:
                stats.ok("session_unicode", time.time() - t0)
        except Exception as e:
            stats.fail("session_unicode", f"{sid}: {type(e).__name__}: {e}")

    # 3i: Concurrent writes to same session
    print("  Testing concurrent writes to same session...")
    sid = "concurrent-session"
    t0 = time.time()
    try:
        tasks = [store.add_user_turn(sid, f"concurrent msg {i}") for i in range(50)]
        await asyncio.gather(*tasks)
        turns = await store.get_turns(sid)
        if len(turns) != 50:
            stats.fail("session_concurrent", f"expected 50 turns, got {len(turns)}")
        else:
            stats.ok("session_concurrent", time.time() - t0)
    except Exception as e:
        stats.fail("session_concurrent", f"{type(e).__name__}: {e}")

    print(f"  Done: {stats.passed} passed so far")


# ===========================================================================
# 4. CRASH RECOVERY STRESS
# ===========================================================================
async def stress_crash_recovery(client):
    from nat.plugins.redis_orchestration.state_tracker import TaskStateTracker
    from nat.plugins.redis_orchestration.crash_recovery import CrashRecovery
    from nat.plugins.redis_orchestration.models import TaskState

    print("\n[4/5] Crash Recovery Stress: 100 orphaned tasks...")
    tracker = TaskStateTracker(client, f"{PREFIX}cr", state_ttl=300, instance_id="old-crash")

    # Create 100 orphaned running tasks + 50 completed (should be skipped)
    t0 = time.time()
    try:
        tasks = []
        for i in range(100):
            tasks.append(tracker.set_running(f"orphan-{i:04d}", f"func_{i}"))
        for i in range(50):
            tasks.append(tracker.set_running(f"done-{i:04d}", f"func_done_{i}"))
        await asyncio.gather(*tasks)

        # Complete the 50 "done" tasks
        for i in range(50):
            await tracker.set_completed(f"done-{i:04d}")

        # Run crash recovery
        recovery = CrashRecovery(client, f"{PREFIX}cr", state_ttl=300, instance_id="new-recovery")
        recovered = await recovery.recover_orphaned_tasks()

        if len(recovered) != 100:
            stats.fail("crash_recovery", f"expected 100 recovered, got {len(recovered)}")
        else:
            # Verify all orphans are now FAILED
            all_failed = True
            for i in range(100):
                info = await tracker.get_state(f"orphan-{i:04d}")
                if info is None or info.state != TaskState.FAILED:
                    all_failed = False
                    break
            # Verify completed tasks untouched
            all_completed = True
            for i in range(50):
                info = await tracker.get_state(f"done-{i:04d}")
                if info is None or info.state != TaskState.COMPLETED:
                    all_completed = False
                    break

            if all_failed and all_completed:
                stats.ok("crash_recovery", time.time() - t0)
            else:
                stats.fail("crash_recovery", f"failed={all_failed}, completed={all_completed}")
    except Exception as e:
        stats.fail("crash_recovery", f"{type(e).__name__}: {e}")

    print(f"  Done: {stats.passed} passed so far")


# ===========================================================================
# 5. LLM MIDDLEWARE STRESS — 50 real LLM calls
# ===========================================================================
async def stress_llm_middleware(client):
    from nat.plugins.redis_orchestration.orchestration_middleware import RedisOrchestrationMiddleware
    from nat.plugins.redis_orchestration.orchestration_middleware_config import RedisOrchestrationConfig
    from nat.middleware.middleware import FunctionMiddlewareContext
    from unittest.mock import Mock, AsyncMock

    print(f"\n[5/5] LLM Middleware Stress: 50 real LLM calls via {OLLAMA_MODEL}...")

    cfg = RedisOrchestrationConfig(
        redis_url=REDIS_URL, enable_state_tracking=True, enable_abort=False,
        enable_session_continuity=False, state_ttl=300, key_prefix=f"{PREFIX}llm",
        crash_recovery_on_startup=False)

    builder = Mock()
    builder._functions = {}
    for m in ["get_llm", "get_embedder", "get_retriever", "get_memory_client",
              "get_object_store_client", "get_auth_provider", "get_function"]:
        setattr(builder, m, AsyncMock())
    builder.get_function_config = Mock()

    mw = RedisOrchestrationMiddleware(cfg, builder, client, "stress-llm")

    # Subscribe to state events
    sub_client = await get_sub_client()
    ps = sub_client.pubsub()
    await ps.subscribe(f"{PREFIX}llm:state_events")
    await ps.get_message(timeout=2.0)

    prompts = [
        "What is 2+2?", "Name a color.", "Say hello.", "What day follows Monday?",
        "Name a fruit.", "What is the capital of France?", "Is water wet?",
        "Name a planet.", "What comes after 5?", "Name an animal.",
    ] * 5  # 50 prompts

    completed = 0
    failed_calls = 0

    async def one_llm_call(i: int, prompt: str):
        nonlocal completed, failed_calls
        ctx = FunctionMiddlewareContext(
            name=f"llm_{i}", config=Mock(), description=f"LLM call {i}",
            input_schema=None, single_output_schema=type(None), stream_output_schema=type(None))

        async def fn(*a, **kw):
            return await call_llm(prompt, max_tokens=50)

        t0 = time.time()
        try:
            result = await mw.function_middleware_invoke("x", call_next=fn, context=ctx)
            if result and len(result.strip()) > 0:
                stats.ok("llm_call", time.time() - t0)
                completed += 1
            else:
                stats.fail("llm_call", f"call {i}: empty response")
                failed_calls += 1
        except Exception as e:
            stats.fail("llm_call", f"call {i}: {type(e).__name__}: {e}")
            failed_calls += 1

    # Run in batches of 10
    for batch_start in range(0, 50, 10):
        batch = [(i, prompts[i]) for i in range(batch_start, min(batch_start + 10, 50))]
        await asyncio.gather(*[one_llm_call(i, p) for i, p in batch])
        print(f"    Batch {batch_start//10 + 1}/5 done ({completed} completed, {failed_calls} failed)")

    # Verify state events were published
    await asyncio.sleep(0.5)
    events = []
    while True:
        msg = await ps.get_message(timeout=0.5)
        if msg is None:
            break
        if msg["type"] == "message":
            events.append(json.loads(msg["data"]))

    running_events = sum(1 for e in events if e["state"] == "running")
    completed_events = sum(1 for e in events if e["state"] == "completed")

    if running_events >= completed and completed_events >= completed:
        stats.ok("llm_state_events", 0)
    else:
        stats.fail("llm_state_events", f"expected {completed}+ running/completed events, got {running_events}/{completed_events}")

    await ps.unsubscribe()
    await ps.close()
    await sub_client.close()

    print(f"  Done: {completed} LLM calls completed, {running_events} running events, {completed_events} completed events")


# ===========================================================================
# Helper
# ===========================================================================
async def get_sub_client():
    import redis.asyncio as aioredis
    return aioredis.from_url(REDIS_URL, decode_responses=True, socket_timeout=10.0)


# ===========================================================================
# MAIN
# ===========================================================================
async def main():
    import redis.asyncio as aioredis

    print("=" * 70)
    print("INTENSIVE STRESS TEST — nvidia_nat_redis_orchestration")
    print(f"Redis: {REDIS_URL} | LLM: {OLLAMA_URL} ({OLLAMA_MODEL})")
    print("=" * 70)

    client = aioredis.from_url(REDIS_URL, decode_responses=True, socket_timeout=10.0)
    await client.ping()
    print("Redis connected.")

    # Clean up from previous runs
    for pfx in [PREFIX, f"{PREFIX}cr", f"{PREFIX}llm"]:
        cursor = 0
        while True:
            cursor, keys = await client.scan(cursor=cursor, match=f"{pfx}:*", count=200)
            if keys:
                await client.delete(*keys)
            if cursor == 0:
                break
    print("Cleanup done.\n")

    try:
        await stress_state_tracking(client)
        await stress_abort(client)
        await stress_sessions(client)
        await stress_crash_recovery(client)
        await stress_llm_middleware(client)
    except Exception as e:
        print(f"\nFATAL: {type(e).__name__}: {e}")
        traceback.print_exc()

    # Final durability check — verify session data still exists
    print("\n[FINAL] Durability check — re-reading all session data...")
    from nat.plugins.redis_orchestration.session_store import SessionStore
    store = SessionStore(client, PREFIX, session_ttl=600, max_turns=200, max_bytes=5_000_000)
    for i in range(10):
        sid = f"sess-{i:02d}"
        turns = await store.get_turns(sid)
        if len(turns) == 100:
            stats.ok("durability_check", 0)
        else:
            stats.fail("durability_check", f"{sid}: expected 100 turns, got {len(turns)}")

    # Cleanup
    for pfx in [PREFIX, f"{PREFIX}cr", f"{PREFIX}llm"]:
        cursor = 0
        while True:
            cursor, keys = await client.scan(cursor=cursor, match=f"{pfx}:*", count=200)
            if keys:
                await client.delete(*keys)
            if cursor == 0:
                break

    await client.close()

    print(stats.summary())
    return 0 if stats.failed == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
