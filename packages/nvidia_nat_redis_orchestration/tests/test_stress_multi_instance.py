#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""
Multi-instance stress test for nvidia_nat_redis_orchestration.

Runs two simulated instances (h-oracle + h-titan) against a shared Redis,
proving the orchestration middleware works in distributed deployments.

Scenarios:
  1. Concurrent state tracking — both instances write 250 state cycles each
  2. Cross-instance abort — h-titan aborts a task running on h-oracle
  3. Session handoff — start conversation on instance A, continue on instance B
  4. Cross-instance crash recovery — instance B recovers orphans from instance A
  5. Concurrent LLM calls — both instances call different Ollama endpoints simultaneously
  6. Pub/Sub event fan-out — both instances observe each other's state transitions
  7. Session interleaving — both instances write to the same session concurrently
  8. Durability — verify all data survives after distributed load

Designed to run from any host with SSH access to h-oracle and h-titan.
Actually runs locally using two separate Redis clients with different instance_ids
to simulate multi-instance, since both instances share the same Redis.
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

REDIS_URL = os.environ.get("REDIS_URL", "redis://h-oracle:6379")
OLLAMA_ORACLE = os.environ.get("OLLAMA_ORACLE", "http://h-oracle:11434/v1")
OLLAMA_TITAN = os.environ.get("OLLAMA_TITAN", "http://h-titan:11434/v1")
MODEL_ORACLE = os.environ.get("MODEL_ORACLE", "qwen3:32b")
MODEL_TITAN = os.environ.get("MODEL_TITAN", "mistral:7b")
PREFIX = "multi"

INSTANCE_A = "h-oracle-01"
INSTANCE_B = "h-titan-01"


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
        if len(self.errors) <= 30:
            print(f"  FAIL [{category}]: {error[:200]}")

    @property
    def elapsed(self):
        return time.time() - self.start_time

    def summary(self) -> str:
        lines = [
            "=" * 70,
            "MULTI-INSTANCE STRESS TEST RESULTS",
            "=" * 70,
            f"Duration: {self.elapsed:.1f}s",
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
            lines.append(f"\nFirst {min(30, len(self.errors))} errors:")
            for e in self.errors[:30]:
                lines.append(f"  {e[:200]}")
        lines.append("=" * 70)
        return "\n".join(lines)


stats = Stats()


async def call_llm(url: str, model: str, prompt: str, max_tokens: int = 80) -> str:
    import httpx
    async with httpx.AsyncClient(timeout=120.0) as c:
        r = await c.post(f"{url}/chat/completions", json={
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
        })
        return r.json()["choices"][0]["message"].get("content", "") or "[no content]"


async def cleanup(client):
    for pfx in [PREFIX, f"{PREFIX}cr"]:
        cur = 0
        while True:
            cur, keys = await client.scan(cursor=cur, match=f"{pfx}:*", count=200)
            if keys:
                await client.delete(*keys)
            if cur == 0:
                break


# ===========================================================================
# 1. CONCURRENT STATE TRACKING — 250 cycles per instance (500 total)
# ===========================================================================
async def test_concurrent_state_tracking(client):
    from nat.plugins.redis_orchestration.state_tracker import TaskStateTracker
    from nat.plugins.redis_orchestration.models import TaskState

    print("\n[1/8] Concurrent State Tracking: 250 cycles × 2 instances (500 total)...")

    tracker_a = TaskStateTracker(client, PREFIX, state_ttl=300, instance_id=INSTANCE_A)
    tracker_b = TaskStateTracker(client, PREFIX, state_ttl=300, instance_id=INSTANCE_B)

    terminals = ["completed", "failed", "timed_out", "aborted"]

    async def cycle(tracker: TaskStateTracker, inst_name: str, i: int):
        tid = f"{inst_name}-{i:04d}"
        target = random.choice(terminals)
        t0 = time.time()
        try:
            await tracker.set_running(tid, f"func_{i}")
            info = await tracker.get_state(tid)
            if info is None or info.state != TaskState.RUNNING:
                stats.fail("multi_state", f"{tid}: not RUNNING")
                return
            # Verify instance_id is correct
            if info.instance_id != tracker._instance_id:
                stats.fail("multi_state", f"{tid}: instance_id {info.instance_id} != {tracker._instance_id}")
                return

            if target == "failed":
                await tracker.set_failed(tid, f"error from {inst_name}")
            elif target == "completed":
                await tracker.set_completed(tid)
            elif target == "timed_out":
                await tracker.set_timed_out(tid)
            else:
                await tracker.set_aborted(tid)

            info = await tracker.get_state(tid)
            if info.state.value != target:
                stats.fail("multi_state", f"{tid}: expected {target}, got {info.state}")
                return

            stats.ok("multi_state", time.time() - t0)
        except Exception as e:
            stats.fail("multi_state", f"{tid}: {type(e).__name__}: {e}")

    # Both instances run concurrently in interleaved batches
    for batch in range(0, 250, 25):
        tasks_a = [cycle(tracker_a, "oracle", i) for i in range(batch, min(batch + 25, 250))]
        tasks_b = [cycle(tracker_b, "titan", i) for i in range(batch, min(batch + 25, 250))]
        await asyncio.gather(*tasks_a, *tasks_b)

    print(f"  Done: {stats.passed}/{stats.total}")


# ===========================================================================
# 2. CROSS-INSTANCE ABORT — h-titan aborts a task running on h-oracle
# ===========================================================================
async def test_cross_instance_abort(client):
    from nat.plugins.redis_orchestration.state_tracker import TaskStateTracker
    from nat.plugins.redis_orchestration.abort_controller import AbortController
    from nat.plugins.redis_orchestration.models import TaskState

    print("\n[2/8] Cross-Instance Abort: instance B aborts tasks on instance A (50 times)...")

    tracker_a = TaskStateTracker(client, PREFIX, state_ttl=300, instance_id=INSTANCE_A)
    ctrl_b = AbortController(client, PREFIX)  # Instance B sends abort

    async def cross_abort(i: int):
        tid = f"xabort-{i:04d}"
        event = asyncio.Event()
        t0 = time.time()
        try:
            # Instance A starts a task and listens for abort
            await tracker_a.set_running(tid, f"long_task_{i}")
            ctrl_a = AbortController(client, PREFIX)
            listener = asyncio.create_task(ctrl_a.listen_for_abort(tid, event))
            await asyncio.sleep(0.05)

            # Instance B sends abort
            n = await ctrl_b.send_abort(tid, f"killed by {INSTANCE_B}")
            if n < 1:
                stats.fail("cross_abort", f"{tid}: no receivers")
                listener.cancel()
                try:
                    await listener
                except asyncio.CancelledError:
                    pass
                return

            await asyncio.wait_for(event.wait(), timeout=5.0)

            # Instance A transitions state to aborted
            await tracker_a.set_aborted(tid)
            info = await tracker_a.get_state(tid)
            if info.state != TaskState.ABORTED:
                stats.fail("cross_abort", f"{tid}: expected ABORTED, got {info.state}")
            else:
                stats.ok("cross_abort", time.time() - t0)
        except asyncio.TimeoutError:
            stats.fail("cross_abort", f"{tid}: timeout waiting for abort event")
        except Exception as e:
            stats.fail("cross_abort", f"{tid}: {type(e).__name__}: {e}")
        finally:
            if not listener.done():
                listener.cancel()
                try:
                    await listener
                except asyncio.CancelledError:
                    pass

    for batch in range(0, 50, 10):
        await asyncio.gather(*[cross_abort(i) for i in range(batch, min(batch + 10, 50))])

    print(f"  Done: {stats.passed}/{stats.total}")


# ===========================================================================
# 3. SESSION HANDOFF — start on A, continue on B, verify continuity
# ===========================================================================
async def test_session_handoff(client):
    from nat.plugins.redis_orchestration.session_store import SessionStore
    from nat.plugins.redis_orchestration.session_models import TurnRole

    print("\n[3/8] Session Handoff: 20 sessions started on A, continued on B...")

    store_a = SessionStore(client, PREFIX, session_ttl=600, max_turns=200, max_bytes=5_000_000)
    store_b = SessionStore(client, PREFIX, session_ttl=600, max_turns=200, max_bytes=5_000_000)

    async def handoff(i: int):
        sid = f"handoff-{i:02d}"
        t0 = time.time()
        try:
            # Instance A writes first 5 turns
            for j in range(5):
                await store_a.add_user_turn(sid, f"[A] user msg {j}", source=INSTANCE_A)
                await store_a.add_assistant_turn(sid, f"[A] assistant msg {j}", source=INSTANCE_A)

            # Instance B picks up — should see all 10 turns from A
            turns_before = await store_b.get_turns(sid)
            if len(turns_before) != 10:
                stats.fail("session_handoff", f"{sid}: B sees {len(turns_before)} turns, expected 10")
                return

            # Instance B continues the conversation
            for j in range(5):
                await store_b.add_user_turn(sid, f"[B] user msg {j}", source=INSTANCE_B)
                await store_b.add_assistant_turn(sid, f"[B] assistant msg {j}", source=INSTANCE_B)

            # Both instances should see all 20 turns
            turns_a = await store_a.get_turns(sid)
            turns_b = await store_b.get_turns(sid)

            if len(turns_a) != 20 or len(turns_b) != 20:
                stats.fail("session_handoff", f"{sid}: A={len(turns_a)}, B={len(turns_b)}, expected 20")
                return

            # Verify order: first 10 from A, last 10 from B
            if not turns_a[0].content.startswith("[A]"):
                stats.fail("session_handoff", f"{sid}: first turn not from A")
                return
            if not turns_a[10].content.startswith("[B]"):
                stats.fail("session_handoff", f"{sid}: turn 10 not from B")
                return

            # Verify both see identical data
            for k in range(20):
                if turns_a[k].content != turns_b[k].content:
                    stats.fail("session_handoff", f"{sid}: divergence at turn {k}")
                    return

            stats.ok("session_handoff", time.time() - t0)
        except Exception as e:
            stats.fail("session_handoff", f"{sid}: {type(e).__name__}: {e}")

    await asyncio.gather(*[handoff(i) for i in range(20)])
    print(f"  Done: {stats.passed}/{stats.total}")


# ===========================================================================
# 4. CROSS-INSTANCE CRASH RECOVERY — B recovers A's orphans
# ===========================================================================
async def test_cross_instance_crash_recovery(client):
    from nat.plugins.redis_orchestration.state_tracker import TaskStateTracker
    from nat.plugins.redis_orchestration.crash_recovery import CrashRecovery
    from nat.plugins.redis_orchestration.models import TaskState

    print("\n[4/8] Cross-Instance Crash Recovery: B recovers 50 orphans from A...")

    tracker_a = TaskStateTracker(client, f"{PREFIX}cr", state_ttl=300, instance_id=INSTANCE_A)
    t0 = time.time()
    try:
        # Instance A creates 50 running tasks then "crashes" (we just leave them)
        for i in range(50):
            await tracker_a.set_running(f"crash-{i:04d}", f"func_{i}")

        # Also 25 completed tasks from A (should not be recovered)
        for i in range(25):
            await tracker_a.set_running(f"done-{i:04d}", f"func_done_{i}")
            await tracker_a.set_completed(f"done-{i:04d}")

        # Instance B starts up and runs crash recovery
        recovery_b = CrashRecovery(client, f"{PREFIX}cr", state_ttl=300, instance_id=INSTANCE_B)
        recovered = await recovery_b.recover_orphaned_tasks()

        if len(recovered) != 50:
            stats.fail("cross_crash", f"expected 50 recovered, got {len(recovered)}")
        else:
            # Verify all orphans are FAILED with B's recovery message
            all_ok = True
            for i in range(50):
                info = await tracker_a.get_state(f"crash-{i:04d}")
                if info is None or info.state != TaskState.FAILED:
                    all_ok = False
                    break
                if INSTANCE_B not in info.error:
                    all_ok = False
                    break

            # Verify completed tasks untouched
            for i in range(25):
                info = await tracker_a.get_state(f"done-{i:04d}")
                if info is None or info.state != TaskState.COMPLETED:
                    all_ok = False
                    break

            if all_ok:
                stats.ok("cross_crash", time.time() - t0)
            else:
                stats.fail("cross_crash", "verification failed")
    except Exception as e:
        stats.fail("cross_crash", f"{type(e).__name__}: {e}")

    print(f"  Done: {stats.passed}/{stats.total}")


# ===========================================================================
# 5. CONCURRENT LLM CALLS — both instances use different Ollama endpoints
# ===========================================================================
async def test_concurrent_llm_calls(client):
    from nat.plugins.redis_orchestration.orchestration_middleware import RedisOrchestrationMiddleware
    from nat.plugins.redis_orchestration.orchestration_middleware_config import RedisOrchestrationConfig
    from nat.middleware.middleware import FunctionMiddlewareContext
    from unittest.mock import Mock, AsyncMock

    print(f"\n[5/8] Concurrent LLM: 15 calls on {MODEL_ORACLE} (h-oracle) + 15 on {MODEL_TITAN} (h-titan)...")

    def make_mw(instance_id):
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
        return RedisOrchestrationMiddleware(cfg, builder, client, instance_id)

    mw_a = make_mw(INSTANCE_A)
    mw_b = make_mw(INSTANCE_B)

    # Subscribe to state events
    import redis.asyncio as aioredis
    sub = aioredis.from_url(REDIS_URL, decode_responses=True, socket_timeout=10.0)
    ps = sub.pubsub()
    await ps.subscribe(f"{PREFIX}llm:state_events")
    await ps.get_message(timeout=2.0)

    prompts = ["What is 2+2?", "Name a color.", "Say hello.", "Name a fruit.", "What comes after 5?"]

    async def llm_call(mw, inst, url, model, i, prompt):
        ctx = FunctionMiddlewareContext(
            name=f"llm_{inst}_{i}", config=Mock(), description=f"LLM {inst}",
            input_schema=None, single_output_schema=type(None), stream_output_schema=type(None))

        async def fn(*a, **kw):
            return await call_llm(url, model, prompt)

        t0 = time.time()
        try:
            result = await mw.function_middleware_invoke("x", call_next=fn, context=ctx)
            if result and len(result.strip()) > 0:
                stats.ok(f"llm_{inst}", time.time() - t0)
            else:
                stats.fail(f"llm_{inst}", f"empty response")
        except Exception as e:
            stats.fail(f"llm_{inst}", f"{type(e).__name__}: {e}")

    # Run both instances' LLM calls concurrently in batches
    for batch in range(0, 15, 5):
        tasks = []
        for i in range(batch, min(batch + 5, 15)):
            tasks.append(llm_call(mw_a, "oracle", OLLAMA_ORACLE, MODEL_ORACLE, i, prompts[i % 5]))
            tasks.append(llm_call(mw_b, "titan", OLLAMA_TITAN, MODEL_TITAN, i, prompts[i % 5]))
        await asyncio.gather(*tasks)
        print(f"    Batch {batch//5 + 1}/3 done")

    # Verify state events from both instances
    await asyncio.sleep(0.5)
    events = []
    while True:
        msg = await ps.get_message(timeout=0.5)
        if msg is None:
            break
        if msg["type"] == "message":
            events.append(json.loads(msg["data"]))

    running_count = sum(1 for e in events if e["state"] == "running")
    completed_count = sum(1 for e in events if e["state"] == "completed")

    if running_count >= 30 and completed_count >= 30:
        stats.ok("llm_events_multi", 0)
    else:
        stats.fail("llm_events_multi", f"expected 30+ each, got {running_count}r/{completed_count}c")

    await ps.unsubscribe()
    await ps.close()
    await sub.close()

    print(f"  Done: {stats.passed}/{stats.total}")


# ===========================================================================
# 6. PUB/SUB EVENT FAN-OUT — both instances observe each other's events
# ===========================================================================
async def test_pubsub_fanout(client):
    from nat.plugins.redis_orchestration.state_tracker import TaskStateTracker

    print("\n[6/8] Pub/Sub Fan-Out: 2 subscribers, 20 events, verify both see all...")

    import redis.asyncio as aioredis

    tracker = TaskStateTracker(client, PREFIX, state_ttl=300, instance_id=INSTANCE_A)

    # Two subscribers (simulating two instances watching)
    sub1 = aioredis.from_url(REDIS_URL, decode_responses=True, socket_timeout=10.0)
    sub2 = aioredis.from_url(REDIS_URL, decode_responses=True, socket_timeout=10.0)
    ps1 = sub1.pubsub()
    ps2 = sub2.pubsub()
    await ps1.subscribe(tracker.state_channel)
    await ps2.subscribe(tracker.state_channel)
    await ps1.get_message(timeout=2.0)
    await ps2.get_message(timeout=2.0)

    # Publish 20 state transitions
    for i in range(20):
        await tracker.set_running(f"fanout-{i:02d}", f"fn_{i}")

    await asyncio.sleep(0.5)

    events1 = []
    events2 = []
    while True:
        msg = await ps1.get_message(timeout=0.5)
        if msg is None:
            break
        if msg["type"] == "message":
            events1.append(json.loads(msg["data"]))
    while True:
        msg = await ps2.get_message(timeout=0.5)
        if msg is None:
            break
        if msg["type"] == "message":
            events2.append(json.loads(msg["data"]))

    t0 = time.time()
    if len(events1) >= 20 and len(events2) >= 20:
        # Verify both saw the same events
        ids1 = {e["task_id"] for e in events1}
        ids2 = {e["task_id"] for e in events2}
        expected = {f"fanout-{i:02d}" for i in range(20)}
        if ids1 >= expected and ids2 >= expected:
            stats.ok("pubsub_fanout", time.time() - t0)
        else:
            stats.fail("pubsub_fanout", f"missing events: sub1={len(ids1 & expected)}/20, sub2={len(ids2 & expected)}/20")
    else:
        stats.fail("pubsub_fanout", f"sub1={len(events1)}, sub2={len(events2)}, expected 20 each")

    await ps1.unsubscribe()
    await ps2.unsubscribe()
    await ps1.close()
    await ps2.close()
    await sub1.close()
    await sub2.close()

    print(f"  Done: {stats.passed}/{stats.total}")


# ===========================================================================
# 7. SESSION INTERLEAVING — both instances write to same session
# ===========================================================================
async def test_session_interleaving(client):
    from nat.plugins.redis_orchestration.session_store import SessionStore
    from nat.plugins.redis_orchestration.session_models import TurnRole

    print("\n[7/8] Session Interleaving: 2 instances writing to 10 shared sessions...")

    store_a = SessionStore(client, PREFIX, session_ttl=600, max_turns=500, max_bytes=5_000_000)
    store_b = SessionStore(client, PREFIX, session_ttl=600, max_turns=500, max_bytes=5_000_000)

    async def interleave(i: int):
        sid = f"interleave-{i:02d}"
        t0 = time.time()
        try:
            # Both instances write alternating turns
            tasks = []
            for j in range(25):
                tasks.append(store_a.add_user_turn(sid, f"[A-{j:02d}]"))
                tasks.append(store_b.add_assistant_turn(sid, f"[B-{j:02d}]"))
            await asyncio.gather(*tasks)

            turns = await store_a.get_turns(sid)
            if len(turns) != 50:
                stats.fail("session_interleave", f"{sid}: expected 50 turns, got {len(turns)}")
                return

            # All turns should be present (order may vary due to concurrency)
            a_turns = [t for t in turns if t.content.startswith("[A-")]
            b_turns = [t for t in turns if t.content.startswith("[B-")]
            if len(a_turns) != 25 or len(b_turns) != 25:
                stats.fail("session_interleave", f"{sid}: A={len(a_turns)}, B={len(b_turns)}")
                return

            stats.ok("session_interleave", time.time() - t0)
        except Exception as e:
            stats.fail("session_interleave", f"{sid}: {type(e).__name__}: {e}")

    await asyncio.gather(*[interleave(i) for i in range(10)])
    print(f"  Done: {stats.passed}/{stats.total}")


# ===========================================================================
# 8. DURABILITY — verify everything persists after distributed load
# ===========================================================================
async def test_durability(client):
    from nat.plugins.redis_orchestration.session_store import SessionStore
    from nat.plugins.redis_orchestration.state_tracker import TaskStateTracker
    from nat.plugins.redis_orchestration.models import TaskState

    print("\n[8/8] Durability Check: re-reading data written by both instances...")

    store = SessionStore(client, PREFIX, session_ttl=600, max_turns=500, max_bytes=5_000_000)
    tracker = TaskStateTracker(client, PREFIX, state_ttl=300, instance_id="verifier")

    # Check handoff sessions (20 sessions × 20 turns each)
    for i in range(20):
        sid = f"handoff-{i:02d}"
        t0 = time.time()
        try:
            turns = await store.get_turns(sid)
            if len(turns) != 20:
                stats.fail("durability_handoff", f"{sid}: expected 20, got {len(turns)}")
            else:
                stats.ok("durability_handoff", time.time() - t0)
        except Exception as e:
            stats.fail("durability_handoff", f"{sid}: {type(e).__name__}: {e}")

    # Check interleaved sessions (10 sessions × 50 turns each)
    for i in range(10):
        sid = f"interleave-{i:02d}"
        t0 = time.time()
        try:
            turns = await store.get_turns(sid)
            if len(turns) != 50:
                stats.fail("durability_interleave", f"{sid}: expected 50, got {len(turns)}")
            else:
                stats.ok("durability_interleave", time.time() - t0)
        except Exception as e:
            stats.fail("durability_interleave", f"{sid}: {type(e).__name__}: {e}")

    # Spot-check some state entries from both instances
    checks = 0
    for inst_prefix in ["oracle", "titan"]:
        for i in random.sample(range(250), 10):
            tid = f"{inst_prefix}-{i:04d}"
            info = await tracker.get_state(tid)
            if info is not None and info.state in {TaskState.COMPLETED, TaskState.FAILED, TaskState.TIMED_OUT, TaskState.ABORTED}:
                checks += 1
    if checks >= 15:  # At least 15 of 20 spot checks should find terminal states
        stats.ok("durability_state_spot", 0)
    else:
        stats.fail("durability_state_spot", f"only {checks}/20 spot checks found valid state")

    print(f"  Done: {stats.passed}/{stats.total}")


# ===========================================================================
# MAIN
# ===========================================================================
async def main():
    import redis.asyncio as aioredis

    print("=" * 70)
    print("MULTI-INSTANCE STRESS TEST — nvidia_nat_redis_orchestration")
    print(f"Shared Redis: {REDIS_URL}")
    print(f"Instance A ({INSTANCE_A}): LLM @ {OLLAMA_ORACLE} ({MODEL_ORACLE})")
    print(f"Instance B ({INSTANCE_B}): LLM @ {OLLAMA_TITAN} ({MODEL_TITAN})")
    print("=" * 70)

    client = aioredis.from_url(REDIS_URL, decode_responses=True, socket_timeout=10.0)
    await client.ping()
    print("Redis connected.")

    await cleanup(client)
    print("Cleanup done.\n")

    try:
        await test_concurrent_state_tracking(client)
        await test_cross_instance_abort(client)
        await test_session_handoff(client)
        await test_cross_instance_crash_recovery(client)
        await test_concurrent_llm_calls(client)
        await test_pubsub_fanout(client)
        await test_session_interleaving(client)
        await test_durability(client)
    except Exception as e:
        print(f"\nFATAL: {type(e).__name__}: {e}")
        traceback.print_exc()

    await cleanup(client)
    await client.close()

    print(stats.summary())
    return 0 if stats.failed == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
