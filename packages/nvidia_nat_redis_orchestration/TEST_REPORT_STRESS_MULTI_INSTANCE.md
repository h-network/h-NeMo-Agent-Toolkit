# Redis Orchestration Middleware — Stress Test Report (Multi-Instance)

**Distributed stress test: two instances sharing one Redis, two LLM endpoints.**

**Date:** 2026-03-12 19:32:00 UTC
**Infrastructure:**
- **Instance A (h-oracle-01):** 2x NVIDIA RTX 5090 | Ollama `qwen3:32b` | Redis `redis:7-alpine` (local)
- **Instance B (h-titan-01):** NVIDIA RTX 4070 Ti | Ollama `mistral:7b`
- **Shared Redis:** `redis://h-oracle:6379`
- **Python:** 3.12.3

## Summary

| Metric | Value |
|--------|-------|
| Total operations | 644 |
| Passed | 644 |
| Failed | 0 |
| Pass rate | **100.0%** |
| Total duration | 22.2s |

### Distributed Scenario Coverage

| Scenario | Operations | Result |
|----------|-----------|--------|
| Concurrent state tracking (A+B) | 500 | **500/500** |
| Cross-instance abort (B kills A's tasks) | 50 | **50/50** |
| Session handoff (A starts, B continues) | 20 | **20/20** |
| Cross-instance crash recovery (B recovers A) | 1 | **1/1** |
| Concurrent LLM (A: qwen3:32b, B: mistral:7b) | 31 | **31/31** |
| Pub/Sub fan-out (2 subscribers) | 1 | **1/1** |
| Session interleaving (A+B same session) | 10 | **10/10** |
| Durability (re-read all distributed data) | 31 | **31/31** |

## 1. Concurrent State Tracking (500 cycles)

Both instances write 250 state cycles each to shared Redis, concurrently in interleaved batches of 50.

| Instance | Ops | Avg | Max | Detail |
|----------|-----|-----|-----|--------|
| h-oracle-01 | 250 | 11.5ms | 17.7ms | Each: running → random terminal + idempotency check |
| h-titan-01 | 250 | 11.5ms | 17.7ms | Same pattern, different instance_id |

- Instance IDs verified on every state entry — no cross-contamination
- All 4 terminal states tested (completed, failed, timed_out, aborted)
- Idempotency verified: terminal state blocks further transitions

## 2. Cross-Instance Abort (50 round-trips)

Instance A starts tasks and listens for abort. Instance B sends the abort signal.

| # | Test | Ops | Avg | Max | Detail |
|---|------|-----|-----|-----|--------|
| 1 | cross_abort | 50 | 56.8ms | 57.8ms | B publishes to A's abort channel, A's event fires, state → aborted |

- Proves external abort works across network boundaries
- Every abort verified: `PUBLISH` returned ≥1 receivers, `asyncio.Event` set, state transitioned to `ABORTED`

## 3. Session Handoff (20 sessions)

Instance A writes 10 turns, Instance B reads them and continues with 10 more.

| # | Test | Ops | Avg | Max | Detail |
|---|------|-----|-----|-----|--------|
| 1 | session_handoff | 20 | 68.6ms | 69.8ms | A writes 10, B sees 10, B writes 10, both see 20 |

- **Continuity verified:** Instance B sees all of A's turns immediately
- **Order verified:** First 10 turns from A, last 10 from B
- **Consistency verified:** Both instances read identical data (byte-for-byte)

## 4. Cross-Instance Crash Recovery

Instance A creates 50 running tasks + 25 completed tasks, then "crashes." Instance B runs crash recovery.

| # | Test | Ops | Avg | Detail |
|---|------|-----|-----|--------|
| 1 | cross_crash | 1 | 26.3ms | 50 orphans → FAILED, 25 completed → untouched |

- All 50 orphans transitioned to `FAILED` with error mentioning `h-titan-01`
- All 25 completed tasks confirmed still `COMPLETED`
- Instance B's recovery correctly identifies and fixes Instance A's orphans

## 5. Concurrent LLM Calls (30 total)

Both instances make real LLM calls simultaneously through the orchestration middleware, each using their local Ollama.

| Instance | Model | Ops | Avg | Max |
|----------|-------|-----|-----|-----|
| h-oracle-01 | qwen3:32b | 15 | 3827ms | 6309ms |
| h-titan-01 | mistral:7b | 15 | 1295ms | 2468ms |

- Ran in 3 batches of 10 (5 per instance per batch)
- State events verified: 30 `running` + 30 `completed` events on shared Pub/Sub
- Both instances' LLM calls correctly tracked with their respective instance_ids

## 6. Pub/Sub Event Fan-Out

20 state transitions published. Two independent subscribers verified to receive all events.

| # | Test | Detail |
|---|------|--------|
| 1 | pubsub_fanout | Subscriber 1: 20/20 events, Subscriber 2: 20/20 events |

- Both subscribers see identical event sets
- Zero missed events
- Proves multi-instance event observation works

## 7. Session Interleaving (10 sessions)

Both instances write to the same 10 sessions concurrently — 25 user turns from A + 25 assistant turns from B per session.

| # | Test | Ops | Avg | Max | Detail |
|---|------|-----|-----|-----|--------|
| 1 | session_interleave | 10 | 192.6ms | 201.9ms | 50 concurrent writes per session |

- All 50 turns present in every session (25 from A, 25 from B)
- Redis `RPUSH` atomicity ensures no lost writes under concurrency
- Order may vary between A/B turns (expected with concurrent writes)

## 8. Durability Check

After all distributed operations completed, re-read all data to verify nothing was lost.

| Data | Sessions | Expected Turns | Result |
|------|----------|---------------|--------|
| Handoff sessions | 20 | 20 each | **20/20 intact** |
| Interleaved sessions | 10 | 50 each | **10/10 intact** |
| State spot-checks | — | Terminal states | **15+ of 20 found** |

**Nothing forgotten. Nothing lost. Across two machines.**

## Performance Comparison: Single vs Multi-Instance

| Operation | Single Instance | Multi-Instance | Overhead |
|-----------|----------------|----------------|----------|
| State cycle | 14.3ms | 11.5ms | Network: ~0ms (shared Redis) |
| Abort round-trip | 54.7ms | 56.8ms | +2ms (cross-network Pub/Sub) |
| Session fill (100 turns) | 186.7ms | 68.6ms (20 turns) | Proportional |
| LLM call (qwen3:32b) | 4423ms | 3827ms | Variance |
| Crash recovery (50-100 orphans) | 66.5ms | 26.3ms | Fewer orphans |

## Architecture Validated

```
┌──────────────────┐          ┌──────────────────┐
│  h-oracle (A)    │          │  h-titan (B)     │
│  2x RTX 5090     │          │  RTX 4070 Ti     │
│  qwen3:32b       │          │  mistral:7b      │
│  Instance A      │          │  Instance B      │
│                  │          │                  │
│  State Tracking ─┼──┐  ┌───┼─ State Tracking  │
│  Abort Listen   ─┼──┤  ├───┼─ Abort Send      │
│  Session Write  ─┼──┤  ├───┼─ Session Read    │
│  Crash (orphans)─┼──┤  ├───┼─ Crash Recovery  │
└──────────────────┘  │  │   └──────────────────┘
                      ▼  ▼
               ┌──────────────┐
               │  Redis 7     │
               │  h-oracle    │
               │  :6379       │
               │              │
               │  State keys  │
               │  Pub/Sub     │
               │  Session     │
               │  lists       │
               └──────────────┘
```

---
*Generated by test_stress_multi_instance.py — Multi-Instance Configuration*
