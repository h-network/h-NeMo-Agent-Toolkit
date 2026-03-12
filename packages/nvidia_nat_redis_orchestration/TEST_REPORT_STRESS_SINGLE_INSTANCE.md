# Redis Orchestration Middleware — Stress Test Report (Single Instance)

**All three features stress-tested at scale on a single node.**

**Date:** 2026-03-12 19:15:00 UTC
**Infrastructure:**
- **Compute:** h-oracle (2x NVIDIA RTX 5090)
- **Redis:** `redis://localhost:6379` (Docker `redis:7-alpine`)
- **LLM:** Ollama `qwen3:32b` (local, 2x5090)
- **Python:** 3.12.3

## Summary

| Metric | Value |
|--------|-------|
| Total operations | 828 |
| Passed | 828 |
| Failed | 0 |
| Pass rate | **100.0%** |
| Total duration | 41.7s |

### Feature Coverage

| Feature | Operations | Result |
|---------|-----------|--------|
| Task Lifecycle State Tracking | 500 state cycles + 10 durability checks | **510/510 passed** |
| External Abort | 100 round-trips + 100 no-listener | **200/200 passed** |
| Session Continuity | 66 ops (fill, persist, limit, rotate, compact, large, unicode, concurrent) | **66/66 passed** |
| Crash Recovery | 100 orphans + 50 completed (skipped correctly) | **1/1 passed** |
| Real LLM Integration | 50 calls through middleware + state event verification | **51/51 passed** |

## 1. State Tracking Stress (500 cycles)

500 concurrent task state cycles: `running` → random terminal state (`completed`, `failed`, `timed_out`, `aborted`), with idempotency verification after each.

| # | Test | Ops | Status | Avg | Max | Detail |
|---|------|-----|--------|-----|-----|--------|
| 1 | state_cycle | 500 | PASS | 14.3ms | 69.3ms | Each: set_running → verify → set_terminal → verify → idempotency check |

- Ran in batches of 50 concurrent operations
- Random terminal state selection per cycle
- Every state transition verified via `get_state()`
- Idempotency: after terminal, `set_failed()` is a confirmed no-op

## 2. External Abort Stress (200 scenarios)

| # | Test | Ops | Status | Avg | Max | Detail |
|---|------|-----|--------|-----|-----|--------|
| 1 | abort_roundtrip | 100 | PASS | 54.7ms | 56.0ms | Subscribe → send abort → verify event set |
| 2 | abort_no_listener | 100 | PASS | 26.2ms | 38.8ms | Publish with no subscriber → verify 0 receivers |

- Round-trips: listener subscribes, abort published, `asyncio.Event` verified set
- No-listener: confirms `PUBLISH` returns 0 when nobody is listening
- Ran in batches of 20 concurrent abort round-trips

## 3. Session Continuity Stress (1000+ turns)

| # | Test | Ops | Status | Avg | Max | Detail |
|---|------|-----|--------|-----|-----|--------|
| 1 | session_fill | 10 | PASS | 186.7ms | 188.1ms | 10 sessions × 100 turns each, order verified |
| 2 | session_persist | 10 | PASS | 0.4ms | 0.8ms | Re-read all 10 sessions, all 100 turns intact |
| 3 | session_limit | 10 | PASS | 0.1ms | 0.1ms | `get_turns(limit=10)` returns correct tail |
| 4 | session_rotation | 5 | PASS | 19.9ms | 20.0ms | 100 turns written with max_turns=20, verified ≤20 retained |
| 5 | session_compaction | 5 | PASS | 11.1ms | 11.2ms | 50 turns → compacted to 10 (threshold=30, keep_recent=10) |
| 6 | session_compact_summary | 5 | PASS | 11.3ms | 11.6ms | Compaction with summary: 11 turns (10 kept + 1 system summary) |
| 7 | session_large | 10 | PASS | 0.2ms | 0.3ms | 10KB messages stored and retrieved intact |
| 8 | session_unicode | 10 | PASS | 0.2ms | 0.4ms | Emoji (🔥💥☃), CJK (你好), Japanese (こんにちは), Arabic (مرحبا) |
| 9 | session_concurrent | 1 | PASS | 8.3ms | 8.3ms | 50 parallel writes to same session, all 50 persisted |

**Total turns written:** 1000 (fill) + 500 (rotation) + 500 (compaction+summary) + 10 (large) + 10 (unicode) + 50 (concurrent) = **2070 turns**

## 4. Crash Recovery Stress (150 tasks)

| # | Test | Ops | Status | Avg | Max | Detail |
|---|------|-----|--------|-----|-----|--------|
| 1 | crash_recovery | 1 | PASS | 66.5ms | 66.5ms | 100 orphaned `running` tasks recovered to `failed`; 50 `completed` tasks untouched |

- All 100 orphans verified as `FAILED` with error message containing "Orphaned"
- All 50 completed tasks confirmed still `COMPLETED` after recovery scan

## 5. Real LLM Integration (50 calls)

50 real LLM calls to `qwen3:32b` (Ollama, local 2x5090) through the orchestration middleware with state tracking enabled.

| # | Test | Ops | Status | Avg | Max | Detail |
|---|------|-----|--------|-----|-----|--------|
| 1 | llm_call | 50 | PASS | 4423ms | 7972ms | 10 unique prompts × 5 repetitions |
| 2 | llm_state_events | 1 | PASS | — | — | 50 `running` + 50 `completed` events on Pub/Sub |

- Ran in batches of 10 concurrent LLM calls
- Every call produced a non-empty response
- State events verified via Pub/Sub subscriber: 50 running, 50 completed
- Zero LLM failures across all 50 calls

## 6. Durability Check

After all stress tests completed, re-read all 10 original sessions (100 turns each):

| # | Test | Ops | Status | Detail |
|---|------|-----|--------|--------|
| 1 | durability_check | 10 | PASS | All 10 sessions × 100 turns still intact after full stress test |

**Nothing forgotten. Nothing lost.**

## Performance Summary

| Operation | Ops/sec | Latency (avg) |
|-----------|---------|---------------|
| State cycle (SET+GET+PUBLISH) | ~35/s | 14.3ms |
| Abort round-trip (SUB+PUB+EVENT) | ~18/s | 54.7ms |
| Session turn append (RPUSH+EXPIRE) | ~536/s | 1.9ms |
| Session read (LRANGE) | ~2500/s | 0.4ms |
| LLM call (qwen3:32b, full middleware) | ~1.2/s | 4.4s |
| Crash recovery (100 orphans) | — | 66.5ms |

## Environment Notes

- Single Redis instance, single application instance
- All operations ran on the same h-oracle node
- Redis and Ollama both running as Docker containers
- No network latency (localhost connections)
- Concurrent batching: 50 (state), 20 (abort), 10 (LLM)

---
*Generated by test_stress.py — Single Instance Configuration*
