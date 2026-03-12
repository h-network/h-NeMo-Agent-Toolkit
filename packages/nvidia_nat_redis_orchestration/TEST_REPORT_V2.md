# Redis Orchestration Middleware — Test Report v2

**All three features tested: State Tracking, External Abort, Session Continuity**

**Date:** 2026-03-12 18:44:07 UTC
**Infrastructure:** Redis @ `redis://h-oracle:6379` | Ollama @ `http://h-titan:11434/v1` (`gpt-oss:20b`)
**Python:** 3.11.2

## Summary

| Metric | Value |
|--------|-------|
| Total tests | 55 |
| Passed | 55 |
| Failed | 0 |
| Pass rate | 100.0% |
| Total duration | 4257ms |

### Feature Coverage

| Feature | Status |
|---------|--------|
| Task Lifecycle State Tracking | 14/14 passed |
| External Abort | 12/12 passed |
| Session Continuity | 13/13 passed |

## F1: State Tracking (unit)

**7/7** passed

| # | Test | Status | Time | Detail |
|---|------|--------|------|--------|
| 1 | state_enum | PASS | 0ms | 5 states, 4 terminal |
| 2 | state_info_immutable | PASS | 0ms | TaskStateInfo is frozen |
| 3 | key_format | PASS | 2ms | p:state:{id}, p:state_events |
| 4 | set_running | PASS | 1ms | SET with TTL=600, PUBLISH to channel |
| 5 | all_transitions | PASS | 7ms | running -> completed, failed, timed_out, aborted |
| 6 | terminal_idempotent | PASS | 1ms | completed blocks failed (no write) |
| 7 | missing_key_noop | PASS | 1ms | Missing key -> no-op |

## F2: External Abort (unit)

**5/5** passed

| # | Test | Status | Time | Detail |
|---|------|--------|------|--------|
| 1 | exceptions | PASS | 0ms | TaskAbortedError carries task_id + reason |
| 2 | channel_format | PASS | 0ms | p:abort:{id} |
| 3 | send_abort | PASS | 0ms | Published with reason + timestamp |
| 4 | listen_sets_event | PASS | 2ms | Event set on message |
| 5 | listen_cancel_cleanup | PASS | 13ms | Cancelled cleanly, unsubscribe + close called |

## F1+F2: Middleware (unit)

**7/7** passed

| # | Test | Status | Time | Detail |
|---|------|--------|------|--------|
| 1 | invoke_success | PASS | 4ms | States: ['running', 'completed'] |
| 2 | invoke_failure | PASS | 3ms | States: ['running', 'failed'] |
| 3 | invoke_timeout | PASS | 3ms | States: ['running', 'timed_out'] |
| 4 | invoke_abort | PASS | 6ms | TaskAbortedError raised, state=aborted |
| 5 | stream_success | PASS | 3ms | Chunks: ['c0', 'c1', 'c2'] |
| 6 | stream_abort | PASS | 205ms | Abort during stream |
| 7 | both_disabled | PASS | 4ms | Pass-through when all disabled |

## F3: Session Continuity (unit)

**13/13** passed

| # | Test | Status | Time | Detail |
|---|------|--------|------|--------|
| 1 | turn_roles | PASS | 0ms | 4 roles |
| 2 | turn_frozen | PASS | 0ms | ConversationTurn is immutable |
| 3 | session_id_derivation | PASS | 0ms | conv_id preferred over user_id; None if neither |
| 4 | append_turn | PASS | 3ms | RPUSH + EXPIRE with TTL=3600 |
| 5 | get_turns | PASS | 2ms | 2 turns deserialized |
| 6 | get_turns_with_limit | PASS | 2ms | LRANGE with negative offset for tail |
| 7 | rotation_by_count | PASS | 2ms | 8 turns -> LTRIM(3, -1) to keep 5 |
| 8 | no_rotation | PASS | 2ms | Under limits -> no trim |
| 9 | clear_and_exists | PASS | 2ms | clear=DELETE, exists=EXISTS |
| 10 | compaction_under_threshold | PASS | 2ms | 5 turns < threshold 50 -> no compaction |
| 11 | compaction_drops_old | PASS | 4ms | 20 -> 5 turns (removed 15) |
| 12 | compaction_with_summary | PASS | 3ms | Summary generated, 6 retained (5 kept + 1 summary) |
| 13 | compaction_summary_failure | PASS | 3ms | LLM failure -> compaction still proceeds without summary |

## Config Validation (all features)

**5/5** passed

| # | Test | Status | Time | Detail |
|---|------|--------|------|--------|
| 1 | defaults | PASS | 0ms | All defaults correct (state=on, abort=on, session=off) |
| 2 | session_config | PASS | 0ms | Custom session config accepted |
| 3 | session_ttl_validation | PASS | 0ms | session_ttl=0 rejected |
| 4 | session_max_turns_validation | PASS | 0ms | session_max_turns=-1 rejected |
| 5 | type_discriminator | PASS | 0ms | type = 'redis_orchestration' |

## Crash Recovery (unit)

**3/3** passed

| # | Test | Status | Time | Detail |
|---|------|--------|------|--------|
| 1 | recovers_orphans | PASS | 2ms | t1 recovered, t2 (completed) skipped |
| 2 | empty_keyspace | PASS | 1ms | Empty -> [] |
| 3 | all_terminal_skipped | PASS | 1ms | All 4 terminal states skipped |

## Live Redis (all features)

**12/12** passed

| # | Test | Status | Time | Detail |
|---|------|--------|------|--------|
| 1 | state_lifecycle | PASS | 2ms | running->completed, terminal idempotent |
| 2 | state_ttl | PASS | 0ms | TTL=5s |
| 3 | pubsub_events | PASS | 2ms | Event: running for ev1 |
| 4 | abort_round_trip | PASS | 203ms | receivers=1, event=True |
| 5 | crash_recovery | PASS | 5ms | Recovered ['cr2', 'cr1'] |
| 6 | session_crud | PASS | 3ms | 4 turns stored/retrieved |
| 7 | session_limit | PASS | 0ms | Last 2: [What is 2+2?, Four.] |
| 8 | session_ttl | PASS | 1ms | TTL=10s on session key |
| 9 | session_rotation | PASS | 4ms | 3 turns after adding 6 (max=3) |
| 10 | session_compaction | PASS | 18ms | 20 -> 5 turns |
| 11 | session_compaction_with_summary | PASS | 18ms | 6 turns, first is summary |
| 12 | session_unicode | PASS | 1ms | Snowman: ☃ Fire: 🔥 |

## E2E with Real LLM

**3/3** passed

| # | Test | Status | Time | Detail |
|---|------|--------|------|--------|
| 1 | successful_llm | PASS | 1099ms | LLM: 'Four', states: ['running', 'completed'] |
| 2 | concurrent_llm | PASS | 1603ms | LLM1: 'Blue', LLM2: 'green', 2r/2c |
| 3 | abort_llm | PASS | 1009ms | abort receivers=1, aborted=True |

---
*Generated by test_full_report_v2.py*