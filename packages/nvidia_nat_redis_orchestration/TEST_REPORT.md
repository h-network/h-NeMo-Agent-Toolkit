# Redis Orchestration Middleware — Test Report

**Date:** 2026-03-12 18:17:39 UTC
**Infrastructure:** Redis @ `redis://h-oracle:6379` | Ollama @ `http://h-titan:11434/v1` (`gpt-oss:20b`)
**Python:** 3.11.2

## Summary

| Metric | Value |
|--------|-------|
| Total tests | 62 |
| Passed | 62 |
| Failed | 0 |
| Pass rate | 100.0% |
| Total duration | 5610ms |

## Models & Exceptions

**7/7** passed

| # | Test | Status | Time | Detail |
|---|------|--------|------|--------|
| 1 | task_state_values | PASS | 0ms | All 5 states have correct string values |
| 2 | terminal_states | PASS | 0ms | RUNNING is non-terminal; 4 terminal states correct |
| 3 | task_state_info_frozen | PASS | 0ms | TaskStateInfo is immutable (frozen dataclass) |
| 4 | task_state_info_fields | PASS | 0ms | All 7 fields present and correct |
| 5 | task_state_info_default_error | PASS | 0ms | error defaults to None |
| 6 | task_aborted_error | PASS | 0ms | TaskAbortedError inherits OrchestrationError, carries task_id + reason |
| 7 | orchestration_error | PASS | 0ms | OrchestrationError is a plain Exception subclass |

## Config Validation

**6/6** passed

| # | Test | Status | Time | Detail |
|---|------|--------|------|--------|
| 1 | minimal_config | PASS | 0ms | Defaults: ttl=3600, prefix=nat:orch, tracking=True |
| 2 | full_config | PASS | 0ms | All fields accepted with custom values |
| 3 | state_ttl_zero_rejected | PASS | 0ms | state_ttl=0 raises ValidationError |
| 4 | state_ttl_negative_rejected | PASS | 0ms | state_ttl=-5 raises ValidationError |
| 5 | type_discriminator | PASS | 0ms | type discriminator = 'redis_orchestration' |
| 6 | redis_password_optional | PASS | 0ms | redis_password accepted as optional field |

## State Tracker (unit)

**11/11** passed

| # | Test | Status | Time | Detail |
|---|------|--------|------|--------|
| 1 | key_format | PASS | 4ms | pfx:state:{id} and pfx:state_events |
| 2 | set_running_payload | PASS | 3ms | JSON payload correct, TTL=600 |
| 3 | set_running_publishes | PASS | 14ms | Published to pfx:state_events |
| 4 | transition_completed | PASS | 2ms | running -> completed |
| 5 | transition_failed_with_error | PASS | 2ms | running -> failed with error message |
| 6 | transition_timed_out | PASS | 2ms | running -> timed_out |
| 7 | transition_aborted | PASS | 2ms | running -> aborted |
| 8 | terminal_state_idempotent | PASS | 2ms | completed state blocks transition to failed (no write) |
| 9 | missing_key_noop | PASS | 2ms | Missing key -> no-op (no write, no publish) |
| 10 | get_state_none | PASS | 2ms | Returns None for missing key |
| 11 | get_state_deserialize | PASS | 2ms | Deserialized TaskStateInfo with correct types |

## Abort Controller (unit)

**5/5** passed

| # | Test | Status | Time | Detail |
|---|------|--------|------|--------|
| 1 | channel_format | PASS | 0ms | pfx:abort:{task_id} |
| 2 | send_abort_publishes | PASS | 1ms | Published to pfx:abort:t1 with reason + timestamp |
| 3 | send_abort_returns_zero | PASS | 1ms | Returns 0 when no subscribers |
| 4 | listen_sets_event | PASS | 3ms | Event set on first message |
| 5 | listen_graceful_cancel | PASS | 14ms | Cancellation cleans up subscription |

## Crash Recovery (unit)

**5/5** passed

| # | Test | Status | Time | Detail |
|---|------|--------|------|--------|
| 1 | recovers_running | PASS | 4ms | Recovered t1 (running), skipped t2 (completed) |
| 2 | empty_keyspace | PASS | 1ms | No keys -> empty list |
| 3 | expired_key | PASS | 2ms | Key expired between SCAN and GET -> skipped |
| 4 | multi_page | PASS | 2ms | 2-page SCAN recovered both tasks |
| 5 | all_terminal_states_skipped | PASS | 2ms | All 4 terminal states skipped (completed, failed, timed_out, aborted) |

## Middleware (unit)

**9/9** passed

| # | Test | Status | Time | Detail |
|---|------|--------|------|--------|
| 1 | invoke_success | PASS | 4ms | States written: ['running', 'completed'] |
| 2 | invoke_failure | PASS | 4ms | States: ['running', 'failed'], error='bad' |
| 3 | invoke_timeout | PASS | 4ms | States: ['running', 'timed_out'] |
| 4 | invoke_tracking_disabled | PASS | 3ms | No Redis writes when tracking disabled |
| 5 | invoke_abort | PASS | 6ms | TaskAbortedError raised, state=aborted written |
| 6 | stream_success | PASS | 3ms | Chunks: ['c0', 'c1', 'c2'], States: ['running', 'completed'] |
| 7 | stream_failure | PASS | 3ms | States: ['running', 'failed'] |
| 8 | stream_abort | PASS | 205ms | TaskAbortedError raised during stream |
| 9 | both_disabled | PASS | 5ms | Pass-through when both features disabled |

## Live Redis Integration

**10/10** passed

| # | Test | Status | Time | Detail |
|---|------|--------|------|--------|
| 1 | connectivity | PASS | 0ms | Connected to redis://h-oracle:6379 |
| 2 | full_lifecycle | PASS | 2ms | running -> completed (terminal blocks failed) |
| 3 | failed_with_error | PASS | 1ms | error='db connection lost' |
| 4 | ttl_applied | PASS | 0ms | TTL=5s on state key |
| 5 | abort_round_trip | PASS | 205ms | receivers=1, event=True |
| 6 | abort_no_subscriber | PASS | 2ms | receivers=0 (expected 0) |
| 7 | crash_recovery | PASS | 6ms | Recovered ['cr2', 'cr1'], cr3 untouched |
| 8 | pubsub_events | PASS | 3ms | Event: {'task_id': 'ps1', 'state': 'running', 'function_name': 'fn', 'timestamp': 1773339454.892584} |
| 9 | unicode_error | PASS | 2ms | Unicode error stored: Error: ☃ snowman 🔥 fire |
| 10 | long_function_name | PASS | 1ms | Stored function name of length 500 |

## E2E with Real LLM

**4/4** passed

| # | Test | Status | Time | Detail |
|---|------|--------|------|--------|
| 1 | successful_llm | PASS | 1032ms | LLM said: 'Four.', states: ['running', 'completed'] |
| 2 | llm_with_long_response | PASS | 1403ms | LLM said: 'The first five prime numbers are:    2, 3, 5, 7, 11....', states: ['running', 'completed'] |
| 3 | abort_during_llm | PASS | 1008ms | abort sent (receivers=1), aborted=True, events: ['aborted'] |
| 4 | concurrent_llm_calls | PASS | 1600ms | LLM1: 'Blue', LLM2: 'Green', 2x running, 2x completed |

## Concurrency & Edge Cases

**5/5** passed

| # | Test | Status | Time | Detail |
|---|------|--------|------|--------|
| 1 | rapid_state_transitions | PASS | 28ms | 20 rapid running->completed cycles |
| 2 | concurrent_state_writes | PASS | 11ms | 10 parallel set_running calls succeeded |
| 3 | state_after_ttl_hint | PASS | 1ms | Key exists with TTL=2s |
| 4 | empty_error_string | PASS | 1ms | Empty string error stored correctly |
| 5 | special_chars_in_task_id | PASS | 1ms | task_id with special chars: 'task:with:colons-and_underscores.and.dots' |

---
*Generated by test_full_report.py*