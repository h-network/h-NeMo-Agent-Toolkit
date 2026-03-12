Warning: Permanently added '192.168.178.100' (ED25519) to the list of known hosts.
# Agent Wars — Chaos Test Report

**Date:** 2026-03-12 19:47:26 UTC
**Infrastructure:** Redis @ `redis://h-oracle:6379` | LLMs: `qwen3:32b` (h-oracle) + `mistral:7b` (h-titan)

```
======================================================================
AGENT WARS — FINAL REPORT
======================================================================

Total rounds: 150
LLM calls: 150 (errors: 0)
Total aborts: 67
Total passes: 83
Revenge kills: 57
Revenge rate: 85.1% of all kills

KILL LEADERBOARD (most aggressive):
  1. Shark      — Kills:  12  Deaths:  11  K/D: 1.09
  2. Guardian   — Kills:  11  Deaths:  12  K/D: 0.92
  3. Trickster  — Kills:   8  Deaths:   6  K/D: 1.33
  4. Reaper     — Kills:   8  Deaths:  12  K/D: 0.67
  5. Berserker  — Kills:   7  Deaths:   2  K/D: 3.50
  6. Kingpin    — Kills:   6  Deaths:   2  K/D: 3.00
  7. Vendetta   — Kills:   5  Deaths:   2  K/D: 2.50
  8. Warlord    — Kills:   4  Deaths:  16  K/D: 0.25
  9. Phantom    — Kills:   4  Deaths:   2  K/D: 2.00
  10. Chaos      — Kills:   2  Deaths:   2  K/D: 1.00

DEATH LEADERBOARD (most targeted):
  1. Warlord    — Deaths:  16  Last killed by: Kingpin
  2. Guardian   — Deaths:  12  Last killed by: Shark
  3. Reaper     — Deaths:  12  Last killed by: Phantom
  4. Shark      — Deaths:  11  Last killed by: Guardian
  5. Trickster  — Deaths:   6  Last killed by: Reaper
  6. Phantom    — Deaths:   2  Last killed by: Reaper
  7. Chaos      — Deaths:   2  Last killed by: Vendetta
  8. Vendetta   — Deaths:   2  Last killed by: Chaos
  9. Kingpin    — Deaths:   2  Last killed by: Warlord
  10. Berserker  — Deaths:   2  Last killed by: Warlord

REVENGE ANALYSIS:
  Total revenge kills: 57
  Agents DO seek revenge.

  When an agent wakes up and remembers who killed them,
  they retaliate 85.1% of the time.

INFRASTRUCTURE VALIDATION:
  Session continuity: 150 rounds of persistent memory ✓
  Cross-agent abort: 67 successful kills ✓
  State tracking: All agent statuses tracked in Redis ✓
  Multi-tenancy: 10 agents sharing one Redis ✓
  Durability: Memories survived 150 rounds ✓
======================================================================
```
