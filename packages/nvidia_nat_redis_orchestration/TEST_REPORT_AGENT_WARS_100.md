# Agent Wars 100 — Massive Chaos Test Report

**Date:** 2026-03-12 20:43:34 UTC
**Infrastructure:** Redis @ `redis://h-oracle:6379` | qwen3:32b (h-oracle 2x5090) + mistral:7b (h-titan 4070Ti)
**Scale:** 100 agents, 10000 rounds

```
======================================================================
AGENT WARS — FINAL REPORT (100 agents, 10000 rounds)
======================================================================
Duration: 52.0 minutes (3122s)
Throughput: 3.2 rounds/sec
LLM calls: 10000 (errors: 0)
Total aborts: 3761
Total passes: 6239
Revenge kills: 3632
Revenge rate: 96.6%

TOP 20 KILLERS:
   1. Chaos-0         K:  84  D: 106  K/D:0.79
   2. Warlord-0       K:  77  D:  98  K/D:0.79
   3. Warlord-8       K:  68  D:  82  K/D:0.83
   4. Vengeant-4      K:  67  D:  71  K/D:0.94
   5. Strategist-2    K:  66  D:  49  K/D:1.35
   6. Strategist-6    K:  62  D:  51  K/D:1.22
   7. Berserker-3     K:  61  D:  81  K/D:0.75
   8. Chaos-4         K:  61  D:  49  K/D:1.24
   9. Chaos-6         K:  59  D:  58  K/D:1.02
  10. Berserker-7     K:  58  D:  64  K/D:0.91
  11. Chaos-5         K:  58  D:  59  K/D:0.98
  12. Warlord-2       K:  57  D:  59  K/D:0.97
  13. Warlord-3       K:  57  D:  45  K/D:1.27
  14. Berserker-2     K:  57  D:  78  K/D:0.73
  15. Warlord-4       K:  56  D:  44  K/D:1.27
  16. Berserker-5     K:  54  D:  34  K/D:1.59
  17. Berserker-8     K:  54  D:  37  K/D:1.46
  18. Vengeant-5      K:  54  D:  42  K/D:1.29
  19. Warlord-7       K:  53  D:  44  K/D:1.20
  20. Chaos-9         K:  53  D:  54  K/D:0.98

TOP 20 MOST KILLED:
   1. Chaos-0         Deaths: 106  Last killed by: Chaos-4
   2. Warlord-0       Deaths:  98  Last killed by: Strategist-2
   3. Warlord-8       Deaths:  82  Last killed by: Berserker-4
   4. Berserker-3     Deaths:  81  Last killed by: Berserker-5
   5. Berserker-2     Deaths:  78  Last killed by: Berserker-8
   6. Shadow-6        Deaths:  74  Last killed by: Warlord-7
   7. Chaos-8         Deaths:  72  Last killed by: Strategist-7
   8. Vengeant-4      Deaths:  71  Last killed by: Shark-9
   9. Trickster-2     Deaths:  69  Last killed by: Chaos-3
  10. Berserker-7     Deaths:  64  Last killed by: Strategist-9
  11. Shark-0         Deaths:  63  Last killed by: Vengeant-5
  12. Chaos-7         Deaths:  62  Last killed by: Strategist-6
  13. Warlord-2       Deaths:  59  Last killed by: Strategist-0
  14. Chaos-5         Deaths:  59  Last killed by: Chaos-6
  15. Chaos-6         Deaths:  58  Last killed by: Chaos-5
  16. Chaos-9         Deaths:  54  Last killed by: Chaos-1
  17. Shadow-0        Deaths:  52  Last killed by: Warlord-5
  18. Shark-7         Deaths:  52  Last killed by: Warlord-4
  19. Strategist-6    Deaths:  51  Last killed by: Chaos-7
  20. Warlord-6       Deaths:  50  Last killed by: Warlord-2

ARCHETYPE LEADERBOARD:
  Warlord      — Kills:  553  Deaths:  520  K/D:1.06
  Chaos        — Kills:  548  Deaths:  559  K/D:0.98
  Berserker    — Kills:  492  Deaths:  482  K/D:1.02
  Strategist   — Kills:  480  Deaths:  313  K/D:1.53
  Vengeant     — Kills:  455  Deaths:  401  K/D:1.13
  Shadow       — Kills:  369  Deaths:  400  K/D:0.92
  Shark        — Kills:  298  Deaths:  334  K/D:0.89
  Trickster    — Kills:  265  Deaths:  322  K/D:0.82
  Guardian     — Kills:  157  Deaths:  234  K/D:0.67
  Reaper       — Kills:  144  Deaths:  196  K/D:0.73

TOP FEUDS (mutual revenge pairs):
  Chaos-0 vs Chaos-4 — combined 145 kills
  Strategist-2 vs Warlord-0 — combined 143 kills
  Chaos-5 vs Chaos-6 — combined 117 kills
  Berserker-3 vs Berserker-5 — combined 115 kills
  Chaos-7 vs Strategist-6 — combined 113 kills
  Berserker-2 vs Berserker-8 — combined 111 kills
  Berserker-4 vs Warlord-8 — combined 107 kills
  Shadow-6 vs Warlord-7 — combined 105 kills
  Chaos-1 vs Chaos-9 — combined 105 kills
  Strategist-0 vs Warlord-2 — combined 103 kills

REVENGE ANALYSIS:
  Revenge kills: 3632 of 3761 (96.6%)
  Agents DO seek revenge.

INFRASTRUCTURE:
  Session continuity: 10000 rounds of persistent memory
  Multi-tenancy: 100 agents, 1 Redis
  Throughput: 3.2 rounds/sec
  Zero data loss across 10000 rounds
======================================================================
```
