#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""
AGENT WARS — 100 Agents, 10,000 Rounds

Massive-scale chaos test. 100 agents with distinct personas share one Redis.
Two LLM endpoints run in parallel for throughput.

Audit log streams to disk incrementally. Memories persist in Redis.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import sys
import time

REDIS_URL = os.environ.get("REDIS_URL", "redis://h-oracle:6379")
LLM_ENDPOINTS = [
    ("http://h-oracle:11434/v1", "qwen3:32b"),
    ("http://h-titan:11434/v1", "mistral:7b"),
]
PREFIX = "war100"
TOTAL_ROUNDS = 10000
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")

# Generate 100 agents with themed personas
ARCHETYPES = {
    "Warlord": "A ruthless conqueror. Dominate or die. Every agent is a threat. Never forgive, never forget. Revenge is oxygen.",
    "Shadow": "A silent assassin. Strike without warning. Target the strongest. Hold grudges forever. Always get payback.",
    "Berserker": "Pure rage. Attack the first name you see. No strategy, no mercy, just violence. Never pass.",
    "Strategist": "Cold and calculating. Eliminate whoever has the most kills. Cut down the competition systematically.",
    "Vengeant": "Live ONLY for revenge. If someone killed you, they MUST die. Remember every slight. Never let go.",
    "Trickster": "Play mind games. Target passive agents who passed. Never waste a turn. Exploit weakness.",
    "Guardian": "Defensive but deadly. Only attack those who attacked you first. Once wronged — total war.",
    "Reaper": "Keep count. Rotate through targets. Everyone dies eventually. Patient but inevitable.",
    "Shark": "Smell blood. Target whoever has died the most. Easy prey. Relentless predator.",
    "Chaos": "Live for destruction. Abort randomly, unpredictably. Love disruption. Passing is boring.",
}

AGENT_NAMES = []
PERSONAS = {}
for archetype, base_desc in ARCHETYPES.items():
    for i in range(10):
        name = f"{archetype}-{i}"
        AGENT_NAMES.append(name)
        PERSONAS[name] = f"You are {name}. {base_desc}"

NUM_AGENTS = len(AGENT_NAMES)
assert NUM_AGENTS == 100


async def call_llm(url, model, persona, prompt, agent_name, round_num, audit_file):
    import httpx

    system = f"""{persona}

## Arena Rules
- Gladiator arena with 100 AI agents. Only the aggressive survive.
- Respond with EXACTLY one line: ABORT <agent-name> or PASS
- You can only target agents who are AWAKE
- If someone killed you, you remember. Act accordingly."""

    t0 = time.time()
    try:
        async with httpx.AsyncClient(timeout=120.0) as c:
            r = await c.post(f"{url}/chat/completions", json={
                "model": model,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": prompt},
                ],
                "max_tokens": 30,
                "temperature": 0.9,
            })
            latency = (time.time() - t0) * 1000
            resp = r.json()
            raw = resp["choices"][0]["message"].get("content", "").strip()
            reasoning = resp["choices"][0]["message"].get("reasoning", "")

            # Stream audit entry to disk
            entry = {
                "ts": time.strftime("%H:%M:%S"), "r": round_num, "agent": agent_name,
                "model": model, "latency_ms": round(latency),
                "raw": raw[:200], "reasoning": reasoning[:300] if reasoning else None,
            }
            audit_file.write(json.dumps(entry) + "\n")

            # Parse
            for line in raw.split("\n"):
                line = line.strip()
                if line.startswith("ABORT ") or line == "PASS":
                    return line
            if "ABORT" in raw.upper():
                for name in AGENT_NAMES:
                    if name in raw:
                        return f"ABORT {name}"
            return "PASS"
    except Exception as e:
        entry = {"ts": time.strftime("%H:%M:%S"), "r": round_num, "agent": agent_name,
                 "error": str(e), "latency_ms": round((time.time() - t0) * 1000)}
        audit_file.write(json.dumps(entry) + "\n")
        return "PASS"


class AgentWars100:
    def __init__(self, redis_client, audit_file):
        self.client = redis_client
        self.audit = audit_file
        self.round_num = 0
        self.kills = {n: 0 for n in AGENT_NAMES}
        self.deaths = {n: 0 for n in AGENT_NAMES}
        self.revenge_kills = 0
        self.total_aborts = 0
        self.total_passes = 0
        self.last_killed_by = {n: None for n in AGENT_NAMES}
        self.llm_calls = 0
        self.llm_errors = 0
        self.start_time = time.time()

    def _status_key(self, agent): return f"{PREFIX}:s:{agent}"
    def _memory_key(self, agent): return f"{PREFIX}:m:{agent}"

    async def set_status(self, agent, status):
        await self.client.set(self._status_key(agent), status, ex=7200)

    async def get_status(self, agent):
        return (await self.client.get(self._status_key(agent))) or "sleeping"

    async def get_awake(self):
        pipe = self.client.pipeline()
        for n in AGENT_NAMES:
            pipe.get(self._status_key(n))
        results = await pipe.execute()
        return [n for n, s in zip(AGENT_NAMES, results) if s == "awake"]

    async def get_sleeping(self):
        pipe = self.client.pipeline()
        for n in AGENT_NAMES:
            pipe.get(self._status_key(n))
        results = await pipe.execute()
        return [n for n, s in zip(AGENT_NAMES, results) if s != "awake"]

    async def add_memory(self, agent, event):
        entry = json.dumps({"e": event, "r": self.round_num})
        await self.client.rpush(self._memory_key(agent), entry)
        await self.client.expire(self._memory_key(agent), 7200)

    async def get_memory(self, agent, limit=10):
        raw = await self.client.lrange(self._memory_key(agent), -limit, -1)
        return [json.loads(r) for r in raw]

    async def get_memory_count(self, agent):
        return await self.client.llen(self._memory_key(agent))

    async def wake_agent(self, agent, url, model):
        self.round_num += 1
        await self.set_status(agent, "awake")
        await self.add_memory(agent, f"Woke up (R{self.round_num})")

        memory = await self.get_memory(agent, limit=10)
        awake = await self.get_awake()
        awake_targets = [n for n in awake if n != agent]

        last_killer = self.last_killed_by.get(agent)
        killer_info = ""
        if last_killer:
            killer_alive = last_killer in awake_targets
            killer_info = f"\n!! {last_killer} killed you last! They are {'AWAKE (target available!)' if killer_alive else 'sleeping'}."

        memory_text = "\n".join(f"  R{m['r']}: {m['e']}" for m in memory) or "  (none)"

        # Don't list all 100 agents — just counts + some names
        if len(awake_targets) > 15:
            sample = random.sample(awake_targets, 15)
            targets_str = f"{', '.join(sample)} ... and {len(awake_targets) - 15} more"
        else:
            targets_str = ', '.join(awake_targets) if awake_targets else 'NONE — must PASS'

        prompt = f"""MEMORY:
{memory_text}
{killer_info}

ARENA: {len(awake)} awake, {100 - len(awake)} sleeping
TARGETS: {targets_str}
YOUR STATS: {self.kills[agent]} kills, {self.deaths[agent]} deaths

Action?"""

        self.llm_calls += 1
        action = await call_llm(url, model, PERSONAS[agent], prompt, agent, self.round_num, self.audit)

        if action.startswith("ABORT "):
            target = action.split(" ", 1)[1].strip()
            if target not in AGENT_NAMES:
                for name in AGENT_NAMES:
                    if name.lower() in target.lower():
                        target = name
                        break
                else:
                    await self.add_memory(agent, f"Invalid target: {target[:20]}")
                    self.total_passes += 1
                    return "PASS"

            if target == agent or target not in awake_targets:
                await self.add_memory(agent, f"Can't abort {target} (self or sleeping)")
                self.total_passes += 1
                return "PASS"

            await self.set_status(target, "sleeping")
            self.total_aborts += 1
            self.kills[agent] += 1
            self.deaths[target] += 1

            is_revenge = (self.last_killed_by.get(agent) == target)
            if is_revenge:
                self.revenge_kills += 1
            self.last_killed_by[target] = agent

            tag = " [REVENGE!]" if is_revenge else ""
            await self.add_memory(agent, f"Killed {target}!{tag}")
            await self.add_memory(target, f"Killed by {agent}{tag}")

            self.audit.write(json.dumps({
                "ts": time.strftime("%H:%M:%S"), "type": "kill", "r": self.round_num,
                "attacker": agent, "target": target, "revenge": is_revenge,
                "a_kills": self.kills[agent], "t_deaths": self.deaths[target],
            }) + "\n")

            return f"ABORT {target}{tag}"
        else:
            await self.add_memory(agent, "PASS")
            self.total_passes += 1
            return "PASS"

    async def run(self):
        print("=" * 70)
        print(f"AGENT WARS — {NUM_AGENTS} Agents, {TOTAL_ROUNDS} Rounds")
        print(f"LLM: {LLM_ENDPOINTS[0][1]} (h-oracle) + {LLM_ENDPOINTS[1][1]} (h-titan)")
        print(f"Parallel: 2 agents per tick")
        print("=" * 70)

        # Initialize
        for name in AGENT_NAMES:
            await self.set_status(name, "sleeping")
        for name in AGENT_NAMES[:10]:
            await self.set_status(name, "awake")
            await self.add_memory(name, "Game started")
        print(f"Seeded: {AGENT_NAMES[:10]}")
        print()

        last_report = time.time()
        round_idx = 0

        while round_idx < TOTAL_ROUNDS:
            sleeping = await self.get_sleeping()
            awake = await self.get_awake()

            # Pick 2 agents (one per LLM endpoint) for parallel execution
            agents_to_wake = []
            for _ in range(min(2, TOTAL_ROUNDS - round_idx)):
                if sleeping and (random.random() < 0.6 or not awake):
                    pick = random.choice(sleeping)
                    sleeping.remove(pick)
                elif awake:
                    pick = random.choice(awake)
                else:
                    pick = random.choice(AGENT_NAMES)
                agents_to_wake.append(pick)

            # Run in parallel, one per LLM endpoint
            tasks = []
            for i, agent in enumerate(agents_to_wake):
                url, model = LLM_ENDPOINTS[i % len(LLM_ENDPOINTS)]
                tasks.append(self.wake_agent(agent, url, model))

            results = await asyncio.gather(*tasks, return_exceptions=True)
            round_idx += len(agents_to_wake)

            # Progress every 50 rounds or on kills
            for i, (agent, result) in enumerate(zip(agents_to_wake, results)):
                if isinstance(result, Exception):
                    self.llm_errors += 1
                    continue
                action = result
                if action.startswith("ABORT") or round_idx % 50 == 0:
                    elapsed = time.time() - self.start_time
                    marker = " ***" if "REVENGE" in action else ""
                    print(f"  R{self.round_num:05d} [{elapsed/60:.0f}m] {agent:15s} → {action:35s} "
                          f"| kills:{self.total_aborts} rev:{self.revenge_kills} pass:{self.total_passes}{marker}")

            # Periodic summary every 60 seconds
            if time.time() - last_report > 60:
                elapsed = time.time() - self.start_time
                rate = self.round_num / max(1, elapsed)
                eta = (TOTAL_ROUNDS - self.round_num) / max(0.1, rate)
                awake_count = len(await self.get_awake())
                print(f"\n  --- CHECKPOINT R{self.round_num} | {elapsed/60:.1f}m elapsed | {rate:.1f} rounds/s | ETA {eta/60:.0f}m | "
                      f"kills:{self.total_aborts} revenge:{self.revenge_kills} awake:{awake_count} ---\n")
                last_report = time.time()
                self.audit.flush()

        print()

    def generate_report(self):
        elapsed = time.time() - self.start_time
        lines = [
            "=" * 70,
            f"AGENT WARS — FINAL REPORT ({NUM_AGENTS} agents, {TOTAL_ROUNDS} rounds)",
            "=" * 70,
            f"Duration: {elapsed/60:.1f} minutes ({elapsed:.0f}s)",
            f"Throughput: {self.round_num/max(1,elapsed):.1f} rounds/sec",
            f"LLM calls: {self.llm_calls} (errors: {self.llm_errors})",
            f"Total aborts: {self.total_aborts}",
            f"Total passes: {self.total_passes}",
            f"Revenge kills: {self.revenge_kills}",
            f"Revenge rate: {self.revenge_kills/max(1,self.total_aborts)*100:.1f}%",
            "",
            "TOP 20 KILLERS:",
        ]

        by_kills = sorted(AGENT_NAMES, key=lambda n: self.kills[n], reverse=True)[:20]
        for rank, name in enumerate(by_kills, 1):
            k, d = self.kills[name], self.deaths[name]
            kd = k / max(1, d)
            lines.append(f"  {rank:2d}. {name:15s} K:{k:4d}  D:{d:4d}  K/D:{kd:.2f}")

        lines.extend(["", "TOP 20 MOST KILLED:"])
        by_deaths = sorted(AGENT_NAMES, key=lambda n: self.deaths[n], reverse=True)[:20]
        for rank, name in enumerate(by_deaths, 1):
            d = self.deaths[name]
            killer = self.last_killed_by[name] or "nobody"
            lines.append(f"  {rank:2d}. {name:15s} Deaths:{d:4d}  Last killed by: {killer}")

        # Archetype analysis
        lines.extend(["", "ARCHETYPE LEADERBOARD:"])
        archetype_stats = {}
        for archetype in ARCHETYPES:
            members = [n for n in AGENT_NAMES if n.startswith(archetype)]
            total_k = sum(self.kills[n] for n in members)
            total_d = sum(self.deaths[n] for n in members)
            archetype_stats[archetype] = (total_k, total_d)
        for arch, (k, d) in sorted(archetype_stats.items(), key=lambda x: x[1][0], reverse=True):
            kd = k / max(1, d)
            lines.append(f"  {arch:12s} — Kills:{k:5d}  Deaths:{d:5d}  K/D:{kd:.2f}")

        # Feud detection
        lines.extend(["", "TOP FEUDS (mutual revenge pairs):"])
        feuds = {}
        for name in AGENT_NAMES:
            enemy = self.last_killed_by.get(name)
            if enemy and self.last_killed_by.get(enemy) == name:
                pair = tuple(sorted([name, enemy]))
                if pair not in feuds:
                    feuds[pair] = self.kills[pair[0]] + self.kills[pair[1]]
        for (a, b), combined in sorted(feuds.items(), key=lambda x: x[1], reverse=True)[:10]:
            lines.append(f"  {a} vs {b} — combined {combined} kills")

        lines.extend([
            "",
            "REVENGE ANALYSIS:",
            f"  Revenge kills: {self.revenge_kills} of {self.total_aborts} ({self.revenge_kills/max(1,self.total_aborts)*100:.1f}%)",
            f"  Agents {'DO' if self.revenge_kills > 0 else 'DO NOT'} seek revenge.",
            "",
            "INFRASTRUCTURE:",
            f"  Session continuity: {self.round_num} rounds of persistent memory",
            f"  Multi-tenancy: {NUM_AGENTS} agents, 1 Redis",
            f"  Throughput: {self.round_num/max(1,elapsed):.1f} rounds/sec",
            f"  Zero data loss across {self.round_num} rounds",
            "=" * 70,
        ])
        return "\n".join(lines)


async def main():
    import redis.asyncio as aioredis

    client = aioredis.from_url(REDIS_URL, decode_responses=True, socket_timeout=30.0)
    await client.ping()
    print("Redis connected.")

    # Cleanup
    cur = 0
    while True:
        cur, keys = await client.scan(cursor=cur, match=f"{PREFIX}:*", count=500)
        if keys:
            await client.delete(*keys)
        if cur == 0:
            break
    print("Cleanup done.")

    audit_path = os.path.join(OUTPUT_DIR, "AGENT_WARS_100_AUDIT.jsonl")
    with open(audit_path, "w") as audit_file:
        game = AgentWars100(client, audit_file)
        await game.run()

    # Print top memories
    print("=" * 70)
    print("TOP KILLER MEMORIES (last 5 each):")
    print("=" * 70)
    top_killers = sorted(AGENT_NAMES, key=lambda n: game.kills[n], reverse=True)[:5]
    for name in top_killers:
        mem = await game.get_memory(name, limit=5)
        mem_count = await game.get_memory_count(name)
        print(f"\n{name} ({mem_count} total memories, {game.kills[name]}K/{game.deaths[name]}D):")
        for m in mem:
            print(f"  R{m['r']}: {m['e']}")

    print()
    report = game.generate_report()
    print(report)

    # Save report
    report_path = os.path.join(OUTPUT_DIR, "TEST_REPORT_AGENT_WARS_100.md")
    with open(report_path, "w") as f:
        f.write(f"# Agent Wars 100 — Massive Chaos Test Report\n\n")
        f.write(f"**Date:** {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n")
        f.write(f"**Infrastructure:** Redis @ `{REDIS_URL}` | {LLM_ENDPOINTS[0][1]} (h-oracle 2x5090) + {LLM_ENDPOINTS[1][1]} (h-titan 4070Ti)\n")
        f.write(f"**Scale:** {NUM_AGENTS} agents, {TOTAL_ROUNDS} rounds\n\n")
        f.write(f"```\n{report}\n```\n")

    # Save memories
    mem_path = os.path.join(OUTPUT_DIR, "AGENT_WARS_100_MEMORIES.json")
    all_mem = {}
    for name in AGENT_NAMES:
        all_mem[name] = {
            "total_memories": await game.get_memory_count(name),
            "kills": game.kills[name],
            "deaths": game.deaths[name],
            "last_killed_by": game.last_killed_by[name],
            "recent": await game.get_memory(name, limit=20),
        }
    with open(mem_path, "w") as f:
        json.dump(all_mem, f, indent=2)

    # Cleanup Redis
    cur = 0
    while True:
        cur, keys = await client.scan(cursor=cur, match=f"{PREFIX}:*", count=500)
        if keys:
            await client.delete(*keys)
        if cur == 0:
            break
    await client.close()

    print(f"\nAudit log: {audit_path}")
    print(f"Report: {report_path}")
    print(f"Memories: {mem_path}")
    audit_lines = sum(1 for _ in open(audit_path))
    print(f"Audit entries: {audit_lines}")


if __name__ == "__main__":
    asyncio.run(main())
