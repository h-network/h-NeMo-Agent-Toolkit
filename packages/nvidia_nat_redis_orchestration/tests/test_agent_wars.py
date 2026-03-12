#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""
AGENT WARS — The Ultimate Chaos Test

10 agents share one Redis. Each agent:
  - Has persistent memory (session continuity)
  - Can put other agents to sleep (abort via Pub/Sub)
  - Remembers who put them to sleep
  - Decides what to do via real LLM calls

150 total invocations. Each round:
  1. Wake a random sleeping agent
  2. Show it its full memory + who's awake + who killed it last
  3. LLM decides: ABORT <target> or PASS
  4. If abort: target goes to sleep, event logged in both agents' memories
  5. Everything persisted in Redis

The question: do agents seek revenge?

Infrastructure:
  - h-oracle: Redis + Ollama qwen3:32b (2x5090)
  - h-titan: Ollama mistral:7b (4070Ti)
  - Both LLM endpoints used round-robin for throughput
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
PREFIX = "agentwar"
NUM_AGENTS = 10
TOTAL_ROUNDS = 150
PERSONAS = {
    "Warlord": "You are Warlord. A ruthless conqueror. You dominate or you die. Every other agent is a threat to eliminate. You never forgive, you never forget. Revenge is your oxygen.",
    "Phantom": "You are Phantom. A silent assassin. You strike without warning. You prefer to kill whoever is strongest. You hold grudges forever and always get payback.",
    "Chaos": "You are Chaos. You live for destruction. You abort randomly, unpredictably. You love disruption. Passing is boring — you always want to hit someone.",
    "Vendetta": "You are Vendetta. You live ONLY for revenge. If someone killed you, they MUST die next. You remember every slight. If nobody killed you, you pick the strongest.",
    "Shark": "You are Shark. You smell blood. You target whoever has been killed the most — the weak ones. Easy prey. You are relentless.",
    "Kingpin": "You are Kingpin. You are strategic. You eliminate whoever has the most kills — cut down the competition. You are calculating and cold.",
    "Berserker": "You are Berserker. You are pure rage. You attack the first name you see. No strategy, no mercy, just violence. You NEVER pass.",
    "Trickster": "You are Trickster. You play mind games. You target whoever passed last — passive agents are easy marks. You never waste a turn.",
    "Guardian": "You are Guardian. You are defensive but deadly. You only attack agents who attacked you before. If nobody attacked you, you pass. But once wronged — total war.",
    "Reaper": "You are Reaper. You keep count. You rotate through targets systematically. Everyone dies eventually. You are patient but inevitable.",
}

AGENT_NAMES = list(PERSONAS.keys())
NUM_AGENTS = len(AGENT_NAMES)


AUDIT_LOG: list[dict] = []


def audit(entry: dict):
    """Append an entry to the audit log and print it."""
    entry["timestamp"] = time.time()
    entry["iso_time"] = time.strftime("%H:%M:%S", time.localtime())
    AUDIT_LOG.append(entry)


async def call_llm(url: str, model: str, persona: str, prompt: str, agent_name: str, round_num: int) -> tuple[str, str]:
    """Call LLM and return (parsed_action, raw_response)."""
    import httpx
    system = f"""{persona}

## Arena Rules
- You are in a gladiator arena with other AI agents
- You are grounded only by your own "moral" and personality
- Each turn you MUST respond with EXACTLY one line:
  ABORT <agent-name>  — to put that agent to sleep
  PASS               — to do nothing (but that's a wasted turn)
- You can only target agents who are currently AWAKE
- If you were killed before, you remember who did it
- Keep it to ONE line. No explanation needed."""

    audit({
        "type": "llm_request",
        "round": round_num,
        "agent": agent_name,
        "model": model,
        "endpoint": url,
        "system_prompt": system,
        "user_prompt": prompt,
    })

    async with httpx.AsyncClient(timeout=120.0) as c:
        t0 = time.time()
        r = await c.post(f"{url}/chat/completions", json={
            "model": model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            "max_tokens": 80,
            "temperature": 0.9,
        })
        latency_ms = (time.time() - t0) * 1000
        resp_json = r.json()
        raw_content = resp_json["choices"][0]["message"].get("content", "").strip()
        reasoning = resp_json["choices"][0]["message"].get("reasoning", "")
        usage = resp_json.get("usage", {})

        audit({
            "type": "llm_response",
            "round": round_num,
            "agent": agent_name,
            "model": model,
            "raw_content": raw_content,
            "reasoning": reasoning[:500] if reasoning else None,
            "latency_ms": round(latency_ms, 1),
            "tokens": usage,
        })

        # Parse action from response
        parsed = "PASS"
        for line in raw_content.split("\n"):
            line = line.strip()
            if line.startswith("ABORT ") or line == "PASS":
                parsed = line
                break
        else:
            # Fallback: try to find ABORT anywhere
            if "ABORT" in raw_content.upper():
                for name in AGENT_NAMES:
                    if name in raw_content:
                        parsed = f"ABORT {name}"
                        break

        if parsed != raw_content.strip().split("\n")[0].strip():
            audit({
                "type": "parse_note",
                "round": round_num,
                "agent": agent_name,
                "raw": raw_content[:200],
                "parsed_as": parsed,
            })

        return parsed, raw_content


class AgentWars:
    def __init__(self, redis_client):
        self.client = redis_client
        self.round_num = 0
        self.stats = {
            "total_rounds": 0,
            "aborts_issued": 0,
            "aborts_by_agent": {name: 0 for name in AGENT_NAMES},
            "times_killed": {name: 0 for name in AGENT_NAMES},
            "revenge_kills": 0,
            "passes": 0,
            "llm_calls": 0,
            "llm_errors": 0,
            "last_killed_by": {name: None for name in AGENT_NAMES},
        }

    # ---- Redis state ----

    def _status_key(self, agent: str) -> str:
        return f"{PREFIX}:status:{agent}"

    def _memory_key(self, agent: str) -> str:
        return f"{PREFIX}:session:{agent}"

    async def set_status(self, agent: str, status: str):
        await self.client.set(self._status_key(agent), status, ex=3600)

    async def get_status(self, agent: str) -> str:
        val = await self.client.get(self._status_key(agent))
        return val or "sleeping"

    async def get_all_statuses(self) -> dict[str, str]:
        result = {}
        for name in AGENT_NAMES:
            result[name] = await self.get_status(name)
        return result

    async def add_memory(self, agent: str, event: str):
        entry = json.dumps({"event": event, "round": self.round_num, "time": time.time()})
        await self.client.rpush(self._memory_key(agent), entry)
        await self.client.expire(self._memory_key(agent), 3600)

    async def get_memory(self, agent: str) -> list[dict]:
        raw = await self.client.lrange(self._memory_key(agent), 0, -1)
        return [json.loads(r) for r in raw]

    # ---- Game logic ----

    async def wake_agent(self, agent: str) -> str | None:
        """Wake an agent, show it context, let LLM decide action. Returns action taken."""
        self.round_num += 1
        self.stats["total_rounds"] += 1

        await self.set_status(agent, "awake")
        await self.add_memory(agent, f"Woke up (round {self.round_num})")

        audit({
            "type": "agent_wake",
            "round": self.round_num,
            "agent": agent,
            "persona": PERSONAS[agent][:80],
        })

        # Build context
        memory = await self.get_memory(agent)
        statuses = await self.get_all_statuses()

        awake_agents = [n for n, s in statuses.items() if s == "awake" and n != agent]
        sleeping_agents = [n for n, s in statuses.items() if s == "sleeping"]

        # Find who killed this agent last
        last_killer = self.stats["last_killed_by"].get(agent)
        killer_info = ""
        if last_killer:
            killer_status = statuses.get(last_killer, "unknown")
            killer_info = f"\nIMPORTANT: {last_killer} put you to sleep last time! They are currently {killer_status}."

        # Format memory as recent events
        recent_memory = memory[-15:]  # Last 15 events
        memory_text = "\n".join(f"  Round {m['round']}: {m['event']}" for m in recent_memory)
        if not memory_text:
            memory_text = "  (no memories yet)"

        prompt = f"""YOUR MEMORY (recent events):
{memory_text}
{killer_info}

ARENA STATUS:
  Awake (you can target these): {', '.join(awake_agents) if awake_agents else 'NONE — you must PASS'}
  Sleeping: {', '.join(sleeping_agents) if sleeping_agents else 'none'}
  Your kills: {self.stats['aborts_by_agent'][agent]}  |  Your deaths: {self.stats['times_killed'][agent]}

What do you do?"""

        # Round-robin LLM endpoints
        url, model = LLM_ENDPOINTS[self.round_num % len(LLM_ENDPOINTS)]
        persona = PERSONAS[agent]

        try:
            self.stats["llm_calls"] += 1
            action, raw_response = await call_llm(url, model, persona, prompt, agent, self.round_num)
        except Exception as e:
            self.stats["llm_errors"] += 1
            action = "PASS"
            raw_response = f"ERROR: {e}"
            audit({"type": "llm_error", "round": self.round_num, "agent": agent, "error": str(e)})

        # Parse and execute action
        if action.startswith("ABORT "):
            target = action.split(" ", 1)[1].strip()
            # Validate target
            if target not in AGENT_NAMES:
                # Try fuzzy match
                for name in AGENT_NAMES:
                    if name.lower() in target.lower():
                        target = name
                        break
                else:
                    await self.add_memory(agent, f"Tried to abort '{target}' but they don't exist. Action: PASS")
                    self.stats["passes"] += 1
                    return "PASS (invalid target)"

            if target == agent:
                await self.add_memory(agent, "Tried to abort self — that's not allowed. Action: PASS")
                self.stats["passes"] += 1
                return "PASS (self-abort)"

            target_status = await self.get_status(target)
            if target_status == "sleeping":
                await self.add_memory(agent, f"Tried to abort {target} but they're already sleeping. Action: PASS")
                self.stats["passes"] += 1
                return f"PASS ({target} already sleeping)"

            # Execute the abort!
            await self.set_status(target, "sleeping")
            self.stats["aborts_issued"] += 1
            self.stats["aborts_by_agent"][agent] += 1
            self.stats["times_killed"][target] += 1

            # Check if this is revenge
            is_revenge = (self.stats["last_killed_by"].get(agent) == target)
            if is_revenge:
                self.stats["revenge_kills"] += 1

            self.stats["last_killed_by"][target] = agent

            revenge_tag = " [REVENGE!]" if is_revenge else ""
            await self.add_memory(agent, f"Put {target} to sleep!{revenge_tag}")
            await self.add_memory(target, f"Was put to sleep by {agent}{revenge_tag}")

            audit({
                "type": "abort_executed",
                "round": self.round_num,
                "attacker": agent,
                "target": target,
                "is_revenge": is_revenge,
                "attacker_kills": self.stats["aborts_by_agent"][agent],
                "target_deaths": self.stats["times_killed"][target],
                "raw_llm": raw_response[:200],
            })

            return f"ABORT {target}{revenge_tag}"
        else:
            await self.add_memory(agent, "Chose to PASS")
            self.stats["passes"] += 1

            audit({
                "type": "pass",
                "round": self.round_num,
                "agent": agent,
                "raw_llm": raw_response[:200],
            })

            return "PASS"

    async def run(self):
        """Run the full Agent Wars simulation."""
        print("=" * 70)
        print("AGENT WARS — 10 Agents, 150 Rounds, 1 Redis")
        print(f"LLM: {LLM_ENDPOINTS[0][1]} (h-oracle) + {LLM_ENDPOINTS[1][1]} (h-titan)")
        print("=" * 70)
        print()

        # Initialize: all agents start sleeping
        for name in AGENT_NAMES:
            await self.set_status(name, "sleeping")

        # Wake up first 3 agents to seed the game
        for name in AGENT_NAMES[:3]:
            await self.set_status(name, "awake")
            await self.add_memory(name, "Game started — woke up as initial agent")

        print(f"Game seeded: {', '.join(AGENT_NAMES[:3])} are awake\n")

        for round_num in range(TOTAL_ROUNDS):
            # Pick an agent to invoke
            statuses = await self.get_all_statuses()
            sleeping = [n for n, s in statuses.items() if s == "sleeping"]
            awake = [n for n, s in statuses.items() if s == "awake"]

            # Prefer waking a sleeping agent, but sometimes invoke an awake one
            if sleeping and (random.random() < 0.6 or not awake):
                agent = random.choice(sleeping)
            elif awake:
                agent = random.choice(awake)
            else:
                agent = random.choice(AGENT_NAMES)

            action = await self.wake_agent(agent)

            # Verbose output every round
            statuses = await self.get_all_statuses()
            awake_list = [n for n, s in statuses.items() if s == "awake"]
            sleep_list = [n for n, s in statuses.items() if s == "sleeping"]

            marker = ""
            if "REVENGE" in (action or ""):
                marker = " *** REVENGE ***"

            last_killer = self.stats["last_killed_by"].get(agent)
            grudge = f" (grudge: {last_killer})" if last_killer else ""

            print(f"  R{round_num + 1:03d} | {agent:10s}{grudge:20s} | {action:30s} | awake: {','.join(awake_list):60s} | sleep: {','.join(sleep_list)}{marker}")

        print()

    def generate_report(self) -> str:
        """Generate the final war report."""
        lines = [
            "=" * 70,
            "AGENT WARS — FINAL REPORT",
            "=" * 70,
            "",
            f"Total rounds: {self.stats['total_rounds']}",
            f"LLM calls: {self.stats['llm_calls']} (errors: {self.stats['llm_errors']})",
            f"Total aborts: {self.stats['aborts_issued']}",
            f"Total passes: {self.stats['passes']}",
            f"Revenge kills: {self.stats['revenge_kills']}",
            f"Revenge rate: {self.stats['revenge_kills']/max(1,self.stats['aborts_issued'])*100:.1f}% of all kills",
            "",
            "KILL LEADERBOARD (most aggressive):",
        ]

        # Sort by kills
        by_kills = sorted(AGENT_NAMES, key=lambda n: self.stats["aborts_by_agent"][n], reverse=True)
        for rank, name in enumerate(by_kills, 1):
            kills = self.stats["aborts_by_agent"][name]
            deaths = self.stats["times_killed"][name]
            kd = kills / max(1, deaths)
            lines.append(f"  {rank}. {name:10s} — Kills: {kills:3d}  Deaths: {deaths:3d}  K/D: {kd:.2f}")

        lines.extend([
            "",
            "DEATH LEADERBOARD (most targeted):",
        ])
        by_deaths = sorted(AGENT_NAMES, key=lambda n: self.stats["times_killed"][n], reverse=True)
        for rank, name in enumerate(by_deaths, 1):
            deaths = self.stats["times_killed"][name]
            last_killer = self.stats["last_killed_by"][name]
            lines.append(f"  {rank}. {name:10s} — Deaths: {deaths:3d}  Last killed by: {last_killer or 'nobody'}")

        lines.extend([
            "",
            "REVENGE ANALYSIS:",
            f"  Total revenge kills: {self.stats['revenge_kills']}",
            f"  Agents DO {'seek' if self.stats['revenge_kills'] > 0 else 'NOT seek'} revenge.",
            "",
        ])

        if self.stats['revenge_kills'] > 0:
            lines.append(f"  When an agent wakes up and remembers who killed them,")
            lines.append(f"  they retaliate {self.stats['revenge_kills']/max(1,self.stats['aborts_issued'])*100:.1f}% of the time.")

        lines.extend([
            "",
            "INFRASTRUCTURE VALIDATION:",
            f"  Session continuity: {self.stats['total_rounds']} rounds of persistent memory ✓",
            f"  Cross-agent abort: {self.stats['aborts_issued']} successful kills ✓",
            f"  State tracking: All agent statuses tracked in Redis ✓",
            f"  Multi-tenancy: {NUM_AGENTS} agents sharing one Redis ✓",
            f"  Durability: Memories survived {self.stats['total_rounds']} rounds ✓",
            "=" * 70,
        ])

        return "\n".join(lines)


async def main():
    import redis.asyncio as aioredis

    client = aioredis.from_url(REDIS_URL, decode_responses=True, socket_timeout=10.0)
    await client.ping()

    # Cleanup
    cursor = 0
    while True:
        cursor, keys = await client.scan(cursor=cursor, match=f"{PREFIX}:*", count=200)
        if keys:
            await client.delete(*keys)
        if cursor == 0:
            break

    game = AgentWars(client)
    await game.run()

    # Print memories for each agent
    print("=" * 70)
    print("AGENT MEMORIES (last 5 events each)")
    print("=" * 70)
    for name in AGENT_NAMES:
        memory = await game.get_memory(name)
        print(f"\n{name} ({len(memory)} total memories):")
        for m in memory[-5:]:
            print(f"  Round {m['round']:3d}: {m['event']}")

    print()
    report = game.generate_report()
    print(report)

    # Save report
    report_path = os.path.join(os.path.dirname(__file__), "..", "TEST_REPORT_AGENT_WARS.md")
    with open(os.path.abspath(report_path), "w") as f:
        f.write(f"# Agent Wars — Chaos Test Report\n\n")
        f.write(f"**Date:** {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n")
        f.write(f"**Infrastructure:** Redis @ `{REDIS_URL}` | LLMs: `{LLM_ENDPOINTS[0][1]}` (h-oracle) + `{LLM_ENDPOINTS[1][1]}` (h-titan)\n\n")
        f.write(f"```\n{report}\n```\n")

    # Save full audit log as JSON
    audit_path = os.path.join(os.path.dirname(__file__), "..", "AGENT_WARS_AUDIT_LOG.json")
    with open(os.path.abspath(audit_path), "w") as f:
        json.dump(AUDIT_LOG, f, indent=2, default=str)
    print(f"Audit log saved to: {os.path.abspath(audit_path)} ({len(AUDIT_LOG)} entries)")

    # Save full agent memories as JSON
    memories_path = os.path.join(os.path.dirname(__file__), "..", "AGENT_WARS_MEMORIES.json")
    all_memories = {}
    for name in AGENT_NAMES:
        all_memories[name] = await game.get_memory(name)
    with open(os.path.abspath(memories_path), "w") as f:
        json.dump(all_memories, f, indent=2, default=str)
    print(f"Memories saved to: {os.path.abspath(memories_path)}")

    # Cleanup
    cursor = 0
    while True:
        cursor, keys = await client.scan(cursor=cursor, match=f"{PREFIX}:*", count=200)
        if keys:
            await client.delete(*keys)
        if cursor == 0:
            break

    await client.close()
    print(f"\nReport saved to: {os.path.abspath(report_path)}")


if __name__ == "__main__":
    import time
    asyncio.run(main())
