#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""
Run the self-testing QA agent.

Uses the NeMo Agent Toolkit to load a workflow that tests the toolkit's
own Redis orchestration middleware — while running under that middleware.

Usage:
    python run_qa_agent.py [prompt]

Default prompt asks the agent to run all tests and verify its own state.
"""

from __future__ import annotations

import asyncio
import sys


async def main():
    from nat.runtime.loader import discover_and_register_plugins
    from nat.runtime.loader import PluginTypes
    from nat.runtime.loader import load_workflow

    # Register all NAT plugins (middleware, functions, LLMs, etc.)
    discover_and_register_plugins(PluginTypes.ALL)

    # Register our local QA tools
    import nat_self_test.register  # noqa: F401

    config_file = "examples/testing/self_test/src/nat_self_test/configs/config.yml"

    prompt = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else (
        "Run ALL the unit test suites for the orchestration package one by one: "
        "test_config.py, test_state_tracker.py, test_abort_controller.py, "
        "test_crash_recovery.py, test_orchestration_middleware.py, and test_session.py. "
        "Report the results for each. "
        "Then use inspect_own_state to check if your own LLM calls were tracked in Redis "
        "by the orchestration middleware. "
        "Finally, give me a summary of everything."
    )

    print("=" * 70)
    print("SELF-TESTING QA AGENT")
    print("=" * 70)
    print(f"Prompt: {prompt[:100]}...")
    print("=" * 70)
    print()

    async with load_workflow(config_file) as session_manager:
        async with session_manager.run(prompt) as runner:
            result = await runner.result(to_type=str)

    print()
    print("=" * 70)
    print("AGENT RESPONSE:")
    print("=" * 70)
    print(result)
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
