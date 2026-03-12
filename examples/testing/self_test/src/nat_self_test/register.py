# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Self-testing QA agent tools.

Provides tools that let a NAT agent test the orchestration middleware
it is running under — the ultimate dog-food.
"""

from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import AsyncGenerator

from pydantic import Field

from nat.builder.builder import Builder
from nat.builder.function import FunctionGroup
from nat.cli.register_workflow import register_function_group
from nat.data_models.function import FunctionGroupBaseConfig


class QAToolsConfig(FunctionGroupBaseConfig, name="qa_tools"):
    """Configuration for the self-testing QA tools."""

    redis_url: str = Field(
        default="redis://localhost:6379",
        description="Redis URL for querying orchestration state.",
    )
    key_prefix: str = Field(
        default="nat:orch",
        description="Key prefix used by the orchestration middleware.",
    )
    test_directory: str = Field(
        default="packages/nvidia_nat_redis_orchestration/tests",
        description="Path to the test directory to run.",
    )


@register_function_group(config_type=QAToolsConfig)
async def qa_tools(config: QAToolsConfig, _builder: Builder) -> AsyncGenerator[FunctionGroup, None]:
    """QA tools that let the agent test the orchestration middleware."""

    import redis.asyncio as aioredis

    client = aioredis.from_url(config.redis_url, decode_responses=True, socket_timeout=5.0)
    await client.ping()

    group = FunctionGroup(config=config)

    # ------------------------------------------------------------------
    # Tool 1: Run pytest
    # ------------------------------------------------------------------
    async def run_pytest(test_file: str) -> str:
        """Run a pytest test file or suite and return the results summary.

        Args:
            test_file: Name of the test file to run (e.g. 'test_session.py'),
                       or 'all' to run the entire test suite.
                       Do NOT include test_integration.py or test_full_report files.
        """
        if test_file == "all":
            path = config.test_directory
        else:
            path = f"{config.test_directory}/{test_file}"

        try:
            result = subprocess.run(
                [sys.executable, "-m", "pytest", path, "-v", "--tb=short",
                 "--ignore", f"{config.test_directory}/test_integration.py",
                 "--ignore", f"{config.test_directory}/test_full_report.py",
                 "--ignore", f"{config.test_directory}/test_full_report_v2.py"],
                capture_output=True, text=True, timeout=120,
            )
            output = result.stdout[-3000:] if len(result.stdout) > 3000 else result.stdout
            if result.returncode != 0 and result.stderr:
                output += f"\nSTDERR:\n{result.stderr[-1000:]}"
            return output
        except subprocess.TimeoutExpired:
            return "ERROR: Test execution timed out after 120 seconds."
        except Exception as e:
            return f"ERROR: {type(e).__name__}: {e}"

    # ------------------------------------------------------------------
    # Tool 2: Query Redis keys
    # ------------------------------------------------------------------
    async def query_redis(pattern: str) -> str:
        """Scan Redis for keys matching a pattern and return their values.

        Args:
            pattern: Redis key pattern (e.g. 'nat:orch:state:*' or 'nat:orch:session:*').
                     Use '*' wildcards.
        """
        results = {}
        cursor = 0
        while True:
            cursor, keys = await client.scan(cursor=cursor, match=pattern, count=100)
            for key in keys[:20]:  # Limit to 20 keys
                key_str = key if isinstance(key, str) else key.decode()
                key_type = await client.type(key_str)
                if key_type == "string":
                    val = await client.get(key_str)
                    try:
                        results[key_str] = json.loads(val)
                    except (json.JSONDecodeError, TypeError):
                        results[key_str] = val
                elif key_type == "list":
                    items = await client.lrange(key_str, 0, -1)
                    results[key_str] = [json.loads(i) if isinstance(i, str) else i for i in items[:10]]
                else:
                    results[key_str] = f"<{key_type}>"
            if cursor == 0:
                break

        if not results:
            return f"No keys found matching pattern: {pattern}"
        return json.dumps(results, indent=2, default=str)

    # ------------------------------------------------------------------
    # Tool 3: Check task state
    # ------------------------------------------------------------------
    async def check_task_state(task_id: str) -> str:
        """Check the orchestration state for a specific task ID.

        Args:
            task_id: The task ID to look up (hex string).
        """
        key = f"{config.key_prefix}:state:{task_id}"
        raw = await client.get(key)
        if raw is None:
            return f"No state found for task {task_id} (key may have expired)"
        try:
            data = json.loads(raw)
            return json.dumps(data, indent=2)
        except (json.JSONDecodeError, TypeError):
            return str(raw)

    # ------------------------------------------------------------------
    # Tool 4: Send abort signal
    # ------------------------------------------------------------------
    async def send_abort(task_id: str) -> str:
        """Send an abort signal to a running task via Redis Pub/Sub.

        Args:
            task_id: The task ID to abort (hex string).
        """
        import time
        channel = f"{config.key_prefix}:abort:{task_id}"
        payload = json.dumps({"reason": "QA agent abort test", "timestamp": time.time()})
        receivers = await client.publish(channel, payload)
        return f"Abort sent to {channel}. Receivers: {receivers} ({'task was listening' if receivers > 0 else 'no active listener'})"

    # ------------------------------------------------------------------
    # Tool 5: Read source file
    # ------------------------------------------------------------------
    async def read_source(file_path: str) -> str:
        """Read a source file from the orchestration package.

        Args:
            file_path: Relative path within packages/nvidia_nat_redis_orchestration/
                       (e.g. 'src/nat/plugins/redis_orchestration/state_tracker.py')
        """
        full_path = f"packages/nvidia_nat_redis_orchestration/{file_path}"
        try:
            with open(full_path) as f:
                content = f.read()
            if len(content) > 4000:
                content = content[:4000] + f"\n... (truncated, {len(content)} total chars)"
            return content
        except FileNotFoundError:
            return f"File not found: {full_path}"
        except Exception as e:
            return f"Error reading file: {e}"

    # ------------------------------------------------------------------
    # Tool 6: Inspect own middleware state
    # ------------------------------------------------------------------
    async def inspect_own_state(unused: str) -> str:
        """Check Redis for state entries created by this agent's own middleware.

        Scans for recent orchestration state keys and returns them.
        This lets you verify the middleware is tracking YOUR execution.
        """
        results = {}
        cursor = 0
        while True:
            cursor, keys = await client.scan(cursor=cursor, match=f"{config.key_prefix}:state:*", count=100)
            for key in keys:
                key_str = key if isinstance(key, str) else key.decode()
                raw = await client.get(key_str)
                if raw:
                    try:
                        data = json.loads(raw)
                        results[key_str] = {
                            "state": data.get("state"),
                            "function_name": data.get("function_name"),
                            "instance_id": data.get("instance_id"),
                        }
                    except (json.JSONDecodeError, TypeError):
                        pass
            if cursor == 0:
                break

        if not results:
            return "No orchestration state entries found. The middleware may not be tracking or keys may have expired."

        return f"Found {len(results)} tracked executions:\n" + json.dumps(results, indent=2)

    group.add_function(name="run_pytest", fn=run_pytest, description=run_pytest.__doc__)
    group.add_function(name="query_redis", fn=query_redis, description=query_redis.__doc__)
    group.add_function(name="check_task_state", fn=check_task_state, description=check_task_state.__doc__)
    group.add_function(name="send_abort", fn=send_abort, description=send_abort.__doc__)
    group.add_function(name="read_source", fn=read_source, description=read_source.__doc__)
    group.add_function(name="inspect_own_state", fn=inspect_own_state, description=inspect_own_state.__doc__)

    yield group

    await client.close()
