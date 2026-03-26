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
"""Configuration for Redis orchestration middleware."""

from __future__ import annotations

from pydantic import Field

from nat.data_models.common import OptionalSecretStr
from nat.middleware.dynamic.dynamic_middleware_config import DynamicMiddlewareConfig


class RedisOrchestrationConfig(DynamicMiddlewareConfig, name="redis_orchestration"):
    """Configuration for Redis-backed task orchestration middleware.

    Provides task lifecycle state tracking, external abort via Pub/Sub,
    and crash recovery for orphaned tasks.
    """

    redis_url: str = Field(
        description="Redis connection URL (e.g. redis://localhost:6379).",
    )

    redis_password: OptionalSecretStr = Field(
        default=None,
        description="Redis password. Overrides any password embedded in redis_url.",
    )

    enable_state_tracking: bool = Field(
        default=True,
        description="Enable task lifecycle state tracking in Redis.",
    )

    enable_abort: bool = Field(
        default=True,
        description="Enable external abort via Redis Pub/Sub.",
    )

    state_ttl: int = Field(
        default=3600,
        gt=0,
        description="TTL in seconds for task state keys in Redis.",
    )

    key_prefix: str = Field(
        default="nat:orch",
        description="Redis key namespace prefix for all orchestration keys.",
    )

    crash_recovery_on_startup: bool = Field(
        default=True,
        description="Run SCAN-based recovery for orphaned running tasks on middleware init.",
    )

    instance_id: str | None = Field(
        default=None,
        description="Instance identifier for multi-instance deployments. Auto-generated UUID if omitted.",
    )

    # --- Session Continuity ---

    enable_session_continuity: bool = Field(
        default=False,
        description="Enable conversation session persistence in Redis.",
    )

    session_ttl: int = Field(
        default=86400,
        gt=0,
        description="TTL in seconds for session data (default 24h).",
    )

    session_max_turns: int = Field(
        default=200,
        gt=0,
        description="Maximum conversation turns per session before rotation.",
    )

    session_max_bytes: int = Field(
        default=1_048_576,
        gt=0,
        description="Maximum total bytes per session before rotation (default 1MB).",
    )

    session_compaction_threshold: int = Field(
        default=50,
        gt=0,
        description="Turn count that triggers compaction of old turns.",
    )

    session_compaction_keep_recent: int = Field(
        default=10,
        gt=0,
        description="Number of recent turns to retain after compaction.",
    )
