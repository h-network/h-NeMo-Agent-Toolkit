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
"""Tests for RedisOrchestrationConfig."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from nat.plugins.redis_orchestration.orchestration_middleware_config import RedisOrchestrationConfig


class TestRedisOrchestrationConfig:
    """Validate config parsing and field constraints."""

    def test_minimal_config(self):
        config = RedisOrchestrationConfig(redis_url="redis://localhost:6379")
        assert config.redis_url == "redis://localhost:6379"
        assert config.enable_state_tracking is True
        assert config.enable_abort is True
        assert config.state_ttl == 3600
        assert config.key_prefix == "nat:orch"
        assert config.crash_recovery_on_startup is True
        assert config.instance_id is None

    def test_full_config(self):
        config = RedisOrchestrationConfig(
            redis_url="redis://myhost:6380/2",
            enable_state_tracking=False,
            enable_abort=False,
            state_ttl=7200,
            key_prefix="myapp:orch",
            crash_recovery_on_startup=False,
            instance_id="node-1",
        )
        assert config.redis_url == "redis://myhost:6380/2"
        assert config.enable_state_tracking is False
        assert config.enable_abort is False
        assert config.state_ttl == 7200
        assert config.key_prefix == "myapp:orch"
        assert config.crash_recovery_on_startup is False
        assert config.instance_id == "node-1"

    def test_state_ttl_must_be_positive(self):
        with pytest.raises(ValidationError, match="state_ttl"):
            RedisOrchestrationConfig(redis_url="redis://localhost:6379", state_ttl=0)

    def test_state_ttl_rejects_negative(self):
        with pytest.raises(ValidationError, match="state_ttl"):
            RedisOrchestrationConfig(redis_url="redis://localhost:6379", state_ttl=-1)

    def test_type_discriminator(self):
        config = RedisOrchestrationConfig(redis_url="redis://localhost:6379")
        assert config.type == "redis_orchestration"
