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
"""Registration for Redis orchestration middleware."""

from __future__ import annotations

import logging
import uuid
from collections.abc import AsyncGenerator

import redis.asyncio as aioredis

from nat.builder.builder import Builder
from nat.cli.register_workflow import register_middleware
from nat.data_models.common import get_secret_value

from .orchestration_middleware import RedisOrchestrationMiddleware
from .orchestration_middleware_config import RedisOrchestrationConfig

logger = logging.getLogger(__name__)


@register_middleware(config_type=RedisOrchestrationConfig)
async def redis_orchestration_middleware(
    config: RedisOrchestrationConfig,
    builder: Builder,
) -> AsyncGenerator[RedisOrchestrationMiddleware, None]:
    """Build a Redis orchestration middleware from configuration.

    Establishes the Redis connection, optionally runs crash recovery,
    yields the middleware instance, and cleans up on teardown.
    """
    instance_id = config.instance_id or uuid.uuid4().hex

    connection_kwargs: dict = {
        "decode_responses": True,
        "socket_timeout": 5.0,
        "socket_connect_timeout": 5.0,
    }
    if config.redis_password:
        connection_kwargs["password"] = get_secret_value(config.redis_password)

    client = aioredis.from_url(config.redis_url, **connection_kwargs)

    # Verify connectivity
    await client.ping()
    logger.info("Redis orchestration middleware connected to %s (instance=%s)", config.redis_url, instance_id)

    # Crash recovery — detect orphaned running tasks from a previous process
    if config.crash_recovery_on_startup and config.enable_state_tracking:
        from .crash_recovery import CrashRecovery

        recovery = CrashRecovery(client, config.key_prefix, config.state_ttl, instance_id)
        recovered = await recovery.recover_orphaned_tasks()
        if recovered:
            logger.info("Recovered %d orphaned task(s) on startup", len(recovered))

    middleware = RedisOrchestrationMiddleware(
        config=config,
        builder=builder,
        client=client,
        instance_id=instance_id,
    )

    yield middleware

    # Teardown — close the Redis connection
    await client.close()
    logger.info("Redis orchestration middleware connection closed")
