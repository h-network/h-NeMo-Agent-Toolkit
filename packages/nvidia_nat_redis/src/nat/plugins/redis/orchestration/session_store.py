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
"""Redis-backed conversation session store.

Uses Redis Lists for ordered conversation turns with TTL and size-based rotation.
"""

from __future__ import annotations

import json
import logging
import time

import redis.asyncio as aioredis

from .session_models import ConversationTurn
from .session_models import TurnRole

logger = logging.getLogger(__name__)


class SessionStore:
    """Manages conversation history in Redis.

    Each session is stored as a Redis List of JSON-serialized turns.
    Sessions auto-expire via TTL and rotate when they exceed size limits.

    Can be used standalone or wired into the orchestration integration.
    """

    def __init__(
        self,
        client: aioredis.Redis,
        key_prefix: str,
        session_ttl: int,
        max_turns: int,
        max_bytes: int,
    ) -> None:
        """Initialize the session store.

        Args:
            client: Async Redis client.
            key_prefix: Namespace prefix for session keys.
            session_ttl: TTL in seconds for session data.
            max_turns: Maximum turns before rotation.
            max_bytes: Maximum bytes before rotation.
        """
        self._client = client
        self._key_prefix = key_prefix
        self._session_ttl = session_ttl
        self._max_turns = max_turns
        self._max_bytes = max_bytes

    # ---- Key helpers ----

    def turns_key(self, session_id: str) -> str:
        """Redis key for the conversation turns list."""
        return f"{self._key_prefix}:session:{session_id}"

    @staticmethod
    def derive_session_id(
        conversation_id: str | None = None,
        user_id: str | None = None,
    ) -> str | None:
        """Derive a session ID from available context.

        Prefers conversation_id (more specific) over user_id.
        Returns None if neither is available.
        """
        if conversation_id:
            return f"conv:{conversation_id}"
        if user_id:
            return f"user:{user_id}"
        return None

    # ---- Core operations ----

    async def append_turn(self, session_id: str, turn: ConversationTurn) -> None:
        """Append a conversation turn to the session and refresh TTL.

        Automatically triggers rotation if the session exceeds size limits.
        """
        key = self.turns_key(session_id)
        payload = self._serialize_turn(turn)
        await self._client.rpush(key, payload)
        await self._client.expire(key, self._session_ttl)
        await self._rotate_if_needed(session_id)

    async def add_user_turn(self, session_id: str, content: str, **meta: object) -> None:
        """Convenience: append a user turn with current timestamp."""
        turn = ConversationTurn(
            role=TurnRole.USER,
            content=content,
            timestamp=time.time(),
            metadata=meta if meta else None,
        )
        await self.append_turn(session_id, turn)

    async def add_assistant_turn(self, session_id: str, content: str, **meta: object) -> None:
        """Convenience: append an assistant turn with current timestamp."""
        turn = ConversationTurn(
            role=TurnRole.ASSISTANT,
            content=content,
            timestamp=time.time(),
            metadata=meta if meta else None,
        )
        await self.append_turn(session_id, turn)

    async def get_turns(self, session_id: str, limit: int = 0) -> list[ConversationTurn]:
        """Retrieve conversation turns.

        Args:
            session_id: The session identifier.
            limit: Maximum number of recent turns to return. 0 means all.

        Returns:
            List of turns in chronological order (oldest first).
        """
        key = self.turns_key(session_id)
        if limit > 0:
            raw_items = await self._client.lrange(key, -limit, -1)
        else:
            raw_items = await self._client.lrange(key, 0, -1)
        return [self._deserialize_turn(item) for item in raw_items]

    async def get_turn_count(self, session_id: str) -> int:
        """Return the number of turns in the session."""
        return await self._client.llen(self.turns_key(session_id))

    async def clear_session(self, session_id: str) -> None:
        """Delete all data for a session."""
        await self._client.delete(self.turns_key(session_id))

    async def touch_ttl(self, session_id: str) -> None:
        """Refresh the TTL on the session without modifying data."""
        await self._client.expire(self.turns_key(session_id), self._session_ttl)

    async def session_exists(self, session_id: str) -> bool:
        """Check if a session has any stored turns."""
        return await self._client.exists(self.turns_key(session_id)) > 0

    # ---- Rotation ----

    async def _rotate_if_needed(self, session_id: str) -> bool:
        """Trim oldest turns if the session exceeds configured limits.

        Returns True if rotation occurred.
        """
        key = self.turns_key(session_id)
        rotated = False

        # Check turn count
        count = await self._client.llen(key)
        if count > self._max_turns:
            trim_to = count - self._max_turns
            await self._client.ltrim(key, trim_to, -1)
            logger.debug("Session %s rotated: trimmed %d turns (count limit)", session_id, trim_to)
            rotated = True

        # Check byte size
        all_items = await self._client.lrange(key, 0, -1)
        total_bytes = sum(len(item.encode() if isinstance(item, str) else item) for item in all_items)
        if total_bytes > self._max_bytes:
            # Drop oldest turns until under the limit
            cumulative = 0
            drop_count = 0
            for item in all_items:
                item_bytes = len(item.encode() if isinstance(item, str) else item)
                cumulative += item_bytes
                if total_bytes - cumulative <= self._max_bytes:
                    drop_count += 1
                    break
                drop_count += 1
            if drop_count > 0:
                await self._client.ltrim(key, drop_count, -1)
                logger.debug("Session %s rotated: trimmed %d turns (byte limit)", session_id, drop_count)
                rotated = True

        return rotated

    # ---- Serialization ----

    @staticmethod
    def _serialize_turn(turn: ConversationTurn) -> str:
        """Serialize a conversation turn to JSON."""
        data = {
            "role": turn.role.value,
            "content": turn.content,
            "timestamp": turn.timestamp,
        }
        if turn.metadata:
            data["metadata"] = turn.metadata
        return json.dumps(data)

    @staticmethod
    def _deserialize_turn(raw: str) -> ConversationTurn:
        """Deserialize a JSON string to a ConversationTurn."""
        data = json.loads(raw)
        return ConversationTurn(
            role=TurnRole(data["role"]),
            content=data["content"],
            timestamp=data["timestamp"],
            metadata=data.get("metadata"),
        )
