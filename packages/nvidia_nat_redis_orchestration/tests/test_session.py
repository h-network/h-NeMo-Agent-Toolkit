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
"""Tests for session continuity — SessionStore, SessionCompactor, and models."""

from __future__ import annotations

import json
import time
from unittest.mock import AsyncMock

import pytest

from nat.plugins.redis_orchestration.session_compactor import SessionCompactor
from nat.plugins.redis_orchestration.session_models import CompactionResult
from nat.plugins.redis_orchestration.session_models import ConversationTurn
from nat.plugins.redis_orchestration.session_models import TurnRole
from nat.plugins.redis_orchestration.session_store import SessionStore


# ==================== Models ====================


class TestSessionModels:

    def test_turn_role_values(self):
        assert TurnRole.USER == "user"
        assert TurnRole.ASSISTANT == "assistant"
        assert TurnRole.SYSTEM == "system"
        assert TurnRole.TOOL == "tool"

    def test_conversation_turn_frozen(self):
        turn = ConversationTurn(role=TurnRole.USER, content="hello", timestamp=1.0)
        with pytest.raises(AttributeError):
            turn.content = "changed"

    def test_conversation_turn_optional_metadata(self):
        turn = ConversationTurn(role=TurnRole.USER, content="hi", timestamp=1.0)
        assert turn.metadata is None
        turn2 = ConversationTurn(role=TurnRole.USER, content="hi", timestamp=1.0, metadata={"k": "v"})
        assert turn2.metadata == {"k": "v"}


# ==================== SessionStore Key Helpers ====================


class TestSessionStoreKeys:

    def test_turns_key(self):
        m = AsyncMock()
        store = SessionStore(m, "pfx", 3600, 100, 1024)
        assert store.turns_key("sess1") == "pfx:session:sess1"

    def test_derive_session_id_conversation(self):
        assert SessionStore.derive_session_id(conversation_id="c1") == "conv:c1"

    def test_derive_session_id_user(self):
        assert SessionStore.derive_session_id(user_id="u1") == "user:u1"

    def test_derive_session_id_prefers_conversation(self):
        assert SessionStore.derive_session_id(conversation_id="c1", user_id="u1") == "conv:c1"

    def test_derive_session_id_none(self):
        assert SessionStore.derive_session_id() is None


# ==================== SessionStore Operations (unit) ====================


@pytest.fixture(name="mock_redis")
def fixture_mock_redis():
    m = AsyncMock()
    m.rpush = AsyncMock()
    m.expire = AsyncMock()
    m.lrange = AsyncMock(return_value=[])
    m.llen = AsyncMock(return_value=0)
    m.ltrim = AsyncMock()
    m.delete = AsyncMock()
    m.exists = AsyncMock(return_value=0)
    return m


@pytest.fixture(name="store")
def fixture_store(mock_redis):
    return SessionStore(mock_redis, "test", session_ttl=3600, max_turns=100, max_bytes=1_000_000)


class TestSessionStoreAppend:

    async def test_append_turn(self, store, mock_redis):
        turn = ConversationTurn(role=TurnRole.USER, content="hello", timestamp=1000.0)
        await store.append_turn("s1", turn)

        mock_redis.rpush.assert_called_once()
        key, payload = mock_redis.rpush.call_args[0]
        assert key == "test:session:s1"
        data = json.loads(payload)
        assert data["role"] == "user"
        assert data["content"] == "hello"
        mock_redis.expire.assert_called_once_with("test:session:s1", 3600)

    async def test_add_user_turn(self, store, mock_redis):
        await store.add_user_turn("s1", "hi there")
        data = json.loads(mock_redis.rpush.call_args[0][1])
        assert data["role"] == "user"
        assert data["content"] == "hi there"

    async def test_add_assistant_turn(self, store, mock_redis):
        await store.add_assistant_turn("s1", "hello!")
        data = json.loads(mock_redis.rpush.call_args[0][1])
        assert data["role"] == "assistant"
        assert data["content"] == "hello!"


class TestSessionStoreGet:

    async def test_get_turns_empty(self, store, mock_redis):
        mock_redis.lrange = AsyncMock(return_value=[])
        turns = await store.get_turns("s1")
        assert turns == []

    async def test_get_turns_deserializes(self, store, mock_redis):
        raw = [
            json.dumps({"role": "user", "content": "hi", "timestamp": 1.0}),
            json.dumps({"role": "assistant", "content": "hello", "timestamp": 2.0}),
        ]
        mock_redis.lrange = AsyncMock(return_value=raw)
        turns = await store.get_turns("s1")
        assert len(turns) == 2
        assert turns[0].role == TurnRole.USER
        assert turns[1].role == TurnRole.ASSISTANT

    async def test_get_turns_with_limit(self, store, mock_redis):
        mock_redis.lrange = AsyncMock(return_value=[])
        await store.get_turns("s1", limit=5)
        mock_redis.lrange.assert_called_once_with("test:session:s1", -5, -1)

    async def test_get_turns_no_limit(self, store, mock_redis):
        mock_redis.lrange = AsyncMock(return_value=[])
        await store.get_turns("s1", limit=0)
        mock_redis.lrange.assert_called_once_with("test:session:s1", 0, -1)


class TestSessionStoreRotation:

    async def test_rotation_by_turn_count(self, mock_redis):
        store = SessionStore(mock_redis, "test", session_ttl=3600, max_turns=5, max_bytes=1_000_000)
        mock_redis.llen = AsyncMock(return_value=8)
        mock_redis.lrange = AsyncMock(return_value=["x"] * 5)  # After trim
        rotated = await store._rotate_if_needed("s1")
        assert rotated is True
        mock_redis.ltrim.assert_called_once_with("test:session:s1", 3, -1)

    async def test_no_rotation_under_limits(self, store, mock_redis):
        mock_redis.llen = AsyncMock(return_value=5)
        mock_redis.lrange = AsyncMock(return_value=["x"] * 5)
        rotated = await store._rotate_if_needed("s1")
        assert rotated is False
        mock_redis.ltrim.assert_not_called()


class TestSessionStoreOther:

    async def test_clear_session(self, store, mock_redis):
        await store.clear_session("s1")
        mock_redis.delete.assert_called_once_with("test:session:s1")

    async def test_session_exists_true(self, store, mock_redis):
        mock_redis.exists = AsyncMock(return_value=1)
        assert await store.session_exists("s1") is True

    async def test_session_exists_false(self, store, mock_redis):
        mock_redis.exists = AsyncMock(return_value=0)
        assert await store.session_exists("s1") is False

    async def test_touch_ttl(self, store, mock_redis):
        await store.touch_ttl("s1")
        mock_redis.expire.assert_called_once_with("test:session:s1", 3600)


# ==================== SessionCompactor ====================


class TestSessionCompactor:

    async def test_no_compaction_under_threshold(self, store, mock_redis):
        mock_redis.lrange = AsyncMock(return_value=[
            json.dumps({"role": "user", "content": f"msg{i}", "timestamp": float(i)})
            for i in range(5)
        ])
        compactor = SessionCompactor(store, compaction_threshold=50, keep_recent=10)
        result = await compactor.maybe_compact("s1")
        assert result is None

    async def test_compaction_drops_old_turns(self, mock_redis):
        store = SessionStore(mock_redis, "test", session_ttl=3600, max_turns=1000, max_bytes=10_000_000)
        turns_raw = [
            json.dumps({"role": "user" if i % 2 == 0 else "assistant", "content": f"msg{i}", "timestamp": float(i)})
            for i in range(20)
        ]
        mock_redis.lrange = AsyncMock(return_value=turns_raw)
        mock_redis.llen = AsyncMock(return_value=0)

        compactor = SessionCompactor(store, compaction_threshold=15, keep_recent=5)
        result = await compactor.maybe_compact("s1")

        assert result is not None
        assert result.original_turn_count == 20
        assert result.removed_turn_count == 15
        assert result.retained_turn_count == 5
        assert result.summary is None
        mock_redis.delete.assert_called_once()  # clear_session

    async def test_compaction_with_summary(self, mock_redis):
        store = SessionStore(mock_redis, "test", session_ttl=3600, max_turns=1000, max_bytes=10_000_000)
        turns_raw = [
            json.dumps({"role": "user" if i % 2 == 0 else "assistant", "content": f"msg{i}", "timestamp": float(i)})
            for i in range(20)
        ]
        mock_redis.lrange = AsyncMock(return_value=turns_raw)
        mock_redis.llen = AsyncMock(return_value=0)

        async def fake_summarize(transcript):
            return "This is a summary of the conversation."

        compactor = SessionCompactor(store, compaction_threshold=15, keep_recent=5)
        result = await compactor.maybe_compact("s1", summarize_fn=fake_summarize)

        assert result is not None
        assert result.summary == "This is a summary of the conversation."
        assert result.retained_turn_count == 6  # 5 kept + 1 summary

    async def test_compaction_summary_failure_still_compacts(self, mock_redis):
        store = SessionStore(mock_redis, "test", session_ttl=3600, max_turns=1000, max_bytes=10_000_000)
        turns_raw = [
            json.dumps({"role": "user", "content": f"msg{i}", "timestamp": float(i)})
            for i in range(20)
        ]
        mock_redis.lrange = AsyncMock(return_value=turns_raw)
        mock_redis.llen = AsyncMock(return_value=0)

        async def failing_summarize(transcript):
            raise RuntimeError("LLM down")

        compactor = SessionCompactor(store, compaction_threshold=15, keep_recent=5)
        result = await compactor.maybe_compact("s1", summarize_fn=failing_summarize)

        assert result is not None
        assert result.summary is None
        assert result.removed_turn_count == 15

    def test_format_transcript(self):
        turns = [
            ConversationTurn(role=TurnRole.USER, content="hello", timestamp=1.0),
            ConversationTurn(role=TurnRole.ASSISTANT, content="hi there", timestamp=2.0),
        ]
        transcript = SessionCompactor._format_transcript(turns)
        assert "User: hello" in transcript
        assert "Assistant: hi there" in transcript
