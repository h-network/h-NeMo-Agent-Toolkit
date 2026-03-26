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
"""Session compaction — compress old conversation turns into a summary.

Working memory (Redis, hot) is compacted by replacing the oldest turns
with a single system-role summary turn, keeping recent turns intact.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Awaitable
from collections.abc import Callable
from typing import Any

from .session_models import CompactionResult
from .session_models import ConversationTurn
from .session_models import TurnRole
from .session_store import SessionStore

logger = logging.getLogger(__name__)


class SessionCompactor:
    """Compacts session history by replacing old turns with a summary.

    When the turn count exceeds ``compaction_threshold``, the oldest turns
    are removed and optionally summarized via an LLM.  The most recent
    ``keep_recent`` turns are always preserved.
    """

    def __init__(
        self,
        session_store: SessionStore,
        compaction_threshold: int = 50,
        keep_recent: int = 10,
    ) -> None:
        """Initialize the session compactor.

        Args:
            session_store: Session store to compact.
            compaction_threshold: Turn count that triggers compaction.
            keep_recent: Number of recent turns to preserve.
        """
        self._store = session_store
        self._compaction_threshold = compaction_threshold
        self._keep_recent = keep_recent

    async def maybe_compact(
        self,
        session_id: str,
        summarize_fn: Callable[[str], Awaitable[str]] | None = None,
    ) -> CompactionResult | None:
        """Compact the session if it exceeds the threshold.

        Args:
            session_id: The session to compact.
            summarize_fn: Optional async callable that takes a conversation
                transcript and returns a summary string.  If not provided,
                old turns are dropped without summarization.

        Returns:
            CompactionResult if compaction occurred, None otherwise.
        """
        turns = await self._store.get_turns(session_id)
        if len(turns) < self._compaction_threshold:
            return None

        archive_count = len(turns) - self._keep_recent
        if archive_count <= 0:
            return None

        archive_turns = turns[:archive_count]
        keep_turns = turns[archive_count:]

        summary: str | None = None
        if summarize_fn:
            transcript = self._format_transcript(archive_turns)
            try:
                summary = await summarize_fn(transcript)
            except Exception:
                logger.warning("Summarization failed for session %s; dropping turns without summary", session_id)

        # Clear and rewrite the session
        await self._store.clear_session(session_id)

        # Prepend summary as a system turn if available
        if summary:
            summary_turn = ConversationTurn(
                role=TurnRole.SYSTEM,
                content=f"[Session summary] {summary}",
                timestamp=time.time(),
                metadata={"compacted_turns": archive_count},
            )
            await self._store.append_turn(session_id, summary_turn)

        # Re-append the kept turns
        for turn in keep_turns:
            await self._store.append_turn(session_id, turn)

        result = CompactionResult(
            original_turn_count=len(turns),
            retained_turn_count=self._keep_recent + (1 if summary else 0),
            removed_turn_count=archive_count,
            summary=summary,
        )
        logger.info(
            "Session %s compacted: %d -> %d turns (removed %d, summary=%s)",
            session_id, result.original_turn_count, result.retained_turn_count,
            result.removed_turn_count, "yes" if summary else "no",
        )
        return result

    @staticmethod
    def _format_transcript(turns: list[ConversationTurn]) -> str:
        """Format turns into a human-readable transcript for summarization."""
        lines = []
        for turn in turns:
            role = turn.role.value.capitalize()
            lines.append(f"{role}: {turn.content}")
        return "\n".join(lines)
