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
"""Data models for session continuity."""

from __future__ import annotations

import dataclasses
import enum
from typing import Any


class TurnRole(enum.StrEnum):
    """Role of a conversation participant."""

    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"
    TOOL = "tool"


@dataclasses.dataclass(frozen=True)
class ConversationTurn:
    """A single turn in a conversation session."""

    role: TurnRole
    content: str
    timestamp: float
    metadata: dict[str, Any] | None = None


@dataclasses.dataclass(frozen=True)
class CompactionResult:
    """Result of a compaction operation."""

    original_turn_count: int
    retained_turn_count: int
    removed_turn_count: int
    summary: str | None = None
