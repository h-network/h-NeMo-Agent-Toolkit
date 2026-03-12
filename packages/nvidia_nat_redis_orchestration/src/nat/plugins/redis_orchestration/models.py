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
"""Data models for Redis orchestration middleware."""

from __future__ import annotations

import dataclasses
import enum


class TaskState(enum.StrEnum):
    """Lifecycle states for an orchestrated task execution."""

    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    ABORTED = "aborted"


TERMINAL_STATES: frozenset[TaskState] = frozenset({
    TaskState.COMPLETED,
    TaskState.FAILED,
    TaskState.TIMED_OUT,
    TaskState.ABORTED,
})


@dataclasses.dataclass(frozen=True)
class TaskStateInfo:
    """Snapshot of a task's state stored in Redis."""

    task_id: str
    function_name: str
    state: TaskState
    instance_id: str
    started_at: float
    updated_at: float
    error: str | None = None
