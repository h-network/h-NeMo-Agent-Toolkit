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
"""Exceptions for Redis orchestration integration."""

from __future__ import annotations


class OrchestrationError(Exception):
    """Base error for orchestration failures."""


class TaskAbortedError(OrchestrationError):
    """Raised when a task is cancelled via an external abort signal."""

    def __init__(self, task_id: str, reason: str = "External abort requested"):
        self.task_id = task_id
        self.reason = reason
        super().__init__(f"Task {task_id} aborted: {reason}")
