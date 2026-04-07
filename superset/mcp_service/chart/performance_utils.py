# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Performance helpers shared by MCP chart workflows."""

from __future__ import annotations

from contextlib import contextmanager
from time import perf_counter
from typing import Iterator

from superset.mcp_service.chart.schemas import PerformanceMetadata


@contextmanager
def record_stage(
    stage_durations_ms: dict[str, int],
    stage_name: str,
) -> Iterator[None]:
    """Record elapsed time for a named stage."""
    start_time = perf_counter()
    try:
        yield
    finally:
        stage_durations_ms[stage_name] = elapsed_ms(start_time)


def elapsed_ms(start_time: float) -> int:
    """Convert a ``perf_counter`` start time into elapsed milliseconds."""
    return int((perf_counter() - start_time) * 1000)


def merge_stage_durations(
    *stage_maps: dict[str, int] | None,
) -> dict[str, int]:
    """Merge stage-duration dictionaries while preserving later overrides."""
    merged: dict[str, int] = {}
    for stage_map in stage_maps:
        if stage_map:
            merged.update(stage_map)
    return merged


def build_performance_metadata(
    *,
    total_start_time: float,
    cache_status: str,
    optimization_suggestions: list[str] | None = None,
    stage_durations_ms: dict[str, int] | None = None,
    compile_query_duration_ms: int | None = None,
    estimated_cost: str | None = None,
) -> PerformanceMetadata:
    """Build a standardized performance payload for chart workflows."""
    return PerformanceMetadata(
        query_duration_ms=elapsed_ms(total_start_time),
        estimated_cost=estimated_cost,
        cache_status=cache_status,
        optimization_suggestions=optimization_suggestions or [],
        stage_durations_ms=stage_durations_ms or {},
        compile_query_duration_ms=compile_query_duration_ms,
    )
