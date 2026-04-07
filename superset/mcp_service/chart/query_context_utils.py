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

"""Helpers for building QueryContext objects from chart form_data."""

from __future__ import annotations

from typing import Any

from superset.utils import json


def _coerce_columns(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [column for column in value if isinstance(column, str)]
    return []


def _append_unique_column(columns: list[str], column: str) -> None:
    if column not in columns:
        columns.append(column)


def _extract_x_axis_column(x_axis_config: Any) -> str | None:
    if isinstance(x_axis_config, str):
        return x_axis_config
    if isinstance(x_axis_config, dict):
        column_name = x_axis_config.get("column_name")
        if isinstance(column_name, str):
            return column_name
    return None


def build_query_columns(form_data: dict[str, Any]) -> list[str]:
    """Build query columns from form_data while preserving table raw-mode fields."""
    query_mode = form_data.get("query_mode")
    all_columns = _coerce_columns(form_data.get("all_columns") or [])
    raw_columns_field = _coerce_columns(form_data.get("columns") or [])
    if query_mode == "raw" and (all_columns or raw_columns_field):
        return list(all_columns or raw_columns_field)

    columns = _coerce_columns(form_data.get("groupby") or [])

    if not columns:
        for column in raw_columns_field:
            _append_unique_column(columns, column)

    if x_axis_column := _extract_x_axis_column(form_data.get("x_axis")):
        if x_axis_column not in columns:
            columns.insert(0, x_axis_column)

    if not columns and isinstance(form_data.get("granularity_sqla"), str):
        columns = [form_data["granularity_sqla"]]

    return columns


def build_orderby(form_data: dict[str, Any]) -> list[Any]:
    """Translate form_data ordering fields into QueryObject ``orderby`` tuples."""
    if orderby := form_data.get("orderby"):
        return list(orderby)

    raw_order_by_cols = form_data.get("order_by_cols") or []
    if not isinstance(raw_order_by_cols, list):
        return []

    default_order_desc = form_data.get("order_desc", True)
    translated: list[Any] = []
    for entry in raw_order_by_cols:
        parsed: Any = entry
        if isinstance(entry, str):
            try:
                parsed = json.loads(entry)
            except ValueError:
                parsed = entry

        if isinstance(parsed, (list, tuple)) and parsed:
            translated.append(
                (
                    parsed[0],
                    bool(parsed[1]) if len(parsed) > 1 else default_order_desc,
                )
            )
        else:
            translated.append((parsed, default_order_desc))
    return translated


def build_chart_query(
    form_data: dict[str, Any],
    *,
    row_limit: int | None = None,
) -> dict[str, Any]:
    """Build a query object from chart form_data for preview and compile checks."""
    from superset.mcp_service.chart.chart_utils import adhoc_filters_to_query_filters

    metrics = list(form_data.get("metrics") or [])
    if not metrics and form_data.get("metric"):
        metrics = [form_data["metric"]]

    filters = list(form_data.get("filters") or [])
    filters.extend(adhoc_filters_to_query_filters(form_data.get("adhoc_filters", [])))

    query: dict[str, Any] = {
        "filters": filters,
        "columns": build_query_columns(form_data),
        "metrics": metrics,
        "row_limit": (
            row_limit if row_limit is not None else form_data.get("row_limit", 100)
        ),
        "order_desc": form_data.get("order_desc", True),
        "time_range": form_data.get("time_range", "No filter"),
    }

    if orderby := build_orderby(form_data):
        query["orderby"] = orderby
    if granularity := form_data.get("granularity"):
        query["granularity"] = granularity
    elif granularity_sqla := form_data.get("granularity_sqla"):
        query["granularity"] = granularity_sqla
    if post_processing := form_data.get("post_processing"):
        query["post_processing"] = post_processing
    if extras := form_data.get("extras"):
        query["extras"] = extras
    if time_shift := form_data.get("time_shift"):
        query["time_shift"] = time_shift
    if series_limit := form_data.get("series_limit"):
        query["series_limit"] = series_limit
    if series_limit_metric := form_data.get("series_limit_metric"):
        query["series_limit_metric"] = series_limit_metric
    return query


def create_query_context_from_form_data(
    form_data: dict[str, Any],
    dataset_id: int,
    *,
    row_limit: int | None = None,
    force: bool = False,
) -> Any:
    """Create a Superset QueryContext from chart form_data."""
    from superset.common.query_context_factory import QueryContextFactory

    factory = QueryContextFactory()
    return factory.create(
        datasource={"id": dataset_id, "type": "table"},
        queries=[build_chart_query(form_data, row_limit=row_limit)],
        form_data=form_data,
        force=force,
    )
