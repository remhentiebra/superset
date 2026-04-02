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

"""Shared helpers for MCP dashboard mutation tools."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from superset.mcp_service.dashboard.constants import (
    generate_id,
    GRID_COLUMN_COUNT,
    GRID_DEFAULT_CHART_WIDTH,
)
from superset.mcp_service.dashboard.schemas import DashboardError
from superset.utils import json


def resolve_dashboard(identifier: int | str) -> Any:
    """Resolve a dashboard by numeric ID, UUID, or slug."""
    from superset.daos.dashboard import DashboardDAO

    return DashboardDAO.get_by_id_or_slug(identifier)


def load_dashboard_layout(dashboard: Any) -> dict[str, Any]:
    """Parse dashboard layout JSON safely."""
    try:
        layout = json.loads(getattr(dashboard, "position_json", "") or "{}")
    except (TypeError, json.JSONDecodeError):
        layout = {}
    return layout if isinstance(layout, dict) else {}


def load_dashboard_metadata(dashboard: Any) -> dict[str, Any]:
    """Parse dashboard metadata JSON safely."""
    try:
        metadata = json.loads(getattr(dashboard, "json_metadata", "") or "{}")
    except (TypeError, json.JSONDecodeError):
        metadata = {}
    return metadata if isinstance(metadata, dict) else {}


def build_dashboard_metadata_payload(
    dashboard: Any,
    *,
    positions: dict[str, Any] | None = None,
    cross_filters_enabled: bool | None = None,
) -> dict[str, Any]:
    """Build the metadata payload expected by ``DashboardDAO.set_dash_metadata``."""
    metadata = load_dashboard_metadata(dashboard)
    if positions is not None:
        metadata["positions"] = positions
    if cross_filters_enabled is not None:
        metadata["cross_filters_enabled"] = cross_filters_enabled
    return metadata


def make_dashboard_error(message: str, error_type: str) -> DashboardError:
    """Create a structured dashboard mutation error."""
    return DashboardError.create(error=message, error_type=error_type)


def map_dashboard_mutation_exception(
    exception: Exception,
    *,
    identifier: int | str,
    operation: str,
) -> DashboardError:
    """Convert dashboard command failures into stable MCP error payloads."""
    from sqlalchemy.exc import SQLAlchemyError

    from superset.commands.dashboard.exceptions import (
        DashboardForbiddenError,
        DashboardInvalidError,
        DashboardNativeFiltersUpdateFailedError,
        DashboardNotFoundError,
        DashboardUpdateFailedError,
    )

    if isinstance(exception, DashboardNotFoundError):
        return make_dashboard_error(
            f"Dashboard '{identifier}' not found",
            "NotFound",
        )
    if isinstance(exception, DashboardForbiddenError):
        return make_dashboard_error(
            "You do not have permission to update this dashboard",
            "PermissionDenied",
        )
    if isinstance(exception, DashboardInvalidError):
        return make_dashboard_error(
            str(exception.normalized_messages()),
            "ValidationError",
        )
    if isinstance(
        exception,
        (
            DashboardUpdateFailedError,
            DashboardNativeFiltersUpdateFailedError,
            SQLAlchemyError,
            ValueError,
        ),
    ):
        return make_dashboard_error(
            f"Failed to {operation}: {str(exception)}",
            "UpdateFailed",
        )
    return make_dashboard_error(
        f"Unexpected failure while {operation}: {str(exception)}",
        "InternalError",
    )


def create_dashboard_layout(chart_objects: list[Any]) -> dict[str, Any]:
    """Create the default simple auto-grid dashboard layout."""
    return create_dashboard_layout_from_rows(
        [chart_objects[i : i + 2] for i in range(0, len(chart_objects), 2)]
    )


def create_dashboard_layout_from_rows(chart_rows: list[list[Any]]) -> dict[str, Any]:
    """Create a dashboard layout from explicit chart rows."""
    layout: dict[str, Any] = {}
    chart_height = 50
    row_ids: list[str] = []

    for row_charts in chart_rows:
        if not row_charts:
            continue

        row_id = generate_id("ROW")
        row_ids.append(row_id)
        column_keys: list[str] = []
        col_width = GRID_COLUMN_COUNT // len(row_charts)

        for chart in row_charts:
            chart_key = f"CHART-{chart.id}"
            column_key = generate_id("COLUMN")
            column_keys.append(column_key)

            layout[chart_key] = {
                "children": [],
                "id": chart_key,
                "meta": {
                    "chartId": chart.id,
                    "height": chart_height,
                    "sliceName": chart.slice_name or f"Chart {chart.id}",
                    "uuid": str(chart.uuid) if chart.uuid else f"chart-{chart.id}",
                    "width": GRID_DEFAULT_CHART_WIDTH,
                },
                "parents": ["ROOT_ID", "GRID_ID", row_id, column_key],
                "type": "CHART",
            }
            layout[column_key] = {
                "children": [chart_key],
                "id": column_key,
                "meta": {
                    "background": "BACKGROUND_TRANSPARENT",
                    "width": col_width,
                },
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "type": "COLUMN",
            }

        layout[row_id] = {
            "children": column_keys,
            "id": row_id,
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
            "parents": ["ROOT_ID", "GRID_ID"],
            "type": "ROW",
        }

    layout["GRID_ID"] = {
        "children": row_ids,
        "id": "GRID_ID",
        "parents": ["ROOT_ID"],
        "type": "GRID",
    }
    layout["ROOT_ID"] = {
        "children": ["GRID_ID"],
        "id": "ROOT_ID",
        "type": "ROOT",
    }
    layout["DASHBOARD_VERSION_KEY"] = "v2"
    return layout


def _remove_component(layout: dict[str, Any], component_id: str) -> None:
    """Remove a layout component and collapse empty row/column containers."""
    component = layout.pop(component_id, None)
    if not isinstance(component, dict):
        return

    parents = component.get("parents") or []
    direct_parent_id = parents[-1] if parents else None
    if not isinstance(direct_parent_id, str):
        return

    parent = layout.get(direct_parent_id)
    if not isinstance(parent, dict):
        return

    children = parent.get("children")
    if isinstance(children, list):
        parent["children"] = [child for child in children if child != component_id]

    if not parent.get("children") and parent.get("type") in {"COLUMN", "ROW"}:
        _remove_component(layout, direct_parent_id)


def remove_chart_from_layout(layout: dict[str, Any], chart_id: int) -> bool:
    """Remove a chart from the dashboard layout, preserving higher-order containers."""
    chart_key = None
    for key, value in layout.items():
        if not isinstance(value, dict) or value.get("type") != "CHART":
            continue
        meta = value.get("meta") or {}
        if meta.get("chartId") == chart_id:
            chart_key = key
            break

    if chart_key is None:
        return False

    _remove_component(layout, chart_key)
    return True


def find_chart_component(
    layout: dict[str, Any], chart_id: int
) -> tuple[str, dict[str, Any]] | None:
    """Find a chart component entry in the dashboard layout."""
    for key, value in layout.items():
        if not isinstance(value, dict) or value.get("type") != "CHART":
            continue
        meta = value.get("meta") or {}
        if meta.get("chartId") == chart_id:
            return key, value
    return None


def _find_parent_column(
    layout: dict[str, Any], chart_component: dict[str, Any]
) -> dict[str, Any] | None:
    parents = chart_component.get("parents") or []
    if len(parents) < 4:
        return None
    column = layout.get(parents[-1])
    return column if isinstance(column, dict) else None


def _apply_single_chart_dimension_update(
    layout: dict[str, Any],
    dimension: Any,
) -> str | None:
    found = find_chart_component(layout, dimension.chart_id)
    if found is None:
        return f"Chart {dimension.chart_id} not found in dashboard layout"

    _chart_key, chart_component = found
    meta = chart_component.setdefault("meta", {})
    if dimension.height is not None:
        meta["height"] = dimension.height
    if dimension.width is not None:
        meta["width"] = dimension.width
        parent_column = _find_parent_column(layout, chart_component)
        if parent_column is not None:
            parent_meta = parent_column.setdefault("meta", {})
            parent_meta["width"] = dimension.width

    return None


def _validate_row_widths(layout: dict[str, Any]) -> str | None:
    for value in layout.values():
        if not isinstance(value, dict) or value.get("type") != "ROW":
            continue
        width_total = 0
        for child_id in value.get("children") or []:
            child = layout.get(child_id)
            if not isinstance(child, dict) or child.get("type") != "COLUMN":
                continue
            child_width = (child.get("meta") or {}).get("width", 0)
            if isinstance(child_width, int):
                width_total += child_width
        if width_total > GRID_COLUMN_COUNT:
            return (
                f"Row {value.get('id')} exceeds grid width {GRID_COLUMN_COUNT} "
                "after chart dimension updates"
            )

    return None


def _grid_row_ids(layout: dict[str, Any]) -> list[str]:
    grid = layout.get("GRID_ID")
    if not isinstance(grid, dict):
        return []
    children = grid.get("children") or []
    return [child for child in children if isinstance(child, str)]


def _detach_chart_from_layout(layout: dict[str, Any], chart_key: str) -> str | None:
    chart_component = layout.get(chart_key)
    if not isinstance(chart_component, dict):
        return f"Chart component {chart_key} not found"

    parents = chart_component.get("parents") or []
    if len(parents) < 4:
        return f"Chart component {chart_key} has an invalid layout path"

    row_id = parents[-2]
    column_id = parents[-1]
    row = layout.get(row_id)
    column = layout.get(column_id)
    if not isinstance(row, dict) or not isinstance(column, dict):
        return f"Chart component {chart_key} has an invalid parent container"

    column_children = column.get("children")
    if isinstance(column_children, list):
        column["children"] = [child for child in column_children if child != chart_key]

    if not column.get("children"):
        row_children = row.get("children")
        if isinstance(row_children, list):
            row["children"] = [child for child in row_children if child != column_id]
        layout.pop(column_id, None)

    if not row.get("children"):
        grid = layout.get("GRID_ID")
        if isinstance(grid, dict) and isinstance(grid.get("children"), list):
            grid["children"] = [child for child in grid["children"] if child != row_id]
        layout.pop(row_id, None)

    return None


def _ensure_target_row(
    layout: dict[str, Any],
    row_index: int,
) -> tuple[str | None, str | None]:
    row_ids = _grid_row_ids(layout)
    if row_index > len(row_ids):
        return None, f"row_index {row_index} is out of range for this dashboard layout"

    if row_index == len(row_ids):
        row_id = generate_id("ROW")
        layout[row_id] = {
            "children": [],
            "id": row_id,
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
            "parents": ["ROOT_ID", "GRID_ID"],
            "type": "ROW",
        }
        grid = layout.setdefault(
            "GRID_ID",
            {"children": [], "id": "GRID_ID", "parents": ["ROOT_ID"], "type": "GRID"},
        )
        children = grid.setdefault("children", [])
        if isinstance(children, list):
            children.insert(row_index, row_id)
        return row_id, None

    row_id = row_ids[row_index]
    row = layout.get(row_id)
    if not isinstance(row, dict) or row.get("type") != "ROW":
        return None, f"Target row {row_id} is invalid"
    return row_id, None


def _insert_chart_into_row(
    layout: dict[str, Any],
    chart_key: str,
    row_id: str,
    column_index: int,
    width: int,
) -> str | None:
    chart_component = layout.get(chart_key)
    row = layout.get(row_id)
    if not isinstance(chart_component, dict) or not isinstance(row, dict):
        return f"Unable to insert chart {chart_key} into row {row_id}"

    row_children = row.setdefault("children", [])
    if not isinstance(row_children, list):
        return f"Row {row_id} has an invalid children structure"
    if column_index > len(row_children):
        return (
            f"column_index {column_index} is out of range for row {row_id} "
            f"with {len(row_children)} columns"
        )

    column_id = generate_id("COLUMN")
    row_children.insert(column_index, column_id)
    layout[column_id] = {
        "children": [chart_key],
        "id": column_id,
        "meta": {
            "background": "BACKGROUND_TRANSPARENT",
            "width": width,
        },
        "parents": ["ROOT_ID", "GRID_ID", row_id],
        "type": "COLUMN",
    }
    chart_component["parents"] = ["ROOT_ID", "GRID_ID", row_id, column_id]
    return None


def _apply_single_chart_move(
    layout: dict[str, Any],
    chart_move: Any,
) -> str | None:
    found = find_chart_component(layout, chart_move.chart_id)
    if found is None:
        return f"Chart {chart_move.chart_id} not found in dashboard layout"

    chart_key, chart_component = found
    chart_meta = chart_component.setdefault("meta", {})
    width = chart_meta.get("width")
    if not isinstance(width, int):
        parent_column = _find_parent_column(layout, chart_component)
        parent_meta = parent_column.get("meta", {}) if parent_column else {}
        width = parent_meta.get("width")
    if not isinstance(width, int):
        width = GRID_DEFAULT_CHART_WIDTH

    if (detach_error := _detach_chart_from_layout(layout, chart_key)) is not None:
        return detach_error

    row_id, row_error = _ensure_target_row(layout, chart_move.row_index)
    if row_id is None:
        return row_error

    return _insert_chart_into_row(
        layout,
        chart_key,
        row_id,
        chart_move.column_index,
        width,
    )


def apply_chart_moves(
    layout: dict[str, Any],
    chart_moves: list[Any],
) -> tuple[dict[str, Any] | None, str | None]:
    """Apply typed chart move/reorder actions to a dashboard layout."""
    updated = deepcopy(layout)

    for chart_move in chart_moves:
        error = _apply_single_chart_move(updated, chart_move)
        if error is not None:
            return None, error

    if (width_error := _validate_row_widths(updated)) is not None:
        return None, width_error

    return updated, None


def apply_chart_dimension_updates(
    layout: dict[str, Any],
    chart_dimensions: list[Any],
) -> tuple[dict[str, Any] | None, str | None]:
    """Apply typed chart width/height updates to a dashboard layout."""
    updated = deepcopy(layout)

    for dimension in chart_dimensions:
        error = _apply_single_chart_dimension_update(updated, dimension)
        if error is not None:
            return None, error

    if (width_error := _validate_row_widths(updated)) is not None:
        return None, width_error

    return updated, None


def _prune_chart_configuration(metadata: dict[str, Any], chart_key: str) -> None:
    chart_configuration = metadata.get("chart_configuration")
    if isinstance(chart_configuration, dict):
        chart_configuration.pop(chart_key, None)


def _prune_expanded_slices(metadata: dict[str, Any], chart_key: str) -> None:
    expanded_slices = metadata.get("expanded_slices")
    if isinstance(expanded_slices, dict):
        expanded_slices.pop(chart_key, None)


def _prune_timed_refresh(metadata: dict[str, Any], chart_id: int) -> None:
    timed_refresh = metadata.get("timed_refresh_immune_slices")
    if isinstance(timed_refresh, list):
        metadata["timed_refresh_immune_slices"] = [
            value for value in timed_refresh if value != chart_id
        ]


def _prune_default_filters(metadata: dict[str, Any], chart_key: str) -> None:
    default_filters_raw = metadata.get("default_filters")
    if not isinstance(default_filters_raw, str):
        return
    try:
        default_filters = json.loads(default_filters_raw)
    except (TypeError, json.JSONDecodeError):
        default_filters = {}
    if isinstance(default_filters, dict):
        default_filters.pop(chart_key, None)
        metadata["default_filters"] = json.dumps(default_filters)


def _prune_native_filter_scope(metadata: dict[str, Any], chart_id: int) -> None:
    native_filters = metadata.get("native_filter_configuration")
    if not isinstance(native_filters, list):
        return
    for native_filter in native_filters:
        if not isinstance(native_filter, dict):
            continue
        charts_in_scope = native_filter.get("chartsInScope")
        if isinstance(charts_in_scope, list):
            native_filter["chartsInScope"] = [
                value for value in charts_in_scope if value != chart_id
            ]
        scope = native_filter.get("scope")
        if isinstance(scope, dict) and isinstance(scope.get("excluded"), list):
            scope["excluded"] = [
                value for value in scope["excluded"] if value != chart_id
            ]


def prune_metadata_for_removed_chart(
    metadata: dict[str, Any],
    chart_id: int,
) -> dict[str, Any]:
    """Clean common metadata references after a chart is removed."""
    cleaned = deepcopy(metadata)
    chart_key = str(chart_id)
    _prune_chart_configuration(cleaned, chart_key)
    _prune_expanded_slices(cleaned, chart_key)
    _prune_timed_refresh(cleaned, chart_id)
    _prune_default_filters(cleaned, chart_key)
    _prune_native_filter_scope(cleaned, chart_id)
    return cleaned
