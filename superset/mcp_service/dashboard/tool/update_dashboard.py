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

"""MCP tool: update_dashboard."""

from __future__ import annotations

import logging
from typing import Any

from fastmcp import Context
from superset_core.mcp.decorators import tool, ToolAnnotations

from superset.extensions import event_logger
from superset.mcp_service.dashboard.schemas import (
    DashboardMutationResponse,
    serialize_dashboard_object,
    UpdateDashboardRequest,
)
from superset.mcp_service.dashboard.utils import (
    apply_chart_dimension_updates,
    apply_chart_moves,
    build_dashboard_metadata_payload,
    create_dashboard_layout,
    create_dashboard_layout_from_rows,
    load_dashboard_layout,
    make_dashboard_error,
    map_dashboard_mutation_exception,
    resolve_dashboard,
)
from superset.mcp_service.utils.url_utils import get_superset_base_url
from superset.utils import json

logger = logging.getLogger(__name__)


def _validate_requested_charts(
    chart_ids: list[int],
) -> tuple[list[Any] | None, str | None]:
    """Load chart objects for a layout rebuild and validate access."""
    from superset import db
    from superset.mcp_service.auth import check_chart_data_access
    from superset.models.slice import Slice

    chart_objects = (
        db.session.query(Slice).filter(Slice.id.in_(chart_ids)).order_by(Slice.id).all()
    )
    found_chart_ids = {chart.id for chart in chart_objects}
    missing_chart_ids = [
        chart_id for chart_id in chart_ids if chart_id not in found_chart_ids
    ]
    if missing_chart_ids:
        return None, f"Charts not found: {missing_chart_ids}"

    accessible_chart_map = {chart.id: chart for chart in chart_objects}
    for chart_id in chart_ids:
        chart = accessible_chart_map[chart_id]
        validation = check_chart_data_access(chart)
        if not validation.is_valid:
            return (
                None,
                f"Chart {chart_id} is not accessible: {validation.error}",
            )

    ordered_charts = [accessible_chart_map[chart_id] for chart_id in chart_ids]
    return ordered_charts, None


def _dashboard_url(dashboard_id: int) -> str:
    return f"{get_superset_base_url()}/superset/dashboard/{dashboard_id}/"


def _build_scalar_dashboard_updates(
    request: UpdateDashboardRequest,
) -> dict[str, Any]:
    """Extract scalar dashboard attributes from the typed request."""
    update_data: dict[str, Any] = {}
    for field_name in (
        "dashboard_title",
        "description",
        "slug",
        "css",
        "published",
    ):
        value = getattr(request, field_name)
        if value is not None:
            update_data[field_name] = value
    return update_data


def _build_metadata_update(
    dashboard: Any,
    request: UpdateDashboardRequest,
) -> tuple[dict[str, Any] | None, str | None]:
    """Build the dashboard metadata payload for layout and cross-filter changes."""
    layout, error = _build_requested_layout(dashboard, request)
    if error is not None:
        return None, error

    layout, error = _apply_layout_mutations(dashboard, request, layout)
    if error is not None:
        return None, error

    if request.cross_filters_enabled is None and layout is None:
        return None, None

    metadata_payload = build_dashboard_metadata_payload(
        dashboard,
        positions=layout,
        cross_filters_enabled=request.cross_filters_enabled,
    )
    return metadata_payload, None


def _build_requested_layout(
    dashboard: Any,
    request: UpdateDashboardRequest,
) -> tuple[dict[str, Any] | None, str | None]:
    layout: dict[str, Any] | None = None
    if request.layout_rows is not None:
        requested_chart_ids = [
            chart_id for row in request.layout_rows for chart_id in row.chart_ids
        ]
        chart_objects, error = _validate_requested_charts(requested_chart_ids)
        if chart_objects is None:
            return None, error
        chart_by_id = {chart.id: chart for chart in chart_objects}
        layout = create_dashboard_layout_from_rows(
            [
                [chart_by_id[chart_id] for chart_id in row.chart_ids]
                for row in request.layout_rows
            ]
        )
    elif request.chart_ids is not None:
        chart_objects, error = _validate_requested_charts(request.chart_ids)
        if chart_objects is None:
            return None, error
        layout = create_dashboard_layout(chart_objects)
    elif request.chart_dimensions is not None or request.chart_moves is not None:
        layout = load_dashboard_layout(dashboard)
    return layout, None


def _apply_layout_mutations(
    dashboard: Any,
    request: UpdateDashboardRequest,
    layout: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, str | None]:
    if request.chart_moves is not None:
        if layout is None:
            layout = load_dashboard_layout(dashboard)
        layout, error = apply_chart_moves(layout, request.chart_moves)
        if layout is None:
            return None, error

    if request.chart_dimensions is not None:
        if layout is None:
            layout = load_dashboard_layout(dashboard)
        layout, error = apply_chart_dimension_updates(layout, request.chart_dimensions)
        if layout is None:
            return None, error

    return layout, None


@tool(
    tags=["mutate"],
    class_permission_name="Dashboard",
    annotations=ToolAnnotations(
        title="Update dashboard",
        readOnlyHint=False,
        destructiveHint=False,
    ),
)
def update_dashboard(
    request: UpdateDashboardRequest, ctx: Context
) -> DashboardMutationResponse:
    """Update dashboard metadata and optionally mutate the layout."""
    from superset.commands.dashboard.update import UpdateDashboardCommand

    try:
        with event_logger.log_context(action="mcp.update_dashboard.lookup"):
            dashboard = resolve_dashboard(request.identifier)

        update_data = _build_scalar_dashboard_updates(request)
        metadata_payload, error = _build_metadata_update(dashboard, request)
        if error is not None:
            return DashboardMutationResponse(
                dashboard=None,
                dashboard_url=None,
                error=make_dashboard_error(error, "ValidationError"),
            )

        if metadata_payload is not None:
            update_data["json_metadata"] = json.dumps(metadata_payload)

        if not update_data:
            dashboard_info = serialize_dashboard_object(dashboard)
            return DashboardMutationResponse(
                dashboard=dashboard_info,
                dashboard_url=dashboard_info.url,
                error=None,
            )

        with event_logger.log_context(action="mcp.update_dashboard.db_write"):
            updated_dashboard = UpdateDashboardCommand(dashboard.id, update_data).run()

        refreshed_dashboard = resolve_dashboard(updated_dashboard.id)
        dashboard_info = serialize_dashboard_object(refreshed_dashboard)
        return DashboardMutationResponse(
            dashboard=dashboard_info,
            dashboard_url=dashboard_info.url or _dashboard_url(updated_dashboard.id),
            error=None,
        )
    except Exception as ex:  # noqa: BLE001
        logger.exception("Dashboard update failed for %s", request.identifier)
        return DashboardMutationResponse(
            dashboard=None,
            dashboard_url=None,
            error=map_dashboard_mutation_exception(
                ex,
                identifier=request.identifier,
                operation="update dashboard",
            ),
        )
