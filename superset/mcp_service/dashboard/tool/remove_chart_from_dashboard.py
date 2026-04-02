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

"""MCP tool: remove_chart_from_dashboard."""

from __future__ import annotations

import logging

from fastmcp import Context
from superset_core.mcp.decorators import tool, ToolAnnotations

from superset.extensions import event_logger
from superset.mcp_service.dashboard.schemas import (
    DashboardMutationResponse,
    RemoveChartFromDashboardRequest,
    serialize_dashboard_object,
)
from superset.mcp_service.dashboard.tool.generate_dashboard import (
    _create_dashboard_layout,
)
from superset.mcp_service.dashboard.utils import (
    load_dashboard_layout,
    load_dashboard_metadata,
    make_dashboard_error,
    map_dashboard_mutation_exception,
    prune_metadata_for_removed_chart,
    remove_chart_from_layout,
    resolve_dashboard,
)
from superset.utils import json

logger = logging.getLogger(__name__)


@tool(
    tags=["mutate"],
    class_permission_name="Dashboard",
    annotations=ToolAnnotations(
        title="Remove chart from dashboard",
        readOnlyHint=False,
        destructiveHint=True,
    ),
)
def remove_chart_from_dashboard(
    request: RemoveChartFromDashboardRequest,
    ctx: Context,
) -> DashboardMutationResponse:
    """Remove a chart from a dashboard and clean common metadata references."""
    from superset.commands.dashboard.update import UpdateDashboardCommand

    try:
        with event_logger.log_context(action="mcp.remove_chart_from_dashboard.lookup"):
            dashboard = resolve_dashboard(request.identifier)

        chart_ids = [chart.id for chart in getattr(dashboard, "slices", [])]
        if request.chart_id not in chart_ids:
            return DashboardMutationResponse(
                dashboard=None,
                dashboard_url=None,
                error=make_dashboard_error(
                    (
                        f"Chart {request.chart_id} is not part of dashboard "
                        f"'{request.identifier}'"
                    ),
                    "ValidationError",
                ),
            )

        layout = load_dashboard_layout(dashboard)
        metadata = prune_metadata_for_removed_chart(
            load_dashboard_metadata(dashboard),
            request.chart_id,
        )

        remaining_charts = [
            chart
            for chart in getattr(dashboard, "slices", [])
            if chart.id != request.chart_id
        ]
        removed_from_layout = remove_chart_from_layout(layout, request.chart_id)
        if not removed_from_layout:
            layout = _create_dashboard_layout(remaining_charts)

        metadata["positions"] = layout

        with event_logger.log_context(
            action="mcp.remove_chart_from_dashboard.db_write"
        ):
            updated_dashboard = UpdateDashboardCommand(
                dashboard.id,
                {"json_metadata": json.dumps(metadata)},
            ).run()

        refreshed_dashboard = resolve_dashboard(updated_dashboard.id)
        dashboard_info = serialize_dashboard_object(refreshed_dashboard)
        return DashboardMutationResponse(
            dashboard=dashboard_info,
            dashboard_url=dashboard_info.url,
            error=None,
        )
    except Exception as ex:  # noqa: BLE001
        logger.exception(
            "Removing chart %s from dashboard %s failed",
            request.chart_id,
            request.identifier,
        )
        return DashboardMutationResponse(
            dashboard=None,
            dashboard_url=None,
            error=map_dashboard_mutation_exception(
                ex,
                identifier=request.identifier,
                operation="remove chart from dashboard",
            ),
        )
