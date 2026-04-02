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

"""MCP tool: upsert_dashboard_native_filters."""

from __future__ import annotations

import logging
from typing import Any

from fastmcp import Context
from superset_core.mcp.decorators import tool, ToolAnnotations

from superset.extensions import event_logger
from superset.mcp_service.auth import has_dataset_access
from superset.mcp_service.chart.chart_utils import chart_filters_to_adhoc_filters
from superset.mcp_service.dashboard.constants import generate_id
from superset.mcp_service.dashboard.schemas import (
    DashboardNativeFilterConfig,
    DashboardNativeFiltersMutationResponse,
    serialize_dashboard_object,
    UpsertDashboardNativeFiltersRequest,
)
from superset.mcp_service.dashboard.utils import (
    load_dashboard_layout,
    load_dashboard_metadata,
    make_dashboard_error,
    map_dashboard_mutation_exception,
    resolve_dashboard,
)

logger = logging.getLogger(__name__)


def _resolve_filter_dataset(dataset_identifier: int | str) -> Any | None:
    """Resolve a dataset by numeric ID or UUID and validate access."""
    from superset.daos.dataset import DatasetDAO

    if isinstance(dataset_identifier, int) or (
        isinstance(dataset_identifier, str) and dataset_identifier.isdigit()
    ):
        dataset = DatasetDAO.find_by_id(int(dataset_identifier))
    else:
        dataset = DatasetDAO.find_by_id(dataset_identifier, id_column="uuid")

    if not dataset or not has_dataset_access(dataset):
        return None
    return dataset


def _build_target(
    dataset: Any, filter_config: DashboardNativeFilterConfig
) -> dict[str, Any]:
    target: dict[str, Any] = {
        "column": {"name": filter_config.target.column},
        "datasetId": dataset.id,
    }
    if getattr(dataset, "uuid", None):
        target["datasetUuid"] = str(dataset.uuid)
    return target


def _validate_filter_scope(
    dashboard: Any,
    filter_config: DashboardNativeFilterConfig,
) -> str | None:
    dashboard_chart_ids = {
        getattr(chart, "id", None) for chart in getattr(dashboard, "slices", []) or []
    }
    requested_chart_ids = set(filter_config.charts_in_scope).union(
        filter_config.excluded_charts
    )
    if not requested_chart_ids.issubset(dashboard_chart_ids):
        missing_chart_ids = sorted(requested_chart_ids.difference(dashboard_chart_ids))
        return (
            "Native filter scope references charts not present on this dashboard: "
            f"{missing_chart_ids}"
        )

    layout = load_dashboard_layout(dashboard)
    for component_id in filter_config.root_path:
        if component_id != "ROOT_ID" and component_id not in layout:
            return (
                f"Native filter root_path references unknown component '{component_id}'"
            )
    for tab_id in filter_config.tabs_in_scope:
        tab_component = layout.get(tab_id)
        if not isinstance(tab_component, dict) or tab_component.get("type") != "TAB":
            return f"tabs_in_scope references non-tab component '{tab_id}'"
    return None


def _build_control_values(
    filter_config: DashboardNativeFilterConfig,
) -> dict[str, Any]:
    control_values: dict[str, Any] = {
        "enableEmptyFilter": filter_config.enable_empty_filter,
    }
    if filter_config.filter_type == "filter_select":
        control_values.update(
            {
                "defaultToFirstItem": filter_config.default_to_first_item,
                "creatable": filter_config.creatable,
                "multiSelect": filter_config.multi_select,
                "inverseSelection": filter_config.inverse_selection,
                "searchAllOptions": filter_config.search_all_options,
                "sortAscending": filter_config.sort_ascending,
            }
        )
    return control_values


def _build_default_data_mask(
    filter_config: DashboardNativeFilterConfig,
) -> dict[str, Any]:
    default_data_mask: dict[str, Any] = {
        "extraFormData": {},
        "filterState": {},
        "ownState": {},
    }
    if filter_config.filter_type == "filter_time":
        if filter_config.default_time_range:
            return {
                "extraFormData": {"time_range": filter_config.default_time_range},
                "filterState": {"value": filter_config.default_time_range},
                "ownState": {},
            }
        return default_data_mask

    if filter_config.default_value is None:
        return default_data_mask

    default_value: Any = filter_config.default_value
    if filter_config.filter_type == "filter_select" and not isinstance(
        default_value, list
    ):
        default_value = [default_value]
    return {
        "extraFormData": {},
        "filterState": {"value": default_value},
        "ownState": {},
    }


def _build_native_filter_config(
    dashboard: Any,
    filter_config: DashboardNativeFilterConfig,
) -> tuple[dict[str, Any] | None, str | None]:
    """Convert the typed native filter config into dashboard metadata."""
    dataset = _resolve_filter_dataset(filter_config.target.dataset_id)
    if dataset is None:
        return None, (
            "Dataset access denied or not found for native filter target "
            f"'{filter_config.target.dataset_id}'"
        )

    if (scope_error := _validate_filter_scope(dashboard, filter_config)) is not None:
        return None, scope_error

    payload: dict[str, Any] = {
        "id": filter_config.id or generate_id("NATIVE_FILTER"),
        "name": filter_config.name,
        "description": filter_config.description,
        "filterType": filter_config.filter_type,
        "targets": [_build_target(dataset, filter_config)],
        "type": "NATIVE_FILTER",
        "chartsInScope": filter_config.charts_in_scope,
        "tabsInScope": filter_config.tabs_in_scope,
        "cascadeParentIds": filter_config.cascade_parent_ids,
        "scope": {
            "excluded": filter_config.excluded_charts,
            "rootPath": filter_config.root_path,
        },
        "controlValues": _build_control_values(filter_config),
        "defaultDataMask": _build_default_data_mask(filter_config),
    }
    if filter_config.sort_metric is not None:
        payload["sortMetric"] = filter_config.sort_metric
    if filter_config.time_range is not None:
        payload["time_range"] = filter_config.time_range
    if filter_config.granularity_sqla is not None:
        payload["granularity_sqla"] = filter_config.granularity_sqla
    if filter_config.adhoc_filters:
        payload["adhoc_filters"] = chart_filters_to_adhoc_filters(
            filter_config.adhoc_filters
        )

    return payload, None


@tool(
    tags=["mutate"],
    class_permission_name="Dashboard",
    annotations=ToolAnnotations(
        title="Upsert dashboard native filters",
        readOnlyHint=False,
        destructiveHint=False,
    ),
)
def upsert_dashboard_native_filters(
    request: UpsertDashboardNativeFiltersRequest,
    ctx: Context,
) -> DashboardNativeFiltersMutationResponse:
    """Create or update dashboard native filters using a typed filter contract."""
    from superset.commands.dashboard.update import UpdateDashboardNativeFiltersCommand

    try:
        with event_logger.log_context(
            action="mcp.upsert_dashboard_native_filters.lookup"
        ):
            dashboard = resolve_dashboard(request.identifier)

        updated_filter_configs: list[dict[str, Any]] = []
        for filter_config in request.filters:
            filter_payload, error = _build_native_filter_config(
                dashboard, filter_config
            )
            if filter_payload is None:
                return DashboardNativeFiltersMutationResponse(
                    dashboard=None,
                    dashboard_url=None,
                    error=make_dashboard_error(
                        error or "Invalid native filter configuration",
                        "ValidationError",
                    ),
                    native_filter_ids=[],
                )
            updated_filter_configs.append(filter_payload)

        existing_native_filters = (
            load_dashboard_metadata(dashboard).get("native_filter_configuration") or []
        )
        existing_filter_ids = [
            native_filter.get("id")
            for native_filter in existing_native_filters
            if isinstance(native_filter, dict) and native_filter.get("id")
        ]
        updated_filter_ids = [
            filter_config["id"] for filter_config in updated_filter_configs
        ]
        deleted_filter_ids = (
            [
                filter_id
                for filter_id in existing_filter_ids
                if filter_id not in updated_filter_ids
            ]
            if request.replace_existing
            else []
        )
        reordered_filter_ids = list(updated_filter_ids)
        if not request.replace_existing:
            reordered_filter_ids.extend(
                filter_id
                for filter_id in existing_filter_ids
                if filter_id not in updated_filter_ids
            )

        with event_logger.log_context(
            action="mcp.upsert_dashboard_native_filters.db_write"
        ):
            configuration = UpdateDashboardNativeFiltersCommand(
                dashboard.id,
                {
                    "modified": updated_filter_configs,
                    "deleted": deleted_filter_ids,
                    "reordered": reordered_filter_ids,
                },
            ).run()

        refreshed_dashboard = resolve_dashboard(dashboard.id)
        dashboard_info = serialize_dashboard_object(refreshed_dashboard)
        native_filter_ids = [
            filter_config.get("id")
            for filter_config in configuration
            if isinstance(filter_config, dict) and filter_config.get("id")
        ]
        return DashboardNativeFiltersMutationResponse(
            dashboard=dashboard_info,
            dashboard_url=dashboard_info.url,
            error=None,
            native_filter_ids=native_filter_ids,
        )
    except Exception as ex:  # noqa: BLE001
        logger.exception(
            "Native filter update failed for dashboard %s",
            request.identifier,
        )
        return DashboardNativeFiltersMutationResponse(
            dashboard=None,
            dashboard_url=None,
            error=map_dashboard_mutation_exception(
                ex,
                identifier=request.identifier,
                operation="upsert dashboard native filters",
            ),
            native_filter_ids=[],
        )
