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

"""Unit tests for dashboard mutation MCP tools."""

from unittest.mock import Mock, patch

import pytest
from fastmcp import Client

from superset.mcp_service.app import mcp
from superset.mcp_service.chart.chart_utils import DatasetValidationResult
from superset.mcp_service.dashboard.schemas import (
    DashboardNativeFilterConfig,
    UpdateDashboardRequest,
)
from superset.utils import json


@pytest.fixture
def mcp_server():
    return mcp


@pytest.fixture(autouse=True)
def mock_auth():
    with patch("superset.mcp_service.auth.get_user_from_request") as mock_get_user:
        mock_user = Mock()
        mock_user.id = 1
        mock_user.username = "admin"
        mock_get_user.return_value = mock_user
        yield mock_get_user


@pytest.fixture(autouse=True)
def mock_chart_access():
    with patch(
        "superset.mcp_service.auth.check_chart_data_access",
        return_value=DatasetValidationResult(
            is_valid=True,
            dataset_id=1,
            dataset_name="test_dataset",
            warnings=[],
            error=None,
        ),
    ):
        yield


def _mock_chart(chart_id: int, slice_name: str = "Chart") -> Mock:
    chart = Mock()
    chart.id = chart_id
    chart.slice_name = f"{slice_name} {chart_id}"
    chart.uuid = f"chart-uuid-{chart_id}"
    chart.tags = []
    chart.owners = []
    chart.viz_type = "table"
    chart.datasource_name = None
    chart.datasource_type = None
    chart.description = None
    chart.cache_timeout = None
    chart.changed_by = None
    chart.changed_by_name = None
    chart.changed_on = None
    chart.changed_on_humanized = None
    chart.created_by = None
    chart.created_by_name = None
    chart.created_on = None
    chart.created_on_humanized = None
    return chart


def _mock_dashboard(dashboard_id: int = 1, title: str = "Dashboard") -> Mock:
    dashboard = Mock()
    dashboard.id = dashboard_id
    dashboard.dashboard_title = title
    dashboard.slug = f"dashboard-{dashboard_id}"
    dashboard.description = "Test dashboard"
    dashboard.published = True
    dashboard.created_on = "2024-01-01"
    dashboard.changed_on = "2024-01-01"
    dashboard.created_by_name = "admin"
    dashboard.changed_by_name = "admin"
    dashboard.uuid = f"dashboard-uuid-{dashboard_id}"
    dashboard.slices = []
    dashboard.owners = []
    dashboard.tags = []
    dashboard.roles = []
    dashboard.json_metadata = "{}"
    dashboard.position_json = "{}"
    return dashboard


class TestUpdateDashboard:
    @patch("superset.commands.dashboard.update.UpdateDashboardCommand")
    @patch("superset.mcp_service.dashboard.tool.update_dashboard.resolve_dashboard")
    @pytest.mark.asyncio
    async def test_update_dashboard_metadata_and_cross_filters(
        self,
        mock_resolve_dashboard,
        mock_update_command,
        mcp_server,
    ) -> None:
        dashboard = _mock_dashboard(5, "Original Dashboard")
        dashboard.json_metadata = json.dumps({"cross_filters_enabled": True})
        updated_dashboard = _mock_dashboard(5, "Renamed Dashboard")
        mock_resolve_dashboard.side_effect = [dashboard, updated_dashboard]
        mock_update_command.return_value.run.return_value = updated_dashboard

        request = {
            "identifier": 5,
            "dashboard_title": "Renamed Dashboard",
            "description": "Updated dashboard description",
            "cross_filters_enabled": False,
        }

        async with Client(mcp_server) as client:
            result = await client.call_tool("update_dashboard", {"request": request})

        assert result.structured_content["error"] is None
        assert (
            result.structured_content["dashboard"]["dashboard_title"]
            == "Renamed Dashboard"
        )
        command_args = mock_update_command.call_args.args
        payload = command_args[1]
        assert payload["dashboard_title"] == "Renamed Dashboard"
        metadata = json.loads(payload["json_metadata"])
        assert metadata["cross_filters_enabled"] is False

    @patch("superset.commands.dashboard.update.UpdateDashboardCommand")
    @patch("superset.mcp_service.dashboard.tool.update_dashboard.resolve_dashboard")
    @patch("superset.db.session.query")
    @pytest.mark.asyncio
    async def test_update_dashboard_rebuilds_layout_from_chart_ids(
        self,
        mock_query,
        mock_resolve_dashboard,
        mock_update_command,
        mcp_server,
    ) -> None:
        dashboard = _mock_dashboard(7, "Layout Dashboard")
        updated_dashboard = _mock_dashboard(7, "Layout Dashboard")
        mock_resolve_dashboard.side_effect = [dashboard, updated_dashboard]
        mock_update_command.return_value.run.return_value = updated_dashboard

        chart_1 = _mock_chart(10)
        chart_2 = _mock_chart(20)
        mock_filter = Mock()
        mock_filter.order_by.return_value = mock_filter
        mock_filter.all.return_value = [chart_1, chart_2]
        mock_query.return_value.filter.return_value = mock_filter

        request = {"identifier": 7, "chart_ids": [10, 20]}

        async with Client(mcp_server) as client:
            result = await client.call_tool("update_dashboard", {"request": request})

        assert result.structured_content["error"] is None
        payload = mock_update_command.call_args.args[1]
        metadata = json.loads(payload["json_metadata"])
        positions = metadata["positions"]
        chart_ids_in_layout = sorted(
            value["meta"]["chartId"]
            for value in positions.values()
            if isinstance(value, dict) and value.get("type") == "CHART"
        )
        assert chart_ids_in_layout == [10, 20]

    @patch("superset.commands.dashboard.update.UpdateDashboardCommand")
    @patch("superset.mcp_service.dashboard.tool.update_dashboard.resolve_dashboard")
    @patch("superset.db.session.query")
    @pytest.mark.asyncio
    async def test_update_dashboard_rebuilds_explicit_layout_rows(
        self,
        mock_query,
        mock_resolve_dashboard,
        mock_update_command,
        mcp_server,
    ) -> None:
        dashboard = _mock_dashboard(8, "Row Layout Dashboard")
        updated_dashboard = _mock_dashboard(8, "Row Layout Dashboard")
        mock_resolve_dashboard.side_effect = [dashboard, updated_dashboard]
        mock_update_command.return_value.run.return_value = updated_dashboard

        chart_10 = _mock_chart(10)
        chart_20 = _mock_chart(20)
        chart_30 = _mock_chart(30)
        mock_filter = Mock()
        mock_filter.order_by.return_value = mock_filter
        mock_filter.all.return_value = [chart_10, chart_20, chart_30]
        mock_query.return_value.filter.return_value = mock_filter

        request = {
            "identifier": 8,
            "layout_rows": [
                {"chart_ids": [10]},
                {"chart_ids": [20, 30]},
            ],
        }

        async with Client(mcp_server) as client:
            result = await client.call_tool("update_dashboard", {"request": request})

        assert result.structured_content["error"] is None
        payload = mock_update_command.call_args.args[1]
        metadata = json.loads(payload["json_metadata"])
        positions = metadata["positions"]
        grid_rows = positions["GRID_ID"]["children"]
        assert len(grid_rows) == 2
        first_row_children = positions[grid_rows[0]]["children"]
        second_row_children = positions[grid_rows[1]]["children"]
        first_row_chart = positions[positions[first_row_children[0]]["children"][0]]
        second_row_chart_ids = [
            positions[positions[column_id]["children"][0]]["meta"]["chartId"]
            for column_id in second_row_children
        ]
        assert first_row_chart["meta"]["chartId"] == 10
        assert second_row_chart_ids == [20, 30]

    @patch("superset.commands.dashboard.update.UpdateDashboardCommand")
    @patch("superset.mcp_service.dashboard.tool.update_dashboard.resolve_dashboard")
    @pytest.mark.asyncio
    async def test_update_dashboard_applies_chart_dimensions(
        self,
        mock_resolve_dashboard,
        mock_update_command,
        mcp_server,
    ) -> None:
        dashboard = _mock_dashboard(12, "Resize Dashboard")
        dashboard.position_json = json.dumps(
            {
                "CHART-10": {
                    "id": "CHART-10",
                    "type": "CHART",
                    "meta": {"chartId": 10, "height": 50, "width": 4},
                    "parents": ["ROOT_ID", "GRID_ID", "ROW-1", "COLUMN-1"],
                    "children": [],
                },
                "COLUMN-1": {
                    "id": "COLUMN-1",
                    "type": "COLUMN",
                    "meta": {"width": 4},
                    "parents": ["ROOT_ID", "GRID_ID", "ROW-1"],
                    "children": ["CHART-10"],
                },
                "ROW-1": {
                    "id": "ROW-1",
                    "type": "ROW",
                    "parents": ["ROOT_ID", "GRID_ID"],
                    "children": ["COLUMN-1"],
                },
                "GRID_ID": {
                    "id": "GRID_ID",
                    "type": "GRID",
                    "parents": ["ROOT_ID"],
                    "children": ["ROW-1"],
                },
                "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
                "DASHBOARD_VERSION_KEY": "v2",
            }
        )
        updated_dashboard = _mock_dashboard(12, "Resize Dashboard")
        mock_resolve_dashboard.side_effect = [dashboard, updated_dashboard]
        mock_update_command.return_value.run.return_value = updated_dashboard

        request = {
            "identifier": 12,
            "chart_dimensions": [{"chart_id": 10, "width": 8, "height": 60}],
        }

        async with Client(mcp_server) as client:
            result = await client.call_tool("update_dashboard", {"request": request})

        assert result.structured_content["error"] is None
        payload = mock_update_command.call_args.args[1]
        metadata = json.loads(payload["json_metadata"])
        positions = metadata["positions"]
        assert positions["CHART-10"]["meta"]["width"] == 8
        assert positions["CHART-10"]["meta"]["height"] == 60
        assert positions["COLUMN-1"]["meta"]["width"] == 8

    @patch("superset.commands.dashboard.update.UpdateDashboardCommand")
    @patch("superset.mcp_service.dashboard.tool.update_dashboard.resolve_dashboard")
    @pytest.mark.asyncio
    async def test_update_dashboard_moves_chart_to_new_row(
        self,
        mock_resolve_dashboard,
        mock_update_command,
        mcp_server,
    ) -> None:
        dashboard = _mock_dashboard(14, "Move Dashboard")
        dashboard.position_json = json.dumps(
            {
                "CHART-10": {
                    "id": "CHART-10",
                    "type": "CHART",
                    "meta": {"chartId": 10, "height": 50, "width": 4},
                    "parents": ["ROOT_ID", "GRID_ID", "ROW-1", "COLUMN-1"],
                    "children": [],
                },
                "CHART-20": {
                    "id": "CHART-20",
                    "type": "CHART",
                    "meta": {"chartId": 20, "height": 50, "width": 4},
                    "parents": ["ROOT_ID", "GRID_ID", "ROW-1", "COLUMN-2"],
                    "children": [],
                },
                "COLUMN-1": {
                    "id": "COLUMN-1",
                    "type": "COLUMN",
                    "meta": {"width": 4},
                    "parents": ["ROOT_ID", "GRID_ID", "ROW-1"],
                    "children": ["CHART-10"],
                },
                "COLUMN-2": {
                    "id": "COLUMN-2",
                    "type": "COLUMN",
                    "meta": {"width": 4},
                    "parents": ["ROOT_ID", "GRID_ID", "ROW-1"],
                    "children": ["CHART-20"],
                },
                "ROW-1": {
                    "id": "ROW-1",
                    "type": "ROW",
                    "parents": ["ROOT_ID", "GRID_ID"],
                    "children": ["COLUMN-1", "COLUMN-2"],
                },
                "GRID_ID": {
                    "id": "GRID_ID",
                    "type": "GRID",
                    "parents": ["ROOT_ID"],
                    "children": ["ROW-1"],
                },
                "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
                "DASHBOARD_VERSION_KEY": "v2",
            }
        )
        updated_dashboard = _mock_dashboard(14, "Move Dashboard")
        mock_resolve_dashboard.side_effect = [dashboard, updated_dashboard]
        mock_update_command.return_value.run.return_value = updated_dashboard

        request = {
            "identifier": 14,
            "chart_moves": [{"chart_id": 20, "row_index": 1, "column_index": 0}],
        }

        async with Client(mcp_server) as client:
            result = await client.call_tool("update_dashboard", {"request": request})

        assert result.structured_content["error"] is None
        payload = mock_update_command.call_args.args[1]
        metadata = json.loads(payload["json_metadata"])
        positions = metadata["positions"]
        grid_rows = positions["GRID_ID"]["children"]
        assert len(grid_rows) == 2
        first_row_chart_ids = [
            positions[positions[column_id]["children"][0]]["meta"]["chartId"]
            for column_id in positions[grid_rows[0]]["children"]
        ]
        second_row_chart_ids = [
            positions[positions[column_id]["children"][0]]["meta"]["chartId"]
            for column_id in positions[grid_rows[1]]["children"]
        ]
        assert first_row_chart_ids == [10]
        assert second_row_chart_ids == [20]

    @patch("superset.commands.dashboard.update.UpdateDashboardCommand")
    @patch("superset.mcp_service.dashboard.tool.update_dashboard.resolve_dashboard")
    @pytest.mark.asyncio
    async def test_update_dashboard_returns_structured_error_on_runtime_failure(
        self,
        mock_resolve_dashboard,
        mock_update_command,
        mcp_server,
    ) -> None:
        dashboard = _mock_dashboard(16, "Broken Dashboard")
        mock_resolve_dashboard.return_value = dashboard
        mock_update_command.return_value.run.side_effect = RuntimeError("db exploded")

        async with Client(mcp_server) as client:
            result = await client.call_tool(
                "update_dashboard",
                {"request": {"identifier": 16, "dashboard_title": "Broken"}},
            )

        assert result.structured_content["dashboard"] is None
        assert result.structured_content["error"]["error_type"] == "InternalError"
        assert "db exploded" in result.structured_content["error"]["error"]


class TestRemoveChartFromDashboard:
    @patch("superset.commands.dashboard.update.UpdateDashboardCommand")
    @patch(
        "superset.mcp_service.dashboard.tool.remove_chart_from_dashboard.resolve_dashboard"
    )
    @pytest.mark.asyncio
    async def test_remove_chart_from_dashboard_updates_layout(
        self,
        mock_resolve_dashboard,
        mock_update_command,
        mcp_server,
    ) -> None:
        dashboard = _mock_dashboard(9, "Remove Chart Dashboard")
        dashboard.slices = [_mock_chart(10), _mock_chart(20)]
        dashboard.position_json = json.dumps(
            {
                "CHART-10": {
                    "id": "CHART-10",
                    "type": "CHART",
                    "meta": {"chartId": 10},
                    "parents": ["ROOT_ID", "GRID_ID", "ROW-1", "COLUMN-1"],
                    "children": [],
                },
                "CHART-20": {
                    "id": "CHART-20",
                    "type": "CHART",
                    "meta": {"chartId": 20},
                    "parents": ["ROOT_ID", "GRID_ID", "ROW-2", "COLUMN-2"],
                    "children": [],
                },
                "COLUMN-1": {
                    "id": "COLUMN-1",
                    "type": "COLUMN",
                    "parents": ["ROOT_ID", "GRID_ID", "ROW-1"],
                    "children": ["CHART-10"],
                },
                "COLUMN-2": {
                    "id": "COLUMN-2",
                    "type": "COLUMN",
                    "parents": ["ROOT_ID", "GRID_ID", "ROW-2"],
                    "children": ["CHART-20"],
                },
                "ROW-1": {
                    "id": "ROW-1",
                    "type": "ROW",
                    "parents": ["ROOT_ID", "GRID_ID"],
                    "children": ["COLUMN-1"],
                },
                "ROW-2": {
                    "id": "ROW-2",
                    "type": "ROW",
                    "parents": ["ROOT_ID", "GRID_ID"],
                    "children": ["COLUMN-2"],
                },
                "GRID_ID": {
                    "id": "GRID_ID",
                    "type": "GRID",
                    "parents": ["ROOT_ID"],
                    "children": ["ROW-1", "ROW-2"],
                },
                "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
                "DASHBOARD_VERSION_KEY": "v2",
            }
        )
        dashboard.json_metadata = json.dumps(
            {"chart_configuration": {"20": {"foo": "bar"}}}
        )
        updated_dashboard = _mock_dashboard(9, "Remove Chart Dashboard")
        updated_dashboard.slices = [_mock_chart(10)]
        mock_resolve_dashboard.side_effect = [dashboard, updated_dashboard]
        mock_update_command.return_value.run.return_value = updated_dashboard

        request = {"identifier": 9, "chart_id": 20}

        async with Client(mcp_server) as client:
            result = await client.call_tool(
                "remove_chart_from_dashboard",
                {"request": request},
            )

        assert result.structured_content["error"] is None
        payload = mock_update_command.call_args.args[1]
        metadata = json.loads(payload["json_metadata"])
        positions = metadata["positions"]
        assert "CHART-20" not in positions
        assert "20" not in metadata["chart_configuration"]

    @patch("superset.commands.dashboard.update.UpdateDashboardCommand")
    @patch(
        "superset.mcp_service.dashboard.tool.remove_chart_from_dashboard.resolve_dashboard"
    )
    @pytest.mark.asyncio
    async def test_remove_chart_from_dashboard_returns_structured_update_error(
        self,
        mock_resolve_dashboard,
        mock_update_command,
        mcp_server,
    ) -> None:
        dashboard = _mock_dashboard(17, "Remove Failure Dashboard")
        dashboard.slices = [_mock_chart(10)]
        mock_resolve_dashboard.return_value = dashboard
        mock_update_command.return_value.run.side_effect = ValueError("bad metadata")

        async with Client(mcp_server) as client:
            result = await client.call_tool(
                "remove_chart_from_dashboard",
                {"request": {"identifier": 17, "chart_id": 10}},
            )

        assert result.structured_content["dashboard"] is None
        assert result.structured_content["error"]["error_type"] == "UpdateFailed"
        assert "bad metadata" in result.structured_content["error"]["error"]


class TestUpsertDashboardNativeFilters:
    @patch("superset.commands.dashboard.update.UpdateDashboardNativeFiltersCommand")
    @patch(
        "superset.mcp_service.dashboard.tool.upsert_dashboard_native_filters.resolve_dashboard"
    )
    @patch(
        "superset.mcp_service.dashboard.tool.upsert_dashboard_native_filters.has_dataset_access",
        return_value=True,
    )
    @patch("superset.daos.dataset.DatasetDAO.find_by_id")
    @pytest.mark.asyncio
    async def test_upsert_dashboard_native_filters(
        self,
        mock_find_dataset,
        mock_has_access,
        mock_resolve_dashboard,
        mock_update_command,
        mcp_server,
    ) -> None:
        dashboard = _mock_dashboard(11, "Filter Dashboard")
        dashboard.json_metadata = json.dumps({"native_filter_configuration": []})
        refreshed_dashboard = _mock_dashboard(11, "Filter Dashboard")
        mock_resolve_dashboard.side_effect = [dashboard, refreshed_dashboard]
        dataset = Mock()
        dataset.id = 33
        dataset.uuid = "dataset-uuid-33"
        mock_find_dataset.return_value = dataset
        mock_update_command.return_value.run.return_value = [
            {"id": "NATIVE_FILTER-123"},
        ]

        request = {
            "identifier": 11,
            "filters": [
                {
                    "name": "Country",
                    "filter_type": "filter_select",
                    "target": {"dataset_id": 33, "column": "country"},
                    "charts_in_scope": [101, 102],
                    "multi_select": True,
                }
            ],
        }

        async with Client(mcp_server) as client:
            result = await client.call_tool(
                "upsert_dashboard_native_filters",
                {"request": request},
            )

        assert result.structured_content["error"] is None
        assert result.structured_content["native_filter_ids"] == ["NATIVE_FILTER-123"]
        payload = mock_update_command.call_args.args[1]
        assert payload["deleted"] == []
        assert payload["reordered"]
        modified_filter = payload["modified"][0]
        assert modified_filter["filterType"] == "filter_select"
        assert modified_filter["targets"][0]["datasetId"] == 33
        assert modified_filter["targets"][0]["datasetUuid"] == "dataset-uuid-33"

    @patch("superset.commands.dashboard.update.UpdateDashboardNativeFiltersCommand")
    @patch(
        "superset.mcp_service.dashboard.tool.upsert_dashboard_native_filters.resolve_dashboard"
    )
    @patch(
        "superset.mcp_service.dashboard.tool.upsert_dashboard_native_filters.has_dataset_access",
        return_value=True,
    )
    @patch("superset.daos.dataset.DatasetDAO.find_by_id")
    @pytest.mark.asyncio
    async def test_upsert_dashboard_native_filters_supports_scope_root_and_defaults(
        self,
        mock_find_dataset,
        mock_has_access,
        mock_resolve_dashboard,
        mock_update_command,
        mcp_server,
    ) -> None:
        dashboard = _mock_dashboard(13, "Scoped Filter Dashboard")
        dashboard.slices = [_mock_chart(101), _mock_chart(102)]
        dashboard.position_json = json.dumps(
            {
                "TAB-main": {
                    "id": "TAB-main",
                    "type": "TAB",
                    "children": [],
                    "parents": ["ROOT_ID"],
                },
                "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["TAB-main"]},
                "DASHBOARD_VERSION_KEY": "v2",
            }
        )
        dashboard.json_metadata = json.dumps({"native_filter_configuration": []})
        refreshed_dashboard = _mock_dashboard(13, "Scoped Filter Dashboard")
        mock_resolve_dashboard.side_effect = [dashboard, refreshed_dashboard]
        dataset = Mock()
        dataset.id = 44
        dataset.uuid = "dataset-uuid-44"
        mock_find_dataset.return_value = dataset
        mock_update_command.return_value.run.return_value = [
            {"id": "NATIVE_FILTER-789"},
        ]

        request = {
            "identifier": 13,
            "filters": [
                {
                    "name": "Created At",
                    "filter_type": "filter_time",
                    "target": {"dataset_id": 44, "column": "created_at"},
                    "charts_in_scope": [101],
                    "tabs_in_scope": ["TAB-main"],
                    "root_path": ["ROOT_ID", "TAB-main"],
                    "default_time_range": "Last quarter",
                }
            ],
        }

        async with Client(mcp_server) as client:
            result = await client.call_tool(
                "upsert_dashboard_native_filters",
                {"request": request},
            )

        assert result.structured_content["error"] is None
        payload = mock_update_command.call_args.args[1]
        modified_filter = payload["modified"][0]
        assert modified_filter["scope"]["rootPath"] == ["ROOT_ID", "TAB-main"]
        assert modified_filter["tabsInScope"] == ["TAB-main"]
        assert modified_filter["defaultDataMask"]["extraFormData"] == {
            "time_range": "Last quarter"
        }
        assert modified_filter["defaultDataMask"]["filterState"] == {
            "value": "Last quarter"
        }

    @patch("superset.commands.dashboard.update.UpdateDashboardNativeFiltersCommand")
    @patch(
        "superset.mcp_service.dashboard.tool.upsert_dashboard_native_filters.resolve_dashboard"
    )
    @patch(
        "superset.mcp_service.dashboard.tool.upsert_dashboard_native_filters.has_dataset_access",
        return_value=True,
    )
    @patch("superset.daos.dataset.DatasetDAO.find_by_id")
    @pytest.mark.asyncio
    async def test_upsert_dashboard_native_filters_supports_prefilters_and_sort_metric(
        self,
        mock_find_dataset,
        mock_has_access,
        mock_resolve_dashboard,
        mock_update_command,
        mcp_server,
    ) -> None:
        dashboard = _mock_dashboard(15, "Advanced Filter Dashboard")
        dashboard.slices = [_mock_chart(101), _mock_chart(102)]
        dashboard.json_metadata = json.dumps({"native_filter_configuration": []})
        refreshed_dashboard = _mock_dashboard(15, "Advanced Filter Dashboard")
        mock_resolve_dashboard.side_effect = [dashboard, refreshed_dashboard]
        dataset = Mock()
        dataset.id = 55
        dataset.uuid = "dataset-uuid-55"
        mock_find_dataset.return_value = dataset
        mock_update_command.return_value.run.return_value = [
            {"id": "NATIVE_FILTER-456"},
        ]

        request = {
            "identifier": 15,
            "filters": [
                {
                    "name": "Country",
                    "filter_type": "filter_select",
                    "target": {"dataset_id": 55, "column": "country"},
                    "charts_in_scope": [101],
                    "creatable": True,
                    "sort_metric": "SUM(revenue)",
                    "time_range": "Last year",
                    "granularity_sqla": "created_at",
                    "adhoc_filters": [
                        {
                            "filter_type": "value_filter",
                            "column": "region",
                            "op": "IN",
                            "value": ["EMEA"],
                        }
                    ],
                }
            ],
        }

        async with Client(mcp_server) as client:
            result = await client.call_tool(
                "upsert_dashboard_native_filters",
                {"request": request},
            )

        assert result.structured_content["error"] is None
        payload = mock_update_command.call_args.args[1]
        modified_filter = payload["modified"][0]
        assert modified_filter["controlValues"]["creatable"] is True
        assert modified_filter["sortMetric"] == "SUM(revenue)"
        assert modified_filter["time_range"] == "Last year"
        assert modified_filter["granularity_sqla"] == "created_at"
        assert modified_filter["adhoc_filters"] == [
            {
                "clause": "WHERE",
                "expressionType": "SIMPLE",
                "subject": "region",
                "operator": "IN",
                "comparator": ["EMEA"],
            }
        ]

    @patch("superset.commands.dashboard.update.UpdateDashboardNativeFiltersCommand")
    @patch(
        "superset.mcp_service.dashboard.tool.upsert_dashboard_native_filters.resolve_dashboard"
    )
    @patch(
        "superset.mcp_service.dashboard.tool.upsert_dashboard_native_filters.has_dataset_access",
        return_value=True,
    )
    @patch("superset.daos.dataset.DatasetDAO.find_by_id")
    @pytest.mark.asyncio
    async def test_upsert_dashboard_native_filters_returns_structured_error(
        self,
        mock_find_dataset,
        mock_has_access,
        mock_resolve_dashboard,
        mock_update_command,
        mcp_server,
    ) -> None:
        dashboard = _mock_dashboard(18, "Broken Filter Dashboard")
        dashboard.slices = [_mock_chart(101)]
        dashboard.json_metadata = json.dumps({"native_filter_configuration": []})
        mock_resolve_dashboard.return_value = dashboard
        dataset = Mock()
        dataset.id = 66
        dataset.uuid = "dataset-uuid-66"
        mock_find_dataset.return_value = dataset
        mock_update_command.return_value.run.side_effect = RuntimeError("write failed")

        request = {
            "identifier": 18,
            "filters": [
                {
                    "name": "Organization",
                    "filter_type": "filter_select",
                    "target": {"dataset_id": 66, "column": "organization_id"},
                    "charts_in_scope": [101],
                }
            ],
        }

        async with Client(mcp_server) as client:
            result = await client.call_tool(
                "upsert_dashboard_native_filters",
                {"request": request},
            )

        assert result.structured_content["dashboard"] is None
        assert result.structured_content["error"]["error_type"] == "InternalError"
        assert "write failed" in result.structured_content["error"]["error"]


class TestDashboardMutationSchemas:
    def test_update_dashboard_request_rejects_chart_moves_with_layout_rebuild(
        self,
    ) -> None:
        with pytest.raises(ValueError, match="chart_moves cannot be combined"):
            UpdateDashboardRequest(
                identifier=1,
                chart_ids=[10],
                chart_moves=[{"chart_id": 10, "row_index": 0, "column_index": 0}],
            )

    def test_dashboard_native_filter_rejects_metric_prefilters(self) -> None:
        with pytest.raises(
            ValueError,
            match="metric_filter is not supported in native filter adhoc_filters",
        ):
            DashboardNativeFilterConfig(
                name="Revenue",
                filter_type="filter_select",
                target={"dataset_id": 1, "column": "country"},
                adhoc_filters=[
                    {
                        "filter_type": "metric_filter",
                        "metric": "SUM(revenue)",
                        "op": ">",
                        "value": 100,
                    }
                ],
            )
