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

"""Unit tests for MCP saved-query discovery and promotion tools."""

import importlib
import sys
import types
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from marshmallow import ValidationError

from superset.commands.dataset.exceptions import (
    DatasetCreateFailedError,
    DatasetInvalidError,
)
from superset.mcp_service.chart.schemas import (
    ColumnRef,
    GenerateChartResponse,
    TableChartConfig,
)
from superset.mcp_service.dataset.schemas import DatasetError, DatasetInfo
from superset.mcp_service.sql_lab.schemas import (
    CreateVirtualDatasetFromSavedQueryRequest,
    GenerateChartFromSavedQueryRequest,
    GenerateChartFromSqlRequest,
    GenerateExploreLinkFromSavedQueryRequest,
    GenerateExploreLinkFromSqlRequest,
    GetSavedQueryRequest,
    ListSavedQueriesRequest,
)


def _force_passthrough_decorators() -> dict[str, types.ModuleType]:
    def _passthrough_tool(func=None, **kwargs):
        if func is not None:
            return func
        return lambda wrapped: wrapped

    mock_decorators = MagicMock()
    mock_decorators.tool = _passthrough_tool
    saved_modules: dict[str, types.ModuleType] = {}

    for key in (
        "superset_core.api",
        "superset_core.api.mcp",
        "superset_core.api.types",
        "superset_core.mcp",
        "superset_core.mcp.decorators",
    ):
        if key in sys.modules:
            saved_modules[key] = sys.modules[key]

    sys.modules["superset_core.api"] = MagicMock()
    sys.modules["superset_core.api.mcp"] = MagicMock()
    sys.modules["superset_core.mcp"] = MagicMock()
    sys.modules["superset_core.mcp.decorators"] = mock_decorators
    sys.modules.setdefault("superset_core.api.types", MagicMock())
    return saved_modules


def _restore_modules(saved_modules: dict[str, types.ModuleType]) -> None:
    for key in list(sys.modules.keys()):
        if key.startswith("superset_core.api") or key.startswith("superset_core.mcp"):
            del sys.modules[key]
        if key.startswith("superset.mcp_service.sql_lab.tool"):
            del sys.modules[key]
    sys.modules.update(saved_modules)


def _import_tool_module(module_name: str):
    saved_modules = _force_passthrough_decorators()
    module = importlib.import_module(module_name)
    return module, saved_modules


def _make_mock_ctx() -> MagicMock:
    ctx = MagicMock()
    ctx.info = AsyncMock()
    ctx.warning = AsyncMock()
    ctx.error = AsyncMock()
    return ctx


def _make_saved_query() -> MagicMock:
    saved_query = MagicMock()
    saved_query.id = 42
    saved_query.label = "Revenue Query"
    saved_query.sql = "SELECT SUM(revenue) FROM sales"
    saved_query.db_id = 5
    saved_query.schema = "example_schema"
    saved_query.catalog = "main"
    saved_query.description = "Revenue rollup"
    saved_query.template_parameters = '{"country":"DE"}'
    saved_query.changed_on = None
    saved_query.created_on = None
    saved_query.database = MagicMock()
    saved_query.database.database_name = "warehouse"
    return saved_query


class TestSavedQuerySchemas:
    def test_list_saved_queries_request_validates_sorting(self) -> None:
        with pytest.raises(ValueError, match="order_column"):
            ListSavedQueriesRequest(order_column="rows")

    def test_list_saved_queries_request_normalizes_direction(self) -> None:
        request = ListSavedQueriesRequest(order_direction="ASC")
        assert request.order_direction == "asc"

    def test_generate_explore_link_from_saved_query_rejects_blank_dataset_name(
        self,
    ) -> None:
        with pytest.raises(ValueError, match="dataset_name"):
            GenerateExploreLinkFromSavedQueryRequest(
                saved_query_id=42,
                dataset_name="   ",
                config=TableChartConfig(
                    chart_type="table",
                    columns=[ColumnRef(name="revenue", aggregate="SUM")],
                ),
            )

    def test_generate_chart_from_sql_strips_required_text(self) -> None:
        request = GenerateChartFromSqlRequest(
            database_id=5,
            sql="  SELECT SUM(revenue) FROM sales  ",
            table_name="  revenue_dataset  ",
            config=TableChartConfig(
                chart_type="table",
                columns=[ColumnRef(name="revenue", aggregate="SUM")],
            ),
        )
        assert request.sql == "SELECT SUM(revenue) FROM sales"
        assert request.table_name == "revenue_dataset"


class TestSavedQueryTools:
    @pytest.mark.anyio
    async def test_list_saved_queries_serializes_results(self) -> None:
        mod, saved_modules = _import_tool_module(
            "superset.mcp_service.sql_lab.tool.list_saved_queries"
        )
        try:
            mock_ctx = _make_mock_ctx()
            saved_query = _make_saved_query()
            event_logger = MagicMock()
            event_logger.log_context.return_value.__enter__ = Mock()
            event_logger.log_context.return_value.__exit__ = Mock(return_value=False)

            with (
                patch.object(mod, "SavedQueryDAO") as dao,
                patch.object(
                    mod,
                    "get_superset_base_url",
                    return_value="http://localhost:8088",
                ),
                patch.object(mod, "event_logger", event_logger),
            ):
                dao.list.return_value = ([saved_query], 1)
                result = await mod.list_saved_queries(
                    ListSavedQueriesRequest(database_id=5, schema="example_schema"),
                    mock_ctx,
                )

            assert result.count == 1
            assert result.saved_queries[0].database_name == "warehouse"
            assert "savedQueryId=42" in result.saved_queries[0].url
            filters = dao.list.call_args.kwargs["column_operators"]
            assert [(item.col, item.value) for item in filters] == [
                ("db_id", 5),
                ("schema", "example_schema"),
            ]
        finally:
            _restore_modules(saved_modules)

    @pytest.mark.anyio
    async def test_get_saved_query_not_found_raises(self) -> None:
        mod, saved_modules = _import_tool_module(
            "superset.mcp_service.sql_lab.tool.get_saved_query"
        )
        try:
            mock_ctx = _make_mock_ctx()
            event_logger = MagicMock()
            event_logger.log_context.return_value.__enter__ = Mock()
            event_logger.log_context.return_value.__exit__ = Mock(return_value=False)

            with (
                patch.object(mod, "SavedQueryDAO") as dao,
                patch.object(mod, "event_logger", event_logger),
            ):
                dao.find_by_id.return_value = None
                from superset.exceptions import SupersetErrorException

                with pytest.raises(SupersetErrorException, match="not found"):
                    await mod.get_saved_query(
                        GetSavedQueryRequest(identifier=42), mock_ctx
                    )
        finally:
            _restore_modules(saved_modules)

    @pytest.mark.anyio
    async def test_create_virtual_dataset_from_saved_query_builds_payload(self) -> None:
        mod, saved_modules = _import_tool_module(
            "superset.mcp_service.sql_lab.tool.create_virtual_dataset_from_saved_query"
        )
        try:
            mock_ctx = _make_mock_ctx()
            saved_query = _make_saved_query()
            command = MagicMock()
            command.run.return_value = MagicMock()
            event_logger = MagicMock()
            event_logger.log_context.return_value.__enter__ = Mock()
            event_logger.log_context.return_value.__exit__ = Mock(return_value=False)
            serialized = DatasetInfo(
                id=9, table_name="revenue_dataset", schema="example_schema"
            )

            with (
                patch.object(mod, "SavedQueryDAO") as dao,
                patch.object(
                    mod, "CreateDatasetCommand", return_value=command
                ) as create_command,
                patch.object(mod, "serialize_dataset", return_value=serialized),
                patch.object(mod, "event_logger", event_logger),
            ):
                dao.find_by_id.return_value = saved_query
                result = await mod.create_virtual_dataset_from_saved_query(
                    CreateVirtualDatasetFromSavedQueryRequest(
                        saved_query_id=42,
                        table_name="revenue_dataset",
                    ),
                    mock_ctx,
                )

            assert result.id == 9
            payload = create_command.call_args.args[0]
            assert payload["database"] == 5
            assert payload["table_name"] == "revenue_dataset"
            assert payload["template_params"] == '{"country":"DE"}'
        finally:
            _restore_modules(saved_modules)

    @pytest.mark.anyio
    async def test_create_virtual_dataset_from_saved_query_returns_dataset_error(
        self,
    ) -> None:
        mod, saved_modules = _import_tool_module(
            "superset.mcp_service.sql_lab.tool.create_virtual_dataset_from_saved_query"
        )
        try:
            mock_ctx = _make_mock_ctx()
            saved_query = _make_saved_query()
            command = MagicMock()
            command.run.side_effect = DatasetInvalidError(
                exceptions=[
                    ValidationError(
                        "Invalid SQL: syntax error",
                        field_name="sql",
                    )
                ]
            )
            event_logger = MagicMock()
            event_logger.log_context.return_value.__enter__ = Mock()
            event_logger.log_context.return_value.__exit__ = Mock(return_value=False)

            with (
                patch.object(mod, "SavedQueryDAO") as dao,
                patch.object(mod, "CreateDatasetCommand", return_value=command),
                patch.object(mod, "event_logger", event_logger),
            ):
                dao.find_by_id.return_value = saved_query
                result = await mod.create_virtual_dataset_from_saved_query(
                    CreateVirtualDatasetFromSavedQueryRequest(saved_query_id=42),
                    mock_ctx,
                )

            assert isinstance(result, DatasetError)
            assert result.error_type == "InvalidSql"
        finally:
            _restore_modules(saved_modules)

    @pytest.mark.anyio
    async def test_generate_chart_from_saved_query_builds_dataset_and_chart(
        self,
    ) -> None:
        mod, saved_modules = _import_tool_module(
            "superset.mcp_service.sql_lab.tool.generate_chart_from_saved_query"
        )
        try:
            mock_ctx = _make_mock_ctx()
            saved_query = _make_saved_query()
            command = MagicMock()
            command.run.return_value = MagicMock(id=9)
            event_logger = MagicMock()
            event_logger.log_context.return_value.__enter__ = Mock()
            event_logger.log_context.return_value.__exit__ = Mock(return_value=False)
            serialized_dataset = DatasetInfo(
                id=9, table_name="revenue_dataset", schema="example_schema"
            )
            chart_response = GenerateChartResponse(success=True, form_data={})

            with (
                patch.object(mod, "SavedQueryDAO") as dao,
                patch.object(
                    mod, "CreateDatasetCommand", return_value=command
                ) as create_command,
                patch.object(mod, "serialize_dataset", return_value=serialized_dataset),
                patch.object(
                    mod, "generate_chart", AsyncMock(return_value=chart_response)
                ) as generate_chart,
                patch.object(mod, "event_logger", event_logger),
            ):
                dao.find_by_id.return_value = saved_query
                result = await mod.generate_chart_from_saved_query(
                    GenerateChartFromSavedQueryRequest(
                        saved_query_id=42,
                        dataset_name="revenue_dataset",
                        config=TableChartConfig(
                            chart_type="table",
                            columns=[ColumnRef(name="revenue", aggregate="SUM")],
                        ),
                        save_chart=True,
                    ),
                    mock_ctx,
                )

            assert result.dataset.id == 9
            assert result.chart_response.success is True
            payload = create_command.call_args.args[0]
            assert payload["database"] == 5
            assert payload["table_name"] == "revenue_dataset"
            chart_request = generate_chart.call_args.args[0]
            assert chart_request.dataset_id == 9
            assert chart_request.save_chart is True
        finally:
            _restore_modules(saved_modules)

    @pytest.mark.anyio
    async def test_generate_chart_from_saved_query_returns_dataset_error(
        self,
    ) -> None:
        mod, saved_modules = _import_tool_module(
            "superset.mcp_service.sql_lab.tool.generate_chart_from_saved_query"
        )
        try:
            mock_ctx = _make_mock_ctx()
            saved_query = _make_saved_query()
            command = MagicMock()
            command.run.side_effect = DatasetCreateFailedError("insert failed")
            event_logger = MagicMock()
            event_logger.log_context.return_value.__enter__ = Mock()
            event_logger.log_context.return_value.__exit__ = Mock(return_value=False)

            with (
                patch.object(mod, "SavedQueryDAO") as dao,
                patch.object(mod, "CreateDatasetCommand", return_value=command),
                patch.object(mod, "event_logger", event_logger),
                patch.object(mod, "generate_chart", AsyncMock()) as generate_chart,
            ):
                dao.find_by_id.return_value = saved_query
                result = await mod.generate_chart_from_saved_query(
                    GenerateChartFromSavedQueryRequest(
                        saved_query_id=42,
                        dataset_name="revenue_dataset",
                        config=TableChartConfig(
                            chart_type="table",
                            columns=[ColumnRef(name="revenue", aggregate="SUM")],
                        ),
                    ),
                    mock_ctx,
                )

            assert result.dataset is None
            assert result.dataset_error is not None
            assert result.dataset_error.error_type == "CreateFailed"
            assert result.chart_response is None
            generate_chart.assert_not_called()
        finally:
            _restore_modules(saved_modules)

    @pytest.mark.anyio
    async def test_generate_explore_link_from_saved_query_builds_dataset_and_link(
        self,
    ) -> None:
        mod, saved_modules = _import_tool_module(
            "superset.mcp_service.sql_lab.tool.generate_explore_link_from_saved_query"
        )
        try:
            mock_ctx = _make_mock_ctx()
            saved_query = _make_saved_query()
            command = MagicMock()
            command.run.return_value = MagicMock(id=9)
            event_logger = MagicMock()
            event_logger.log_context.return_value.__enter__ = Mock()
            event_logger.log_context.return_value.__exit__ = Mock(return_value=False)
            serialized_dataset = DatasetInfo(
                id=9, table_name="revenue_dataset", schema="example_schema"
            )

            with (
                patch.object(mod, "SavedQueryDAO") as dao,
                patch.object(
                    mod, "CreateDatasetCommand", return_value=command
                ) as create_command,
                patch.object(mod, "serialize_dataset", return_value=serialized_dataset),
                patch.object(
                    mod,
                    "generate_explore_link",
                    AsyncMock(
                        return_value={
                            "url": "http://localhost:8088/explore/?form_data_key=abc",
                            "form_data": {"viz_type": "table"},
                            "form_data_key": "abc",
                            "error": None,
                        }
                    ),
                ) as generate_explore_link,
                patch.object(mod, "event_logger", event_logger),
            ):
                dao.find_by_id.return_value = saved_query
                result = await mod.generate_explore_link_from_saved_query(
                    GenerateExploreLinkFromSavedQueryRequest(
                        saved_query_id=42,
                        dataset_name="revenue_dataset",
                        use_cache=False,
                        force_refresh=True,
                        cache_form_data=False,
                        config=TableChartConfig(
                            chart_type="table",
                            columns=[ColumnRef(name="revenue", aggregate="SUM")],
                        ),
                    ),
                    mock_ctx,
                )

            payload = create_command.call_args.args[0]
            assert payload["database"] == 5
            assert payload["table_name"] == "revenue_dataset"

            explore_request = generate_explore_link.call_args.args[0]
            assert explore_request.dataset_id == 9
            assert explore_request.use_cache is False
            assert explore_request.force_refresh is True
            assert explore_request.cache_form_data is False
            assert result.dataset.id == 9
            assert result.explore_response.form_data_key == "abc"
            assert result.explore_response.error is None
        finally:
            _restore_modules(saved_modules)

    @pytest.mark.anyio
    async def test_generate_explore_link_from_saved_query_returns_dataset_error(
        self,
    ) -> None:
        mod, saved_modules = _import_tool_module(
            "superset.mcp_service.sql_lab.tool.generate_explore_link_from_saved_query"
        )
        try:
            mock_ctx = _make_mock_ctx()
            saved_query = _make_saved_query()
            command = MagicMock()
            command.run.side_effect = DatasetCreateFailedError("insert failed")
            event_logger = MagicMock()
            event_logger.log_context.return_value.__enter__ = Mock()
            event_logger.log_context.return_value.__exit__ = Mock(return_value=False)

            with (
                patch.object(mod, "SavedQueryDAO") as dao,
                patch.object(mod, "CreateDatasetCommand", return_value=command),
                patch.object(mod, "event_logger", event_logger),
                patch.object(
                    mod,
                    "generate_explore_link",
                    AsyncMock(),
                ) as generate_explore_link,
            ):
                dao.find_by_id.return_value = saved_query
                result = await mod.generate_explore_link_from_saved_query(
                    GenerateExploreLinkFromSavedQueryRequest(
                        saved_query_id=42,
                        dataset_name="revenue_dataset",
                        config=TableChartConfig(
                            chart_type="table",
                            columns=[ColumnRef(name="revenue", aggregate="SUM")],
                        ),
                    ),
                    mock_ctx,
                )

            assert result.dataset is None
            assert result.dataset_error is not None
            assert result.dataset_error.error_type == "CreateFailed"
            assert result.explore_response is None
            generate_explore_link.assert_not_called()
        finally:
            _restore_modules(saved_modules)

    @pytest.mark.anyio
    async def test_generate_chart_from_sql_builds_dataset_and_chart(self) -> None:
        mod, saved_modules = _import_tool_module(
            "superset.mcp_service.sql_lab.tool.generate_chart_from_sql"
        )
        try:
            mock_ctx = _make_mock_ctx()
            command = MagicMock()
            command.run.return_value = MagicMock(id=9)
            event_logger = MagicMock()
            event_logger.log_context.return_value.__enter__ = Mock()
            event_logger.log_context.return_value.__exit__ = Mock(return_value=False)
            serialized_dataset = DatasetInfo(
                id=9, table_name="revenue_dataset", schema="example_schema"
            )
            chart_response = GenerateChartResponse(
                success=True,
                form_data={},
                performance={
                    "query_duration_ms": 3,
                    "cache_status": "miss",
                    "optimization_suggestions": [],
                },
            )

            with (
                patch.object(
                    mod, "CreateDatasetCommand", return_value=command
                ) as create_command,
                patch.object(mod, "serialize_dataset", return_value=serialized_dataset),
                patch.object(
                    mod, "generate_chart", AsyncMock(return_value=chart_response)
                ) as generate_chart,
                patch.object(mod, "event_logger", event_logger),
            ):
                result = await mod.generate_chart_from_sql(
                    GenerateChartFromSqlRequest(
                        database_id=5,
                        sql="SELECT SUM(revenue) FROM sales",
                        table_name="revenue_dataset",
                        config=TableChartConfig(
                            chart_type="table",
                            columns=[ColumnRef(name="revenue", aggregate="SUM")],
                        ),
                        save_chart=True,
                    ),
                    mock_ctx,
                )

            assert result.dataset.id == 9
            assert result.chart_response.success is True
            payload = create_command.call_args.args[0]
            assert payload["database"] == 5
            assert payload["table_name"] == "revenue_dataset"
            chart_request = generate_chart.call_args.args[0]
            assert chart_request.dataset_id == 9
            assert chart_request.save_chart is True
            assert (
                "dataset_create" in result.chart_response.performance.stage_durations_ms
            )
            assert (
                "chart_generation"
                in result.chart_response.performance.stage_durations_ms
            )
        finally:
            _restore_modules(saved_modules)

    @pytest.mark.anyio
    async def test_generate_chart_from_sql_returns_dataset_error(self) -> None:
        mod, saved_modules = _import_tool_module(
            "superset.mcp_service.sql_lab.tool.generate_chart_from_sql"
        )
        try:
            mock_ctx = _make_mock_ctx()
            command = MagicMock()
            command.run.side_effect = DatasetCreateFailedError("insert failed")
            event_logger = MagicMock()
            event_logger.log_context.return_value.__enter__ = Mock()
            event_logger.log_context.return_value.__exit__ = Mock(return_value=False)

            with (
                patch.object(mod, "CreateDatasetCommand", return_value=command),
                patch.object(mod, "event_logger", event_logger),
                patch.object(mod, "generate_chart", AsyncMock()) as generate_chart,
            ):
                result = await mod.generate_chart_from_sql(
                    GenerateChartFromSqlRequest(
                        database_id=5,
                        sql="SELECT SUM(revenue) FROM sales",
                        table_name="revenue_dataset",
                        config=TableChartConfig(
                            chart_type="table",
                            columns=[ColumnRef(name="revenue", aggregate="SUM")],
                        ),
                    ),
                    mock_ctx,
                )

            assert result.dataset is None
            assert result.dataset_error is not None
            assert result.dataset_error.error_type == "CreateFailed"
            assert result.chart_response is None
            generate_chart.assert_not_called()
        finally:
            _restore_modules(saved_modules)

    @pytest.mark.anyio
    async def test_generate_explore_link_from_sql_builds_dataset_and_link(self) -> None:
        mod, saved_modules = _import_tool_module(
            "superset.mcp_service.sql_lab.tool.generate_explore_link_from_sql"
        )
        try:
            mock_ctx = _make_mock_ctx()
            command = MagicMock()
            command.run.return_value = MagicMock(id=9)
            event_logger = MagicMock()
            event_logger.log_context.return_value.__enter__ = Mock()
            event_logger.log_context.return_value.__exit__ = Mock(return_value=False)
            serialized_dataset = DatasetInfo(
                id=9, table_name="revenue_dataset", schema="example_schema"
            )

            with (
                patch.object(
                    mod, "CreateDatasetCommand", return_value=command
                ) as create_command,
                patch.object(mod, "serialize_dataset", return_value=serialized_dataset),
                patch.object(
                    mod,
                    "generate_explore_link",
                    AsyncMock(
                        return_value={
                            "url": "http://localhost:8088/explore/?form_data_key=abc",
                            "form_data": {"viz_type": "table"},
                            "form_data_key": "abc",
                            "error": None,
                        }
                    ),
                ) as generate_explore_link,
                patch.object(mod, "event_logger", event_logger),
            ):
                result = await mod.generate_explore_link_from_sql(
                    GenerateExploreLinkFromSqlRequest(
                        database_id=5,
                        sql="SELECT SUM(revenue) FROM sales",
                        table_name="revenue_dataset",
                        use_cache=False,
                        force_refresh=True,
                        cache_form_data=False,
                        config=TableChartConfig(
                            chart_type="table",
                            columns=[ColumnRef(name="revenue", aggregate="SUM")],
                        ),
                    ),
                    mock_ctx,
                )

            payload = create_command.call_args.args[0]
            assert payload["database"] == 5
            assert payload["table_name"] == "revenue_dataset"
            explore_request = generate_explore_link.call_args.args[0]
            assert explore_request.dataset_id == 9
            assert explore_request.use_cache is False
            assert explore_request.force_refresh is True
            assert explore_request.cache_form_data is False
            assert result.dataset.id == 9
            assert result.explore_response.form_data_key == "abc"
            assert result.explore_response.error is None
        finally:
            _restore_modules(saved_modules)
