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

"""Unit tests for MCP dataset mutation tools."""

import importlib
import sys
import types
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from marshmallow import ValidationError

from superset.commands.dataset.exceptions import (
    DatasetCreateFailedError,
    DatasetExistsValidationError,
    DatasetInvalidError,
)
from superset.mcp_service.dataset.schemas import (
    CreateDatasetRequest,
    CreateVirtualDatasetRequest,
    DatasetCalculatedColumnMutation,
    DatasetError,
    DatasetInfo,
    DatasetMetricMutation,
    SqlMetricInfo,
    TableColumnInfo,
    UpdateDatasetCalculatedColumnsRequest,
    UpdateDatasetMetadataRequest,
    UpdateDatasetMetricsRequest,
)
from superset.sql.parse import Table


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
        if key.startswith("superset.mcp_service.dataset.tool"):
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


def _make_dataset_with_metrics_and_columns() -> MagicMock:
    dataset = MagicMock()
    dataset.id = 7
    dataset.table_name = "sales_virtual"
    dataset.schema = "example_schema"
    dataset.description = "virtual sales dataset"
    dataset.changed_by_name = "admin"
    dataset.changed_on = None
    dataset.changed_on_humanized = None
    dataset.created_by_name = "admin"
    dataset.created_on = None
    dataset.created_on_humanized = None
    dataset.tags = []
    dataset.owners = []
    dataset.is_virtual = True
    dataset.database_id = 3
    dataset.schema_perm = "[examples].[example_schema]"
    dataset.url = "/tablemodelview/edit/7"
    dataset.database = MagicMock()
    dataset.database.database_name = "examples"
    dataset.sql = "SELECT * FROM source_table"
    dataset.main_dttm_col = None
    dataset.offset = 0
    dataset.cache_timeout = 0
    dataset.params = {}
    dataset.template_params = {}
    dataset.extra = {}
    dataset.uuid = "dataset-uuid"

    metric_revenue = MagicMock()
    metric_revenue.id = 11
    metric_revenue.metric_name = "revenue"
    metric_revenue.expression = "SUM(revenue)"
    metric_revenue.description = "Revenue"
    metric_revenue.extra = None
    metric_revenue.metric_type = None
    metric_revenue.d3format = None
    metric_revenue.verbose_name = None
    metric_revenue.warning_text = None
    metric_revenue.currency = None

    metric_margin = MagicMock()
    metric_margin.id = 12
    metric_margin.metric_name = "margin"
    metric_margin.expression = "SUM(margin)"
    metric_margin.description = "Margin"
    metric_margin.extra = None
    metric_margin.metric_type = None
    metric_margin.d3format = None
    metric_margin.verbose_name = None
    metric_margin.warning_text = None
    metric_margin.currency = None

    physical_column = MagicMock()
    physical_column.id = 21
    physical_column.column_name = "region"
    physical_column.type = "VARCHAR"
    physical_column.advanced_data_type = None
    physical_column.verbose_name = None
    physical_column.description = None
    physical_column.expression = None
    physical_column.extra = None
    physical_column.filterable = True
    physical_column.groupby = True
    physical_column.is_active = True
    physical_column.is_dttm = False
    physical_column.python_date_format = None
    physical_column.datetime_format = None

    calc_column = MagicMock()
    calc_column.id = 22
    calc_column.column_name = "profit"
    calc_column.type = "NUMERIC"
    calc_column.advanced_data_type = None
    calc_column.verbose_name = None
    calc_column.description = "Existing profit column"
    calc_column.expression = "revenue - cost"
    calc_column.extra = None
    calc_column.filterable = True
    calc_column.groupby = True
    calc_column.is_active = True
    calc_column.is_dttm = False
    calc_column.python_date_format = None
    calc_column.datetime_format = None

    dataset.metrics = [metric_revenue, metric_margin]
    dataset.columns = [physical_column, calc_column]
    return dataset


class TestDatasetMutationSchemas:
    def test_create_dataset_request_supports_physical_datasets(self) -> None:
        request = CreateDatasetRequest(
            database_id=1,
            table_name="  my_dataset  ",
            schema="  analytics  ",
        )
        assert request.table_name == "my_dataset"
        assert request.sql is None
        assert request.is_sqllab_view is False

    def test_create_virtual_dataset_request_strips_fields(self) -> None:
        request = CreateVirtualDatasetRequest(
            database_id=1,
            table_name="  my_dataset  ",
            sql="  SELECT 1  ",
        )
        assert request.table_name == "my_dataset"
        assert request.sql == "SELECT 1"

    def test_update_dataset_metrics_request_rejects_overlap(self) -> None:
        with pytest.raises(ValueError, match="cannot target the same metric names"):
            UpdateDatasetMetricsRequest(
                identifier=7,
                metrics=[
                    DatasetMetricMutation(
                        metric_name="revenue",
                        expression="SUM(revenue)",
                    )
                ],
                remove_metrics=["revenue"],
            )

    def test_update_dataset_columns_request_rejects_duplicates(self) -> None:
        with pytest.raises(ValueError, match="duplicate column names"):
            UpdateDatasetCalculatedColumnsRequest(
                identifier=7,
                columns=[
                    DatasetCalculatedColumnMutation(
                        column_name="profit",
                        expression="revenue - cost",
                    ),
                    DatasetCalculatedColumnMutation(
                        column_name="profit",
                        expression="revenue - discount",
                    ),
                ],
            )

    def test_update_dataset_metadata_request_requires_changes(self) -> None:
        with pytest.raises(ValueError, match="Provide at least one metadata field"):
            UpdateDatasetMetadataRequest(identifier=7)

    def test_update_dataset_metadata_request_normalizes_tag_names(self) -> None:
        request = UpdateDatasetMetadataRequest(
            identifier=7,
            tag_names=[" finance ", "priority"],
        )
        assert request.tag_names == ["finance", "priority"]


class TestDatasetMutationTools:
    @pytest.mark.anyio
    async def test_create_virtual_dataset_builds_command_payload(self) -> None:
        mod, saved_modules = _import_tool_module(
            "superset.mcp_service.dataset.tool.create_virtual_dataset"
        )
        try:
            mock_ctx = _make_mock_ctx()
            dataset = _make_dataset_with_metrics_and_columns()
            command = MagicMock()
            command.run.return_value = dataset
            event_logger = MagicMock()
            event_logger.log_context.return_value.__enter__ = Mock()
            event_logger.log_context.return_value.__exit__ = Mock(return_value=False)

            with (
                patch.object(
                    mod, "CreateDatasetCommand", return_value=command
                ) as create_command,
                patch.object(mod, "event_logger", event_logger),
            ):
                result = await mod.create_virtual_dataset(
                    CreateVirtualDatasetRequest(
                        database_id=3,
                        table_name="sales_virtual",
                        sql="SELECT * FROM source_table",
                        schema="example_schema",
                        template_params={"country": "DE"},
                    ),
                    mock_ctx,
                )

            assert result.id == 7
            payload = create_command.call_args.args[0]
            assert payload["database"] == 3
            assert payload["schema"] == "example_schema"
            assert payload["template_params"] == '{"country": "DE"}'
        finally:
            _restore_modules(saved_modules)

    @pytest.mark.anyio
    async def test_create_dataset_builds_command_payload(self) -> None:
        mod, saved_modules = _import_tool_module(
            "superset.mcp_service.dataset.tool.create_dataset"
        )
        try:
            mock_ctx = _make_mock_ctx()
            dataset = _make_dataset_with_metrics_and_columns()
            command = MagicMock()
            command.run.return_value = dataset
            event_logger = MagicMock()
            event_logger.log_context.return_value.__enter__ = Mock()
            event_logger.log_context.return_value.__exit__ = Mock(return_value=False)

            with (
                patch.object(
                    mod, "CreateDatasetCommand", return_value=command
                ) as create_command,
                patch.object(mod, "event_logger", event_logger),
            ):
                result = await mod.create_dataset(
                    CreateDatasetRequest(
                        database_id=3,
                        table_name="sales_physical",
                        schema="example_schema",
                        normalize_columns=True,
                    ),
                    mock_ctx,
                )

            assert result.id == 7
            payload = create_command.call_args.args[0]
            assert payload["database"] == 3
            assert payload["schema"] == "example_schema"
            assert payload["normalize_columns"] is True
            assert payload["is_sqllab_view"] is False
            assert "sql" not in payload
        finally:
            _restore_modules(saved_modules)

    @pytest.mark.anyio
    async def test_create_dataset_returns_structured_error_on_response_failure(
        self,
    ) -> None:
        mod, saved_modules = _import_tool_module(
            "superset.mcp_service.dataset.tool.create_dataset"
        )
        try:
            mock_ctx = _make_mock_ctx()
            dataset = _make_dataset_with_metrics_and_columns()
            command = MagicMock()
            command.run.return_value = dataset
            event_logger = MagicMock()
            event_logger.log_context.return_value.__enter__ = Mock()
            event_logger.log_context.return_value.__exit__ = Mock(return_value=False)
            expected_error = DatasetError.create(
                "response serialization failed",
                "InternalError",
            )

            with (
                patch.object(mod, "CreateDatasetCommand", return_value=command),
                patch.object(mod, "event_logger", event_logger),
                patch.object(
                    mod,
                    "serialize_created_dataset",
                    return_value=expected_error,
                ),
            ):
                result = await mod.create_dataset(
                    CreateDatasetRequest(
                        database_id=3,
                        table_name="sales_physical",
                        schema="example_schema",
                    ),
                    mock_ctx,
                )

            assert result == expected_error
        finally:
            _restore_modules(saved_modules)

    @pytest.mark.anyio
    async def test_create_virtual_dataset_returns_structured_invalid_sql_error(
        self,
    ) -> None:
        mod, saved_modules = _import_tool_module(
            "superset.mcp_service.dataset.tool.create_virtual_dataset"
        )
        try:
            mock_ctx = _make_mock_ctx()
            command = MagicMock()
            command.run.side_effect = DatasetInvalidError(
                exceptions=[
                    ValidationError(
                        "Invalid SQL: failed to parse query",
                        field_name="sql",
                    )
                ]
            )
            event_logger = MagicMock()
            event_logger.log_context.return_value.__enter__ = Mock()
            event_logger.log_context.return_value.__exit__ = Mock(return_value=False)

            with (
                patch.object(mod, "CreateDatasetCommand", return_value=command),
                patch.object(mod, "event_logger", event_logger),
            ):
                result = await mod.create_virtual_dataset(
                    CreateVirtualDatasetRequest(
                        database_id=3,
                        table_name="sales_virtual",
                        sql="SELECT * FROM",
                    ),
                    mock_ctx,
                )

            assert isinstance(result, DatasetError)
            assert result.error_type == "InvalidSql"
            assert "Invalid SQL" in result.error
        finally:
            _restore_modules(saved_modules)

    @pytest.mark.anyio
    async def test_create_virtual_dataset_returns_structured_exists_error(
        self,
    ) -> None:
        mod, saved_modules = _import_tool_module(
            "superset.mcp_service.dataset.tool.create_virtual_dataset"
        )
        try:
            mock_ctx = _make_mock_ctx()
            command = MagicMock()
            command.run.side_effect = DatasetInvalidError(
                exceptions=[
                    DatasetExistsValidationError(
                        Table("sales_virtual", "example_schema", "main")
                    )
                ]
            )
            event_logger = MagicMock()
            event_logger.log_context.return_value.__enter__ = Mock()
            event_logger.log_context.return_value.__exit__ = Mock(return_value=False)

            with (
                patch.object(mod, "CreateDatasetCommand", return_value=command),
                patch.object(mod, "event_logger", event_logger),
            ):
                result = await mod.create_virtual_dataset(
                    CreateVirtualDatasetRequest(
                        database_id=3,
                        table_name="sales_virtual",
                        sql="SELECT * FROM source_table",
                        schema="example_schema",
                        catalog="main",
                    ),
                    mock_ctx,
                )

            assert isinstance(result, DatasetError)
            assert result.error_type == "DatasetExists"
            assert "already exists" in result.error
        finally:
            _restore_modules(saved_modules)

    @pytest.mark.anyio
    async def test_create_virtual_dataset_returns_structured_create_failed_error(
        self,
    ) -> None:
        mod, saved_modules = _import_tool_module(
            "superset.mcp_service.dataset.tool.create_virtual_dataset"
        )
        try:
            mock_ctx = _make_mock_ctx()
            command = MagicMock()
            command.run.side_effect = DatasetCreateFailedError("database write failed")
            event_logger = MagicMock()
            event_logger.log_context.return_value.__enter__ = Mock()
            event_logger.log_context.return_value.__exit__ = Mock(return_value=False)

            with (
                patch.object(mod, "CreateDatasetCommand", return_value=command),
                patch.object(mod, "event_logger", event_logger),
            ):
                result = await mod.create_virtual_dataset(
                    CreateVirtualDatasetRequest(
                        database_id=3,
                        table_name="sales_virtual",
                        sql="SELECT * FROM source_table",
                    ),
                    mock_ctx,
                )

            assert isinstance(result, DatasetError)
            assert result.error_type == "CreateFailed"
            assert "database write failed" in result.error
        finally:
            _restore_modules(saved_modules)

    @pytest.mark.anyio
    async def test_update_dataset_metrics_merges_existing_metrics(self) -> None:
        mod, saved_modules = _import_tool_module(
            "superset.mcp_service.dataset.tool.update_dataset_metrics"
        )
        try:
            mock_ctx = _make_mock_ctx()
            dataset = _make_dataset_with_metrics_and_columns()
            command = MagicMock()
            command.run.return_value = dataset
            event_logger = MagicMock()
            event_logger.log_context.return_value.__enter__ = Mock()
            event_logger.log_context.return_value.__exit__ = Mock(return_value=False)
            serialized = DatasetInfo(
                id=dataset.id,
                table_name=dataset.table_name,
                schema="example_schema",
                metrics=[SqlMetricInfo(metric_name="revenue", expression="SUM(net)")],
            )

            with (
                patch.object(mod, "get_dataset_by_identifier", return_value=dataset),
                patch.object(
                    mod, "UpdateDatasetCommand", return_value=command
                ) as update_command,
                patch.object(mod, "serialize_dataset", return_value=serialized),
                patch.object(mod, "event_logger", event_logger),
            ):
                result = await mod.update_dataset_metrics(
                    UpdateDatasetMetricsRequest(
                        identifier=dataset.id,
                        metrics=[
                            DatasetMetricMutation(
                                metric_name="revenue",
                                expression="SUM(net)",
                            ),
                            DatasetMetricMutation(
                                metric_name="orders",
                                expression="COUNT(*)",
                            ),
                        ],
                        remove_metrics=["margin"],
                    ),
                    mock_ctx,
                )

            assert result.metrics[0].expression == "SUM(net)"
            payload = update_command.call_args.args[1]["metrics"]
            assert payload == [
                {"id": 11, "metric_name": "revenue", "expression": "SUM(net)"},
                {"metric_name": "orders", "expression": "COUNT(*)"},
            ]
        finally:
            _restore_modules(saved_modules)

    @pytest.mark.anyio
    async def test_update_dataset_calculated_columns_preserves_physical_columns(
        self,
    ) -> None:
        mod, saved_modules = _import_tool_module(
            "superset.mcp_service.dataset.tool.update_dataset_calculated_columns"
        )
        try:
            mock_ctx = _make_mock_ctx()
            dataset = _make_dataset_with_metrics_and_columns()
            command = MagicMock()
            command.run.return_value = dataset
            event_logger = MagicMock()
            event_logger.log_context.return_value.__enter__ = Mock()
            event_logger.log_context.return_value.__exit__ = Mock(return_value=False)
            serialized = DatasetInfo(
                id=dataset.id,
                table_name=dataset.table_name,
                schema="example_schema",
                columns=[
                    TableColumnInfo(column_name="region"),
                    TableColumnInfo(column_name="profit"),
                ],
            )

            with (
                patch.object(mod, "get_dataset_by_identifier", return_value=dataset),
                patch.object(
                    mod, "UpdateDatasetCommand", return_value=command
                ) as update_command,
                patch.object(mod, "serialize_dataset", return_value=serialized),
                patch.object(mod, "event_logger", event_logger),
            ):
                result = await mod.update_dataset_calculated_columns(
                    UpdateDatasetCalculatedColumnsRequest(
                        identifier=dataset.id,
                        columns=[
                            DatasetCalculatedColumnMutation(
                                column_name="profit",
                                expression="revenue - tax",
                            )
                        ],
                    ),
                    mock_ctx,
                )

            assert result.columns[1].column_name == "profit"
            payload = update_command.call_args.args[1]["columns"]
            assert payload[0]["column_name"] == "region"
            assert payload[1]["id"] == 22
            assert payload[1]["expression"] == "revenue - tax"
        finally:
            _restore_modules(saved_modules)

    @pytest.mark.anyio
    async def test_update_dataset_metadata_builds_payload_and_syncs_tags(self) -> None:
        mod, saved_modules = _import_tool_module(
            "superset.mcp_service.dataset.tool.update_dataset_metadata"
        )
        try:
            mock_ctx = _make_mock_ctx()
            dataset = _make_dataset_with_metrics_and_columns()
            dataset.tags = []
            command = MagicMock()
            command.run.return_value = dataset
            event_logger = MagicMock()
            event_logger.log_context.return_value.__enter__ = Mock()
            event_logger.log_context.return_value.__exit__ = Mock(return_value=False)
            refreshed_dataset = _make_dataset_with_metrics_and_columns()
            refreshed_dataset.table_name = "sales_renamed"
            refreshed_dataset.description = "updated"
            serialized = DatasetInfo(
                id=refreshed_dataset.id,
                table_name=refreshed_dataset.table_name,
                schema="example_schema",
                description=refreshed_dataset.description,
            )

            with (
                patch.object(mod, "get_dataset_by_identifier", return_value=dataset),
                patch.object(
                    mod, "UpdateDatasetCommand", return_value=command
                ) as update_command,
                patch.object(mod, "sync_dataset_custom_tags") as sync_tags,
                patch.object(
                    mod,
                    "refetch_dataset_for_response",
                    return_value=refreshed_dataset,
                ),
                patch.object(mod, "serialize_dataset", return_value=serialized),
                patch.object(mod, "event_logger", event_logger),
            ):
                result = await mod.update_dataset_metadata(
                    UpdateDatasetMetadataRequest(
                        identifier=dataset.id,
                        table_name="sales_renamed",
                        sql="SELECT * FROM new_source",
                        description="updated",
                        tag_names=["finance", "priority"],
                        template_params={"country": "DE"},
                    ),
                    mock_ctx,
                )

            assert result.table_name == "sales_renamed"
            payload = update_command.call_args.args[1]
            assert payload["table_name"] == "sales_renamed"
            assert payload["sql"] == "SELECT * FROM new_source"
            assert payload["description"] == "updated"
            assert payload["template_params"] == '{"country": "DE"}'
            sync_tags.assert_called_once_with(dataset, ["finance", "priority"])
        finally:
            _restore_modules(saved_modules)

    @pytest.mark.anyio
    async def test_update_dataset_metadata_rejects_virtual_only_fields(
        self,
    ) -> None:
        mod, saved_modules = _import_tool_module(
            "superset.mcp_service.dataset.tool.update_dataset_metadata"
        )
        try:
            mock_ctx = _make_mock_ctx()
            dataset = _make_dataset_with_metrics_and_columns()
            dataset.is_virtual = False

            with (
                patch.object(mod, "get_dataset_by_identifier", return_value=dataset),
                patch.object(mod, "UpdateDatasetCommand") as update_command,
            ):
                with pytest.raises(
                    Exception,
                    match="only supported for virtual datasets",
                ):
                    await mod.update_dataset_metadata(
                        UpdateDatasetMetadataRequest(
                            identifier=dataset.id,
                            sql="SELECT * FROM changed_source",
                        ),
                        mock_ctx,
                    )

            update_command.assert_not_called()
        finally:
            _restore_modules(saved_modules)
