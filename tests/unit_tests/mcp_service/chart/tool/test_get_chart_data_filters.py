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

"""Runtime-path tests for MCP chart data filter application."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from flask import Flask

from superset.mcp_service.chart.chart_utils import DatasetValidationResult
from superset.mcp_service.chart.schemas import ChartData, GetChartDataRequest
from superset.mcp_service.chart.tool import get_chart_data as mod


def _make_ctx() -> MagicMock:
    ctx = MagicMock()
    ctx.info = AsyncMock()
    ctx.debug = AsyncMock()
    ctx.warning = AsyncMock()
    ctx.error = AsyncMock()
    ctx.report_progress = AsyncMock()
    return ctx


def _make_event_logger() -> MagicMock:
    event_logger = MagicMock()
    event_logger.log_context.return_value.__enter__ = Mock()
    event_logger.log_context.return_value.__exit__ = Mock(return_value=False)
    return event_logger


def _make_chart(*, query_context: str | None, params: str | None) -> MagicMock:
    chart = MagicMock()
    chart.id = 12
    chart.slice_name = "Session Facts"
    chart.viz_type = "table"
    chart.datasource_id = 7
    chart.datasource_type = "table"
    chart.query_context = query_context
    chart.params = params
    return chart


def _validation_result() -> DatasetValidationResult:
    return DatasetValidationResult(
        is_valid=True,
        dataset_id=7,
        dataset_name="sample_events",
        warnings=[],
        error=None,
    )


def _query_result() -> dict[str, object]:
    return {
        "queries": [
            {
                "data": [{"organization_id": 1, "sessions": 10}],
                "colnames": ["organization_id", "sessions"],
                "rowcount": 1,
                "cache_key": "cache-key",
            }
        ]
    }


@pytest.fixture
def flask_app() -> Flask:
    app = Flask(__name__)
    app.config["ROW_LIMIT"] = 1000
    return app


class TestGetChartDataFilterApplication:
    @pytest.mark.asyncio
    async def test_query_from_form_data_merges_extra_simple_filters_into_query(
        self,
        flask_app: Flask,
    ) -> None:
        request = GetChartDataRequest(
            form_data_key="cache-key",
            extra_form_data={
                "filters": [
                    {
                        "col": "organization_id",
                        "op": "IN",
                        "val": [7],
                    }
                ]
            },
        )
        form_data = {
            "datasource": "7__table",
            "viz_type": "table",
            "metrics": [{"label": "COUNT(*)"}],
            "groupby": ["organization_id"],
        }
        mock_ctx = _make_ctx()
        mock_command = MagicMock()
        mock_command.validate.return_value = None
        mock_command.run.return_value = _query_result()

        with (
            flask_app.app_context(),
            patch(
                "superset.common.query_context_factory.QueryContextFactory"
            ) as factory,
            patch(
                "superset.commands.chart.data.get_data_command.ChartDataCommand",
                return_value=mock_command,
            ),
            patch.object(mod, "event_logger", _make_event_logger()),
        ):
            factory.return_value.create.return_value = MagicMock()
            result = await mod._query_from_form_data(form_data, request, mock_ctx)

        assert isinstance(result, ChartData)
        query = factory.return_value.create.call_args.kwargs["queries"][0]
        assert query["filters"][0]["col"] == "organization_id"
        assert query["filters"][0]["val"] == [7]

    @pytest.mark.asyncio
    async def test_get_chart_data_applies_extra_form_data_to_saved_query_context(
        self,
        flask_app: Flask,
    ) -> None:
        chart = _make_chart(
            query_context='{"queries":[{}],"form_data":{"viz_type":"table"}}',
            params=None,
        )
        mock_ctx = _make_ctx()
        mock_command = MagicMock()
        mock_command.validate.return_value = None
        mock_command.run.return_value = _query_result()

        with (
            flask_app.app_context(),
            patch("superset.daos.chart.ChartDAO.find_by_id", return_value=chart),
            patch.object(
                mod, "validate_chart_dataset", return_value=_validation_result()
            ),
            patch("superset.charts.schemas.ChartDataQueryContextSchema") as schema_cls,
            patch(
                "superset.commands.chart.data.get_data_command.ChartDataCommand",
                return_value=mock_command,
            ),
            patch.object(mod, "event_logger", _make_event_logger()),
        ):
            schema_cls.return_value.load.return_value = MagicMock()
            result = await mod.get_chart_data(
                GetChartDataRequest(
                    identifier=chart.id,
                    extra_form_data={
                        "filters": [{"col": "organization_id", "op": "IN", "val": [7]}]
                    },
                ),
                mock_ctx,
            )

        assert isinstance(result, ChartData)
        query_context_json = schema_cls.return_value.load.call_args.args[0]
        assert query_context_json["queries"][0]["filters"] == [
            {"col": "organization_id", "op": "IN", "val": [7]}
        ]

    @pytest.mark.asyncio
    async def test_get_chart_data_applies_extra_form_data_to_fallback_form_data(
        self,
        flask_app: Flask,
    ) -> None:
        chart = _make_chart(
            query_context=None,
            params='{"viz_type":"table","metrics":[{"label":"COUNT(*)"}],"groupby":["organization_id"]}',
        )
        mock_ctx = _make_ctx()
        mock_command = MagicMock()
        mock_command.validate.return_value = None
        mock_command.run.return_value = _query_result()

        with (
            flask_app.app_context(),
            patch("superset.daos.chart.ChartDAO.find_by_id", return_value=chart),
            patch.object(
                mod, "validate_chart_dataset", return_value=_validation_result()
            ),
            patch(
                "superset.common.query_context_factory.QueryContextFactory"
            ) as factory,
            patch(
                "superset.commands.chart.data.get_data_command.ChartDataCommand",
                return_value=mock_command,
            ),
            patch.object(mod, "event_logger", _make_event_logger()),
        ):
            factory.return_value.create.return_value = MagicMock()
            result = await mod.get_chart_data(
                GetChartDataRequest(
                    identifier=chart.id,
                    extra_form_data={
                        "filters": [{"col": "organization_id", "op": "IN", "val": [9]}]
                    },
                ),
                mock_ctx,
            )

        assert isinstance(result, ChartData)
        query = factory.return_value.create.call_args.kwargs["queries"][0]
        assert query["filters"] == [{"col": "organization_id", "op": "IN", "val": [9]}]

    @pytest.mark.asyncio
    async def test_get_chart_data_applies_extra_form_data_to_cached_unsaved_state(
        self,
        flask_app: Flask,
    ) -> None:
        chart = _make_chart(
            query_context='{"queries":[{}],"form_data":{"viz_type":"table"}}',
            params=None,
        )
        cached_form_data = (
            '{"datasource_id":7,"datasource_type":"table","viz_type":"table",'
            '"metrics":[{"label":"COUNT(*)"}],"groupby":["organization_id"]}'
        )
        mock_ctx = _make_ctx()
        mock_command = MagicMock()
        mock_command.validate.return_value = None
        mock_command.run.return_value = _query_result()

        with (
            flask_app.app_context(),
            patch("superset.daos.chart.ChartDAO.find_by_id", return_value=chart),
            patch.object(
                mod, "validate_chart_dataset", return_value=_validation_result()
            ),
            patch.object(mod, "_get_cached_form_data", return_value=cached_form_data),
            patch(
                "superset.common.query_context_factory.QueryContextFactory"
            ) as factory,
            patch(
                "superset.commands.chart.data.get_data_command.ChartDataCommand",
                return_value=mock_command,
            ),
            patch.object(mod, "event_logger", _make_event_logger()),
        ):
            factory.return_value.create.return_value = MagicMock()
            result = await mod.get_chart_data(
                GetChartDataRequest(
                    identifier=chart.id,
                    form_data_key="cache-key",
                    extra_form_data={
                        "filters": [{"col": "organization_id", "op": "IN", "val": [11]}]
                    },
                ),
                mock_ctx,
            )

        assert isinstance(result, ChartData)
        query = factory.return_value.create.call_args.kwargs["queries"][0]
        assert query["filters"] == [{"col": "organization_id", "op": "IN", "val": [11]}]
