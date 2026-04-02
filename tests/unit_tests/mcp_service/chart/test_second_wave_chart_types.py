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

"""Unit tests for the second typed MCP chart batch."""

from superset.mcp_service.chart.chart_utils import (
    generate_chart_name,
    map_box_plot_config,
    map_bubble_config,
    map_config_to_form_data,
    map_sankey_config,
    map_sunburst_config,
    map_treemap_config,
    map_word_cloud_config,
    map_world_map_config,
)
from superset.mcp_service.chart.schemas import (
    BoxPlotChartConfig,
    BubbleChartConfig,
    ColumnRef,
    SankeyChartConfig,
    SunburstChartConfig,
    TreemapChartConfig,
    WordCloudChartConfig,
    WorldMapChartConfig,
)
from superset.mcp_service.chart.validation.schema_validator import SchemaValidator


class TestSecondWaveChartSchemas:
    """Schema-level tests for second-wave chart configs."""

    def test_treemap_config_accepts_columns_alias(self) -> None:
        config = TreemapChartConfig(
            chart_type="treemap",
            columns=[
                ColumnRef(name="year"),
                ColumnRef(name="product_line"),
            ],
            metric=ColumnRef(name="count", aggregate="COUNT"),
        )

        assert [dimension.name for dimension in config.dimensions] == [
            "year",
            "product_line",
        ]

    def test_box_plot_config_accepts_legacy_columns_and_metrics_aliases(self) -> None:
        config = BoxPlotChartConfig(
            chart_type="box_plot",
            columns=[ColumnRef(name="order_date")],
            metrics=[ColumnRef(name="count", aggregate="COUNT")],
        )

        assert config.x.name == "order_date"
        assert config.metric.name == "count"


class TestSecondWaveChartMappings:
    """Form-data mapping tests for second-wave chart configs."""

    def test_map_treemap_config(self) -> None:
        config = TreemapChartConfig(
            chart_type="treemap",
            dimensions=[ColumnRef(name="year"), ColumnRef(name="product_line")],
            metric=ColumnRef(name="count", aggregate="COUNT"),
            show_upper_labels=True,
        )

        result = map_treemap_config(config)

        assert result["viz_type"] == "treemap_v2"
        assert result["groupby"] == ["year", "product_line"]
        assert result["metric"]["aggregate"] == "COUNT"
        assert result["show_upper_labels"] is True

    def test_map_sunburst_config(self) -> None:
        config = SunburstChartConfig(
            chart_type="sunburst",
            dimensions=[ColumnRef(name="year"), ColumnRef(name="product_line")],
            metric=ColumnRef(name="count", aggregate="COUNT"),
            show_labels_threshold=5,
        )

        result = map_sunburst_config(config)

        assert result["viz_type"] == "sunburst_v2"
        assert result["columns"] == ["year", "product_line"]
        assert result["show_labels_threshold"] == 5

    def test_map_sankey_config(self) -> None:
        config = SankeyChartConfig(
            chart_type="sankey",
            source=ColumnRef(name="product_line"),
            target=ColumnRef(name="deal_size"),
            metric=ColumnRef(name="count", aggregate="COUNT"),
        )

        result = map_sankey_config(config)

        assert result["viz_type"] == "sankey_v2"
        assert result["source"] == "product_line"
        assert result["target"] == "deal_size"

    def test_map_word_cloud_config(self) -> None:
        config = WordCloudChartConfig(
            chart_type="word_cloud",
            series=ColumnRef(name="customer_name"),
            metric=ColumnRef(name="count", aggregate="COUNT"),
            rotation="square",
        )

        result = map_word_cloud_config(config)

        assert result["viz_type"] == "word_cloud"
        assert result["series"] == "customer_name"
        assert result["rotation"] == "square"

    def test_map_world_map_config(self) -> None:
        config = WorldMapChartConfig(
            chart_type="world_map",
            entity=ColumnRef(name="country_code"),
            metric=ColumnRef(name="population", aggregate="SUM"),
            secondary_metric=ColumnRef(name="rural_population", aggregate="SUM"),
            show_bubbles=True,
        )

        result = map_world_map_config(config)

        assert result["viz_type"] == "world_map"
        assert result["entity"] == "country_code"
        assert result["show_bubbles"] is True
        assert result["secondary_metric"]["column"]["column_name"] == "rural_population"

    def test_map_box_plot_config(self) -> None:
        config = BoxPlotChartConfig(
            chart_type="box_plot",
            x=ColumnRef(name="order_date"),
            group_by=[ColumnRef(name="product_line")],
            metric=ColumnRef(name="count", aggregate="COUNT"),
            whisker_options="Tukey",
        )

        result = map_box_plot_config(config)

        assert result["viz_type"] == "box_plot"
        assert result["columns"] == ["order_date"]
        assert result["groupby"] == ["product_line"]
        assert result["whiskerOptions"] == "Tukey"

    def test_map_bubble_config(self) -> None:
        config = BubbleChartConfig(
            chart_type="bubble",
            x=ColumnRef(name="quantity_ordered", aggregate="SUM"),
            y=ColumnRef(name="country", aggregate="COUNT_DISTINCT"),
            size=ColumnRef(name="count", aggregate="COUNT"),
            series=ColumnRef(name="product_line"),
            entity=ColumnRef(name="deal_size"),
            max_bubble_size=25,
        )

        result = map_bubble_config(config)

        assert result["viz_type"] == "bubble_v2"
        assert result["entity"] == "deal_size"
        assert result["max_bubble_size"] == "25"
        assert result["x"]["column"]["column_name"] == "quantity_ordered"

    def test_map_config_to_form_data_dispatches_second_wave_types(self) -> None:
        config = TreemapChartConfig(
            chart_type="treemap",
            dimensions=[ColumnRef(name="year"), ColumnRef(name="product_line")],
            metric=ColumnRef(name="count", aggregate="COUNT"),
        )

        result = map_config_to_form_data(config)

        assert result["viz_type"] == "treemap_v2"


class TestSecondWaveChartNamesAndValidation:
    """Name generation and pre-validation tests."""

    def test_generate_chart_name_for_bubble(self) -> None:
        config = BubbleChartConfig(
            chart_type="bubble",
            x=ColumnRef(name="quantity_ordered", aggregate="SUM"),
            y=ColumnRef(name="country", aggregate="COUNT_DISTINCT"),
            size=ColumnRef(name="count", aggregate="COUNT"),
            series=ColumnRef(name="product_line"),
        )

        assert generate_chart_name(config) == "quantity_ordered vs country"

    def test_schema_validator_handles_second_wave_types(self) -> None:
        is_valid, request, error = SchemaValidator.validate_request(
            {
                "dataset_id": 1,
                "config": {
                    "chart_type": "word_cloud",
                    "series": {"name": "customer_name"},
                    "metric": {"name": "count", "aggregate": "COUNT"},
                },
            }
        )

        assert is_valid is True
        assert request is not None
        assert error is None

    def test_schema_validator_reports_missing_bubble_fields(self) -> None:
        is_valid, request, error = SchemaValidator.validate_request(
            {
                "dataset_id": 1,
                "config": {
                    "chart_type": "bubble",
                    "x": {"name": "quantity_ordered", "aggregate": "SUM"},
                },
            }
        )

        assert is_valid is False
        assert request is None
        assert error is not None
        assert error.error_code == "MISSING_BUBBLE_FIELDS"
