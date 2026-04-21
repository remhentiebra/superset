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

"""
Unified schema validation for chart configurations.
Consolidates pre-validation, schema validation, and error enhancement.
"""

import logging
from typing import Any, Dict, Tuple

from pydantic import ValidationError as PydanticValidationError

from superset.mcp_service.chart.registry import (
    CHART_TYPE_CAPABILITIES,
    SUPPORTED_TYPED_CHART_TYPES,
)
from superset.mcp_service.chart.schemas import (
    GenerateChartRequest,
)
from superset.mcp_service.common.error_schemas import ChartGenerationError

logger = logging.getLogger(__name__)


def _typed_chart_union_error(chart_type: str) -> ChartGenerationError | None:
    """Return an explicit validation error for a known typed chart type."""
    error_map = {
        "xy": ChartGenerationError(
            error_type="xy_validation_error",
            message="XY chart configuration validation failed",
            details=(
                "The XY chart configuration is missing required fields "
                "or has invalid structure"
            ),
            suggestions=[
                "Ensure 'x' field exists with {'name': 'column_name'}",
                "Ensure 'y' field is an array: [{'name': 'metric', "
                "'aggregate': 'SUM'}]",
                "Check that all column names are strings",
                "Verify aggregate functions are valid: SUM, COUNT, AVG, MIN, MAX",
            ],
            error_code="XY_VALIDATION_ERROR",
        ),
        "table": ChartGenerationError(
            error_type="table_validation_error",
            message="Table chart configuration validation failed",
            details=(
                "The table chart configuration is missing required fields "
                "or has invalid structure"
            ),
            suggestions=[
                "Ensure 'columns' field is an array of column specifications",
                "Each column needs {'name': 'column_name'}",
                "Optional: add 'aggregate' for metrics",
                "Example: 'columns': [{'name': 'product'}, {'name': 'sales', "
                "'aggregate': 'SUM'}]",
            ],
            error_code="TABLE_VALIDATION_ERROR",
        ),
        "handlebars": ChartGenerationError(
            error_type="handlebars_validation_error",
            message="Handlebars chart configuration validation failed",
            details=(
                "The handlebars chart configuration is missing required fields "
                "or has invalid structure"
            ),
            suggestions=[
                "Ensure 'handlebars_template' is a non-empty string",
                "For aggregate mode: add 'metrics' with aggregate functions",
                "For raw mode: set 'query_mode': 'raw' and add 'columns'",
                "Example: {'chart_type': 'handlebars', 'handlebars_template': "
                "'<ul>{{#each data}}<li>{{this.name}}</li>{{/each}}</ul>', "
                "'metrics': [{'name': 'sales', 'aggregate': 'SUM'}]}",
            ],
            error_code="HANDLEBARS_VALIDATION_ERROR",
        ),
        "funnel": ChartGenerationError(
            error_type="funnel_validation_error",
            message="Funnel chart configuration validation failed",
            details=(
                "The funnel chart configuration is missing required fields "
                "or has invalid structure"
            ),
            suggestions=[
                "Ensure 'dimension' exists with {'name': 'stage_column'}",
                "Ensure 'metric' exists with {'name': 'metric_column', "
                "'aggregate': 'SUM'}",
                "Optional: configure percent_calculation_type, show_labels, "
                "and row_limit",
            ],
            error_code="FUNNEL_VALIDATION_ERROR",
        ),
        "big_number": ChartGenerationError(
            error_type="big_number_validation_error",
            message="Big number chart configuration validation failed",
            details=(
                "The big number configuration is missing required fields "
                "or has invalid structure"
            ),
            suggestions=[
                "Ensure 'metric' exists with {'name': 'metric_column', "
                "'aggregate': 'SUM'}",
                "If show_trend_line=true, also provide 'x': {'name': 'date_column'}",
                "Optional: configure time_grain, number_format, and font sizes",
            ],
            error_code="BIG_NUMBER_VALIDATION_ERROR",
        ),
        "gauge": ChartGenerationError(
            error_type="gauge_validation_error",
            message="Gauge chart configuration validation failed",
            details=(
                "The gauge configuration is missing required fields "
                "or has invalid structure"
            ),
            suggestions=[
                "Ensure 'metric' exists with {'name': 'metric_column', "
                "'aggregate': 'SUM'}",
                "Optional: add 'dimension' and gauge display settings like "
                "start_angle or split_number",
            ],
            error_code="GAUGE_VALIDATION_ERROR",
        ),
        "heatmap": ChartGenerationError(
            error_type="heatmap_validation_error",
            message="Heatmap chart configuration validation failed",
            details=(
                "The heatmap configuration is missing required fields "
                "or has invalid structure"
            ),
            suggestions=[
                "Ensure 'x' and 'y' dimensions exist with {'name': 'column_name'}",
                "Ensure 'metric' exists with {'name': 'metric_column', "
                "'aggregate': 'SUM'}",
                "Optional: configure normalize_across, sort_x_axis, and sort_y_axis",
            ],
            error_code="HEATMAP_VALIDATION_ERROR",
        ),
        "treemap": ChartGenerationError(
            error_type="treemap_validation_error",
            message="Treemap chart configuration validation failed",
            details=(
                "The treemap configuration is missing required fields "
                "or has invalid structure"
            ),
            suggestions=[
                "Ensure 'dimensions' is a non-empty array of columns",
                "Ensure 'metric' exists with {'name': 'metric_column', "
                "'aggregate': 'SUM'}",
                "Optional: configure label_type, show_upper_labels, and row_limit",
            ],
            error_code="TREEMAP_VALIDATION_ERROR",
        ),
        "sunburst": ChartGenerationError(
            error_type="sunburst_validation_error",
            message="Sunburst chart configuration validation failed",
            details=(
                "The sunburst configuration is missing required fields "
                "or has invalid structure"
            ),
            suggestions=[
                "Ensure 'dimensions' is a non-empty array of columns",
                "Ensure 'metric' exists with {'name': 'metric_column', "
                "'aggregate': 'SUM'}",
                "Optional: configure show_labels_threshold and linear_color_scheme",
            ],
            error_code="SUNBURST_VALIDATION_ERROR",
        ),
        "sankey": ChartGenerationError(
            error_type="sankey_validation_error",
            message="Sankey chart configuration validation failed",
            details=(
                "The sankey configuration is missing required fields "
                "or has invalid structure"
            ),
            suggestions=[
                "Ensure 'source' and 'target' exist with {'name': 'column_name'}",
                "Ensure 'metric' exists with {'name': 'metric_column', "
                "'aggregate': 'SUM'}",
                "Optional: configure row_limit and color_scheme",
            ],
            error_code="SANKEY_VALIDATION_ERROR",
        ),
        "word_cloud": ChartGenerationError(
            error_type="word_cloud_validation_error",
            message="Word cloud configuration validation failed",
            details=(
                "The word cloud configuration is missing required fields "
                "or has invalid structure"
            ),
            suggestions=[
                "Ensure 'series' exists with {'name': 'word_column'}",
                "Ensure 'metric' exists with {'name': 'metric_column', "
                "'aggregate': 'COUNT'}",
                "Optional: configure rotation, size_from, size_to, and row_limit",
            ],
            error_code="WORD_CLOUD_VALIDATION_ERROR",
        ),
        "world_map": ChartGenerationError(
            error_type="world_map_validation_error",
            message="World map configuration validation failed",
            details=(
                "The world map configuration is missing required fields "
                "or has invalid structure"
            ),
            suggestions=[
                "Ensure 'entity' exists with {'name': 'country_column'}",
                "Ensure 'metric' exists with {'name': 'metric_column', "
                "'aggregate': 'SUM'}",
                "Optional: configure secondary_metric, country_fieldtype, "
                "and show_bubbles",
            ],
            error_code="WORLD_MAP_VALIDATION_ERROR",
        ),
        "box_plot": ChartGenerationError(
            error_type="box_plot_validation_error",
            message="Box plot configuration validation failed",
            details=(
                "The box plot configuration is missing required fields "
                "or has invalid structure"
            ),
            suggestions=[
                "Ensure 'x' exists with {'name': 'column_name'}",
                "Ensure 'metric' exists with {'name': 'metric_column', "
                "'aggregate': 'COUNT'}",
                "Optional: configure group_by, whisker_options, and time_grain",
            ],
            error_code="BOX_PLOT_VALIDATION_ERROR",
        ),
        "bubble": ChartGenerationError(
            error_type="bubble_validation_error",
            message="Bubble chart configuration validation failed",
            details=(
                "The bubble chart configuration is missing required fields "
                "or has invalid structure"
            ),
            suggestions=[
                "Ensure 'x', 'y', and 'size' are metric columns with aggregates",
                "Ensure 'series' exists with {'name': 'grouping_column'}",
                "Optional: configure entity, max_bubble_size, and axis formats",
            ],
            error_code="BUBBLE_VALIDATION_ERROR",
        ),
    }
    return error_map.get(chart_type)


class SchemaValidator:
    """Unified schema validator with pre-validation and enhanced error messages."""

    @staticmethod
    def validate_request(
        request_data: Dict[str, Any],
    ) -> Tuple[bool, GenerateChartRequest | None, ChartGenerationError | None]:
        """
        Validate request data with pre-validation and enhanced error handling.

        Returns:
            Tuple of (is_valid, parsed_request, error)
        """
        # Pre-validate to catch common issues early
        is_valid, error = SchemaValidator._pre_validate(request_data)
        if not is_valid:
            return False, None, error

        # Try Pydantic validation
        try:
            request = GenerateChartRequest(**request_data)
            return True, request, None
        except PydanticValidationError as e:
            # Enhance the error message
            error = SchemaValidator._enhance_validation_error(e, request_data)
            return False, None, error

    @staticmethod
    def _pre_validate(
        data: Dict[str, Any],
    ) -> Tuple[bool, ChartGenerationError | None]:
        """Pre-validate request data before Pydantic processing."""
        if not isinstance(data, dict):
            return False, ChartGenerationError(
                error_type="invalid_request_format",
                message="Request must be a JSON object",
                details="The request body must be a valid JSON object, not a string "
                "or array",
                suggestions=[
                    "Ensure you're sending a JSON object with 'dataset_id' and "
                    "'config' fields",
                    "Check that Content-Type header is set to 'application/json'",
                ],
                error_code="INVALID_REQUEST_FORMAT",
            )

        # Check for required top-level fields
        if "dataset_id" not in data:
            return False, ChartGenerationError(
                error_type="missing_dataset_id",
                message="Missing required field: dataset_id",
                details="The 'dataset_id' field is required to identify which dataset "
                "to use",
                suggestions=[
                    "Add 'dataset_id' field with the ID of your dataset",
                    "Use list_datasets tool to find available dataset IDs",
                    "Example: {'dataset_id': 1, 'config': {...}}",
                ],
                error_code="MISSING_DATASET_ID",
            )

        if "config" not in data:
            return False, ChartGenerationError(
                error_type="missing_config",
                message="Missing required field: config",
                details="The 'config' field is required to specify chart configuration",
                suggestions=[
                    "Add 'config' field with chart type and settings",
                    "Example: {'dataset_id': 1, 'config': {'chart_type': 'xy', ...}}",
                ],
                error_code="MISSING_CONFIG",
            )

        config = data.get("config", {})
        if not isinstance(config, dict):
            return False, ChartGenerationError(
                error_type="invalid_config_format",
                message="Config must be a JSON object",
                details="The 'config' field must be a valid JSON object with chart "
                "settings",
                suggestions=[
                    "Ensure config is an object, not a string or array",
                    "Example: 'config': {'chart_type': 'xy', 'x': {...}, 'y': [...]}",
                ],
                error_code="INVALID_CONFIG_FORMAT",
            )

        # Check chart_type early
        chart_type = config.get("chart_type")
        if not chart_type:
            createable_types = ", ".join(
                f"'{name}'" for name in SUPPORTED_TYPED_CHART_TYPES
            )
            return False, ChartGenerationError(
                error_type="missing_chart_type",
                message="Missing required field: chart_type",
                details="Chart configuration must specify 'chart_type'",
                suggestions=[
                    "Add 'chart_type': 'xy' for line/bar/area/scatter charts",
                    "Add 'chart_type': 'table' for table visualizations",
                    "Add 'chart_type': 'pie' for pie or donut charts",
                    "Add 'chart_type': 'funnel' for stage conversion analysis",
                    "Add 'chart_type': 'big_number' for KPIs with optional trendline",
                    "Add 'chart_type': 'treemap' for hierarchical charts",
                    f"Supported typed chart types: {createable_types}",
                    "Add 'chart_type': 'pivot_table' for interactive pivot tables",
                    "Add 'chart_type': 'mixed_timeseries' for dual-series time charts",
                    "Add 'chart_type': 'handlebars' for custom HTML template charts",
                    "Add 'chart_type': 'bubble' for x/y/size comparisons",
                    "Example: 'config': {'chart_type': 'xy', ...}",
                ],
                error_code="MISSING_CHART_TYPE",
            )

        return SchemaValidator._pre_validate_chart_type(chart_type, config)

    @staticmethod
    def _pre_validate_chart_type(
        chart_type: str,
        config: Dict[str, Any],
    ) -> Tuple[bool, ChartGenerationError | None]:
        """Validate chart type and dispatch to type-specific pre-validation."""
        chart_type_validators = {
            "xy": SchemaValidator._pre_validate_xy_config,
            "table": SchemaValidator._pre_validate_table_config,
            "pie": SchemaValidator._pre_validate_pie_config,
            "pivot_table": SchemaValidator._pre_validate_pivot_table_config,
            "mixed_timeseries": SchemaValidator._pre_validate_mixed_timeseries_config,
            "handlebars": SchemaValidator._pre_validate_handlebars_config,
            "funnel": SchemaValidator._pre_validate_funnel_config,
            "big_number": SchemaValidator._pre_validate_big_number_config,
            "gauge": SchemaValidator._pre_validate_gauge_config,
            "heatmap": SchemaValidator._pre_validate_heatmap_config,
            "treemap": SchemaValidator._pre_validate_treemap_config,
            "sunburst": SchemaValidator._pre_validate_sunburst_config,
            "sankey": SchemaValidator._pre_validate_sankey_config,
            "word_cloud": SchemaValidator._pre_validate_word_cloud_config,
            "world_map": SchemaValidator._pre_validate_world_map_config,
            "box_plot": SchemaValidator._pre_validate_box_plot_config,
            "bubble": SchemaValidator._pre_validate_bubble_config,
        }

        if not isinstance(chart_type, str) or chart_type not in chart_type_validators:
            valid_types = ", ".join(sorted(SUPPORTED_TYPED_CHART_TYPES))
            supported_summaries = [
                f"{cap.chart_type}: {cap.summary}"
                for cap in CHART_TYPE_CAPABILITIES.values()
            ]
            return False, ChartGenerationError(
                error_type="invalid_chart_type",
                message=f"Invalid chart_type: '{chart_type}'",
                details=f"Chart type '{chart_type}' is not supported. "
                f"Must be one of: {valid_types}",
                suggestions=[
                    "Use 'chart_type': 'xy' for line, bar, area, or scatter charts",
                    "Use 'chart_type': 'table' for tabular data display",
                    "Use 'chart_type': 'pie' for pie or donut charts",
                    "Use 'chart_type': 'funnel' for stage-based conversion analysis",
                    "Use 'chart_type': 'big_number' for a KPI metric",
                    "Use 'chart_type': 'gauge' for a gauge-style KPI",
                    "Use 'chart_type': 'heatmap' for a two-dimensional metric matrix",
                    "Use 'chart_type': 'treemap' for hierarchical rectangles",
                    "Use 'chart_type': 'sunburst' for hierarchical radial charts",
                    "Use 'chart_type': 'sankey' for source-to-target flows",
                    "Use 'chart_type': 'word_cloud' for text frequency displays",
                    "Use 'chart_type': 'world_map' for geographic comparisons",
                    "Use 'chart_type': 'box_plot' for distributions and outliers",
                    "Use 'chart_type': 'bubble' for x/y/size metric comparisons",
                    "Use 'chart_type': 'pivot_table' for interactive pivot tables",
                    "Use 'chart_type': 'mixed_timeseries' for dual-series time charts",
                    "Use 'chart_type': 'handlebars' for custom HTML template charts",
                    *supported_summaries[:3],
                    "Check spelling and ensure lowercase",
                ],
                error_code="INVALID_CHART_TYPE",
            )

        return chart_type_validators[chart_type](config)

    @staticmethod
    def _pre_validate_xy_config(
        config: Dict[str, Any],
    ) -> Tuple[bool, ChartGenerationError | None]:
        """Pre-validate XY chart configuration."""
        # x is optional — defaults to dataset's main_dttm_col in map_xy_config
        if "y" not in config:
            return False, ChartGenerationError(
                error_type="missing_xy_fields",
                message="XY chart missing required field: 'y' (Y-axis metrics)",
                details="XY charts require Y-axis (metrics) specifications. "
                "X-axis is optional and defaults to the dataset's primary "
                "datetime column when omitted.",
                suggestions=[
                    "Add 'y' field: [{'name': 'metric_column', 'aggregate': 'SUM'}] "
                    "for Y-axis",
                    "Example: {'chart_type': 'xy', 'x': {'name': 'date'}, "
                    "'y': [{'name': 'sales', 'aggregate': 'SUM'}]}",
                ],
                error_code="MISSING_XY_FIELDS",
            )

        # Validate Y is a list
        if not isinstance(config.get("y", []), list):
            return False, ChartGenerationError(
                error_type="invalid_y_format",
                message="Y-axis must be a list of metrics",
                details="The 'y' field must be an array of metric specifications",
                suggestions=[
                    "Wrap Y-axis metric in array: 'y': [{'name': 'column', "
                    "'aggregate': 'SUM'}]",
                    "Multiple metrics supported: 'y': [metric1, metric2, ...]",
                ],
                error_code="INVALID_Y_FORMAT",
            )

        return True, None

    @staticmethod
    def _pre_validate_table_config(
        config: Dict[str, Any],
    ) -> Tuple[bool, ChartGenerationError | None]:
        """Pre-validate table chart configuration."""
        if "columns" not in config:
            return False, ChartGenerationError(
                error_type="missing_columns",
                message="Table chart missing required field: columns",
                details="Table charts require a 'columns' array to specify which "
                "columns to display",
                suggestions=[
                    "Add 'columns' field with array of column specifications",
                    "Example: 'columns': [{'name': 'product'}, {'name': 'sales', "
                    "'aggregate': 'SUM'}]",
                    "Each column can have optional 'aggregate' for metrics",
                ],
                error_code="MISSING_COLUMNS",
            )

        if not isinstance(config.get("columns", []), list):
            return False, ChartGenerationError(
                error_type="invalid_columns_format",
                message="Columns must be a list",
                details="The 'columns' field must be an array of column specifications",
                suggestions=[
                    "Ensure columns is an array: 'columns': [...]",
                    "Each column should be an object with 'name' field",
                ],
                error_code="INVALID_COLUMNS_FORMAT",
            )

        return True, None

    @staticmethod
    def _pre_validate_pie_config(
        config: Dict[str, Any],
    ) -> Tuple[bool, ChartGenerationError | None]:
        """Pre-validate pie chart configuration."""
        missing_fields = []

        if "dimension" not in config:
            missing_fields.append("'dimension' (category column for slices)")
        if "metric" not in config:
            missing_fields.append("'metric' (value metric for slice sizes)")

        if missing_fields:
            return False, ChartGenerationError(
                error_type="missing_pie_fields",
                message=f"Pie chart missing required "
                f"fields: {', '.join(missing_fields)}",
                details="Pie charts require a dimension (categories) and a metric "
                "(values)",
                suggestions=[
                    "Add 'dimension' field: {'name': 'category_column'}",
                    "Add 'metric' field: {'name': 'value_column', 'aggregate': 'SUM'}",
                    "Example: {'chart_type': 'pie', 'dimension': {'name': "
                    "'product'}, 'metric': {'name': 'revenue', 'aggregate': 'SUM'}}",
                ],
                error_code="MISSING_PIE_FIELDS",
            )

        return True, None

    @staticmethod
    def _pre_validate_funnel_config(
        config: Dict[str, Any],
    ) -> Tuple[bool, ChartGenerationError | None]:
        """Pre-validate funnel chart configuration."""
        missing_fields = []
        if "dimension" not in config and "groupby" not in config:
            missing_fields.append("'dimension' (funnel stage column)")
        if "metric" not in config:
            missing_fields.append("'metric' (value metric)")

        if missing_fields:
            return False, ChartGenerationError(
                error_type="missing_funnel_fields",
                message=(
                    f"Funnel chart missing required fields: {', '.join(missing_fields)}"
                ),
                details="Funnel charts require a stage dimension and a metric.",
                suggestions=[
                    "Add 'dimension': {'name': 'status'} for the funnel stages",
                    "Add 'metric': {'name': 'count', 'aggregate': 'COUNT'}",
                    "Optional: percent_calculation_type='first_step' "
                    "or 'previous_step'",
                ],
                error_code="MISSING_FUNNEL_FIELDS",
            )
        return True, None

    @staticmethod
    def _pre_validate_big_number_config(
        config: Dict[str, Any],
    ) -> Tuple[bool, ChartGenerationError | None]:
        """Pre-validate big number chart configuration."""
        if "metric" not in config:
            return False, ChartGenerationError(
                error_type="missing_big_number_metric",
                message="Big number chart missing required field: metric",
                details="Big number charts require a metric to display as the KPI.",
                suggestions=[
                    "Add 'metric': {'name': 'revenue', 'aggregate': 'SUM'}",
                    "Optional: set 'show_trend_line': true and provide "
                    "'x' for a trendline",
                ],
                error_code="MISSING_BIG_NUMBER_METRIC",
            )

        if (
            config.get("show_trend_line")
            and "x" not in config
            and "x_axis" not in config
        ):
            return False, ChartGenerationError(
                error_type="missing_big_number_x",
                message="Big number charts with trendline require an x column",
                details=(
                    "When show_trend_line is true, the chart needs "
                    "a temporal x-axis column."
                ),
                suggestions=[
                    "Add 'x': {'name': 'order_date'} when show_trend_line=true",
                    "Or remove 'show_trend_line' for a KPI-only chart",
                ],
                error_code="MISSING_BIG_NUMBER_X",
            )
        return True, None

    @staticmethod
    def _pre_validate_gauge_config(
        config: Dict[str, Any],
    ) -> Tuple[bool, ChartGenerationError | None]:
        """Pre-validate gauge chart configuration."""
        if "metric" not in config:
            return False, ChartGenerationError(
                error_type="missing_gauge_metric",
                message="Gauge chart missing required field: metric",
                details="Gauge charts require a metric to render the gauge value.",
                suggestions=[
                    "Add 'metric': {'name': 'count', 'aggregate': 'COUNT'}",
                    "Optional: add 'dimension' to group the gauge by a category",
                ],
                error_code="MISSING_GAUGE_METRIC",
            )
        return True, None

    @staticmethod
    def _pre_validate_heatmap_config(
        config: Dict[str, Any],
    ) -> Tuple[bool, ChartGenerationError | None]:
        """Pre-validate heatmap chart configuration."""
        missing_fields = []
        if "x" not in config and "x_axis" not in config:
            missing_fields.append("'x' (X-axis dimension)")
        if "y" not in config and "groupby" not in config and "dimension" not in config:
            missing_fields.append("'y' (Y-axis dimension)")
        if "metric" not in config:
            missing_fields.append("'metric' (heatmap metric)")

        if missing_fields:
            return False, ChartGenerationError(
                error_type="missing_heatmap_fields",
                message=(
                    "Heatmap chart missing required fields: "
                    f"{', '.join(missing_fields)}"
                ),
                details="Heatmap charts require x and y dimensions plus a metric.",
                suggestions=[
                    "Add 'x': {'name': 'product_line'}",
                    "Add 'y': {'name': 'deal_size'}",
                    "Add 'metric': {'name': 'count', 'aggregate': 'COUNT'}",
                ],
                error_code="MISSING_HEATMAP_FIELDS",
            )
        return True, None

    @staticmethod
    def _pre_validate_treemap_config(
        config: Dict[str, Any],
    ) -> Tuple[bool, ChartGenerationError | None]:
        """Pre-validate treemap chart configuration."""
        missing_fields = []
        if (
            "dimensions" not in config
            and "groupby" not in config
            and "columns" not in config
        ):
            missing_fields.append("'dimensions' (hierarchy columns)")
        if "metric" not in config:
            missing_fields.append("'metric' (treemap metric)")

        if missing_fields:
            return False, ChartGenerationError(
                error_type="missing_treemap_fields",
                message=(
                    "Treemap chart missing required fields: "
                    f"{', '.join(missing_fields)}"
                ),
                details=(
                    "Treemap charts require at least one hierarchy dimension "
                    "and a metric."
                ),
                suggestions=[
                    "Add 'dimensions': [{'name': 'year'}, {'name': 'product_line'}]",
                    "Add 'metric': {'name': 'count', 'aggregate': 'COUNT'}",
                ],
                error_code="MISSING_TREEMAP_FIELDS",
            )
        return True, None

    @staticmethod
    def _pre_validate_sunburst_config(
        config: Dict[str, Any],
    ) -> Tuple[bool, ChartGenerationError | None]:
        """Pre-validate sunburst chart configuration."""
        missing_fields = []
        if (
            "dimensions" not in config
            and "groupby" not in config
            and "columns" not in config
        ):
            missing_fields.append("'dimensions' (hierarchy columns)")
        if "metric" not in config:
            missing_fields.append("'metric' (sunburst metric)")

        if missing_fields:
            return False, ChartGenerationError(
                error_type="missing_sunburst_fields",
                message=(
                    "Sunburst chart missing required fields: "
                    f"{', '.join(missing_fields)}"
                ),
                details=(
                    "Sunburst charts require at least one hierarchy dimension "
                    "and a metric."
                ),
                suggestions=[
                    "Add 'dimensions': [{'name': 'year'}, {'name': 'product_line'}]",
                    "Add 'metric': {'name': 'count', 'aggregate': 'COUNT'}",
                ],
                error_code="MISSING_SUNBURST_FIELDS",
            )
        return True, None

    @staticmethod
    def _pre_validate_sankey_config(
        config: Dict[str, Any],
    ) -> Tuple[bool, ChartGenerationError | None]:
        """Pre-validate sankey chart configuration."""
        missing_fields = []
        if "source" not in config:
            missing_fields.append("'source' (source dimension)")
        if "target" not in config:
            missing_fields.append("'target' (target dimension)")
        if "metric" not in config:
            missing_fields.append("'metric' (flow metric)")

        if missing_fields:
            return False, ChartGenerationError(
                error_type="missing_sankey_fields",
                message=(
                    f"Sankey chart missing required fields: {', '.join(missing_fields)}"
                ),
                details=(
                    "Sankey charts require source and target dimensions plus a metric."
                ),
                suggestions=[
                    "Add 'source': {'name': 'product_line'}",
                    "Add 'target': {'name': 'deal_size'}",
                    "Add 'metric': {'name': 'count', 'aggregate': 'COUNT'}",
                ],
                error_code="MISSING_SANKEY_FIELDS",
            )
        return True, None

    @staticmethod
    def _pre_validate_word_cloud_config(
        config: Dict[str, Any],
    ) -> Tuple[bool, ChartGenerationError | None]:
        """Pre-validate word cloud chart configuration."""
        missing_fields = []
        if (
            "series" not in config
            and "dimension" not in config
            and "groupby" not in config
        ):
            missing_fields.append("'series' (word dimension)")
        if "metric" not in config:
            missing_fields.append("'metric' (word size metric)")

        if missing_fields:
            return False, ChartGenerationError(
                error_type="missing_word_cloud_fields",
                message=(
                    "Word cloud chart missing required fields: "
                    f"{', '.join(missing_fields)}"
                ),
                details=(
                    "Word cloud charts require a displayed word dimension and a metric."
                ),
                suggestions=[
                    "Add 'series': {'name': 'customer_name'}",
                    "Add 'metric': {'name': 'count', 'aggregate': 'COUNT'}",
                ],
                error_code="MISSING_WORD_CLOUD_FIELDS",
            )
        return True, None

    @staticmethod
    def _pre_validate_world_map_config(
        config: Dict[str, Any],
    ) -> Tuple[bool, ChartGenerationError | None]:
        """Pre-validate world map chart configuration."""
        missing_fields = []
        if "entity" not in config and "country" not in config:
            missing_fields.append("'entity' (country/entity column)")
        if "metric" not in config:
            missing_fields.append("'metric' (map metric)")

        if missing_fields:
            return False, ChartGenerationError(
                error_type="missing_world_map_fields",
                message=(
                    "World map chart missing required fields: "
                    f"{', '.join(missing_fields)}"
                ),
                details=(
                    "World map charts require a geographic entity column and a metric."
                ),
                suggestions=[
                    "Add 'entity': {'name': 'country_code'}",
                    "Add 'metric': {'name': 'population', 'aggregate': 'SUM'}",
                    "Optional: add 'secondary_metric' and 'show_bubbles': true",
                ],
                error_code="MISSING_WORLD_MAP_FIELDS",
            )
        return True, None

    @staticmethod
    def _pre_validate_box_plot_config(
        config: Dict[str, Any],
    ) -> Tuple[bool, ChartGenerationError | None]:
        """Pre-validate box plot chart configuration."""
        missing_fields = []
        if "x" not in config and "x_axis" not in config and "columns" not in config:
            missing_fields.append("'x' (distribution axis column)")
        if "metric" not in config and "metrics" not in config:
            missing_fields.append("'metric' (distribution metric)")

        if missing_fields:
            return False, ChartGenerationError(
                error_type="missing_box_plot_fields",
                message=(
                    "Box plot chart missing required fields: "
                    f"{', '.join(missing_fields)}"
                ),
                details="Box plot charts require a dimension column and a metric.",
                suggestions=[
                    "Add 'x': {'name': 'order_date'}",
                    "Add 'metric': {'name': 'count', 'aggregate': 'COUNT'}",
                    "Optional: add 'group_by': [{'name': 'product_line'}]",
                ],
                error_code="MISSING_BOX_PLOT_FIELDS",
            )
        return True, None

    @staticmethod
    def _pre_validate_bubble_config(
        config: Dict[str, Any],
    ) -> Tuple[bool, ChartGenerationError | None]:
        """Pre-validate bubble chart configuration."""
        missing_fields = []
        for field_name, description in (
            ("x", "'x' (x-axis metric)"),
            ("y", "'y' (y-axis metric)"),
            ("size", "'size' (bubble size metric)"),
        ):
            if field_name not in config:
                missing_fields.append(description)
        if (
            "series" not in config
            and "dimension" not in config
            and "groupby" not in config
        ):
            missing_fields.append("'series' (grouping dimension)")

        if missing_fields:
            return False, ChartGenerationError(
                error_type="missing_bubble_fields",
                message=(
                    f"Bubble chart missing required fields: {', '.join(missing_fields)}"
                ),
                details=(
                    "Bubble charts require x, y, and size metrics plus "
                    "a grouping dimension."
                ),
                suggestions=[
                    "Add 'x': {'name': 'quantity_ordered', 'aggregate': 'SUM'}",
                    "Add 'y': {'name': 'country', 'aggregate': 'COUNT_DISTINCT'}",
                    "Add 'size': {'name': 'count', 'aggregate': 'COUNT'}",
                    "Add 'series': {'name': 'product_line'}",
                ],
                error_code="MISSING_BUBBLE_FIELDS",
            )
        return True, None

    @staticmethod
    def _pre_validate_handlebars_config(
        config: Dict[str, Any],
    ) -> Tuple[bool, ChartGenerationError | None]:
        """Pre-validate handlebars chart configuration."""
        if "handlebars_template" not in config:
            return False, ChartGenerationError(
                error_type="missing_handlebars_template",
                message="Handlebars chart missing required field: handlebars_template",
                details="Handlebars charts require a 'handlebars_template' string "
                "containing Handlebars HTML template markup",
                suggestions=[
                    "Add 'handlebars_template' with a Handlebars HTML template",
                    "Data is available as {{data}} array in the template",
                    "Example: '<ul>{{#each data}}<li>{{this.name}}: "
                    "{{this.value}}</li>{{/each}}</ul>'",
                ],
                error_code="MISSING_HANDLEBARS_TEMPLATE",
            )

        template = config.get("handlebars_template")
        if not isinstance(template, str) or not template.strip():
            return False, ChartGenerationError(
                error_type="invalid_handlebars_template",
                message="Handlebars template must be a non-empty string",
                details="The 'handlebars_template' field must be a non-empty string "
                "containing valid Handlebars HTML template markup",
                suggestions=[
                    "Ensure handlebars_template is a non-empty string",
                    "Example: '<ul>{{#each data}}<li>{{this.name}}</li>{{/each}}</ul>'",
                ],
                error_code="INVALID_HANDLEBARS_TEMPLATE",
            )

        query_mode = config.get("query_mode", "aggregate")
        if query_mode not in ("aggregate", "raw"):
            return False, ChartGenerationError(
                error_type="invalid_query_mode",
                message="Invalid query_mode for handlebars chart",
                details="query_mode must be either 'aggregate' or 'raw'",
                suggestions=[
                    "Use 'aggregate' for aggregated data (default)",
                    "Use 'raw' for individual rows",
                ],
                error_code="INVALID_QUERY_MODE",
            )

        if query_mode == "raw" and not config.get("columns"):
            return False, ChartGenerationError(
                error_type="missing_raw_columns",
                message="Handlebars chart in 'raw' mode requires 'columns'",
                details="When query_mode is 'raw', you must specify which columns "
                "to include in the query results",
                suggestions=[
                    "Add 'columns': [{'name': 'column_name'}] for raw mode",
                    "Or use query_mode='aggregate' with 'metrics' "
                    "and optional 'groupby'",
                ],
                error_code="MISSING_RAW_COLUMNS",
            )

        if query_mode == "aggregate" and not config.get("metrics"):
            return False, ChartGenerationError(
                error_type="missing_aggregate_metrics",
                message="Handlebars chart in 'aggregate' mode requires 'metrics'",
                details="When query_mode is 'aggregate' (default), you must specify "
                "at least one metric with an aggregate function",
                suggestions=[
                    "Add 'metrics': [{'name': 'column', 'aggregate': 'SUM'}]",
                    "Or use query_mode='raw' with 'columns' for individual rows",
                ],
                error_code="MISSING_AGGREGATE_METRICS",
            )

        return True, None

    @staticmethod
    def _pre_validate_pivot_table_config(
        config: Dict[str, Any],
    ) -> Tuple[bool, ChartGenerationError | None]:
        """Pre-validate pivot table configuration."""
        missing_fields = []

        if "rows" not in config:
            missing_fields.append("'rows' (row grouping columns)")
        if "metrics" not in config:
            missing_fields.append("'metrics' (aggregation metrics)")

        if missing_fields:
            return False, ChartGenerationError(
                error_type="missing_pivot_fields",
                message=f"Pivot table missing required "
                f"fields: {', '.join(missing_fields)}",
                details="Pivot tables require row groupings and metrics",
                suggestions=[
                    "Add 'rows' field: [{'name': 'category'}]",
                    "Add 'metrics' field: [{'name': 'sales', 'aggregate': 'SUM'}]",
                    "Optional 'columns' for cross-tabulation: [{'name': 'region'}]",
                ],
                error_code="MISSING_PIVOT_FIELDS",
            )

        if not isinstance(config.get("rows", []), list):
            return False, ChartGenerationError(
                error_type="invalid_rows_format",
                message="Rows must be a list of columns",
                details="The 'rows' field must be an array of column specifications",
                suggestions=[
                    "Wrap row columns in array: 'rows': [{'name': 'category'}]",
                ],
                error_code="INVALID_ROWS_FORMAT",
            )

        if not isinstance(config.get("metrics", []), list):
            return False, ChartGenerationError(
                error_type="invalid_metrics_format",
                message="Metrics must be a list",
                details="The 'metrics' field must be an array of metric specifications",
                suggestions=[
                    "Wrap metrics in array: 'metrics': [{'name': 'sales', "
                    "'aggregate': 'SUM'}]",
                ],
                error_code="INVALID_METRICS_FORMAT",
            )

        return True, None

    @staticmethod
    def _pre_validate_mixed_timeseries_config(
        config: Dict[str, Any],
    ) -> Tuple[bool, ChartGenerationError | None]:
        """Pre-validate mixed timeseries configuration."""
        missing_fields = []

        if "x" not in config:
            missing_fields.append("'x' (X-axis temporal column)")
        if "y" not in config:
            missing_fields.append("'y' (primary Y-axis metrics)")
        if "y_secondary" not in config:
            missing_fields.append("'y_secondary' (secondary Y-axis metrics)")

        if missing_fields:
            return False, ChartGenerationError(
                error_type="missing_mixed_timeseries_fields",
                message=f"Mixed timeseries chart missing required "
                f"fields: {', '.join(missing_fields)}",
                details="Mixed timeseries charts require an x-axis, primary metrics, "
                "and secondary metrics",
                suggestions=[
                    "Add 'x' field: {'name': 'date_column'}",
                    "Add 'y' field: [{'name': 'revenue', 'aggregate': 'SUM'}]",
                    "Add 'y_secondary' field: [{'name': 'orders', "
                    "'aggregate': 'COUNT'}]",
                    "Optional: 'primary_kind' and 'secondary_kind' for chart types",
                ],
                error_code="MISSING_MIXED_TIMESERIES_FIELDS",
            )

        for field_name in ["y", "y_secondary"]:
            if not isinstance(config.get(field_name, []), list):
                return False, ChartGenerationError(
                    error_type=f"invalid_{field_name}_format",
                    message=f"'{field_name}' must be a list of metrics",
                    details=f"The '{field_name}' field must be an array of metric "
                    "specifications",
                    suggestions=[
                        f"Wrap in array: '{field_name}': "
                        "[{'name': 'col', 'aggregate': 'SUM'}]",
                    ],
                    error_code=f"INVALID_{field_name.upper()}_FORMAT",
                )

        return True, None

    @staticmethod
    def _enhance_validation_error(
        error: PydanticValidationError, request_data: Dict[str, Any]
    ) -> ChartGenerationError:
        """Convert Pydantic validation errors to user-friendly messages."""
        errors = error.errors()

        # Check for discriminated union errors (generic "'table' was expected")
        for err in errors:
            if err.get("type") == "union_tag_invalid" or "discriminator" in str(
                err.get("ctx", {})
            ):
                # This is the generic union error - provide better message
                config = request_data.get("config", {})
                chart_type = config.get("chart_type", "unknown")
                typed_error = _typed_chart_union_error(str(chart_type))
                if typed_error is not None:
                    return typed_error

        # Default enhanced error
        error_details = []
        for err in errors[:3]:  # Show first 3 errors
            loc = " -> ".join(str(location) for location in err.get("loc", []))
            msg = err.get("msg", "Validation failed")
            error_details.append(f"{loc}: {msg}")

        return ChartGenerationError(
            error_type="validation_error",
            message="Chart configuration validation failed",
            details="; ".join(error_details),
            suggestions=[
                "Check that all required fields are present",
                "Ensure field types match the schema",
                "Use get_dataset_info to verify column names",
                "Refer to the API documentation for field requirements",
            ],
            error_code="VALIDATION_ERROR",
        )
