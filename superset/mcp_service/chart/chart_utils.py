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
Shared chart utilities for MCP tools

This module contains shared logic for chart configuration mapping and explore link
generation that can be used by both generate_chart and generate_explore_link tools.
"""

import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict

from superset.constants import NO_TIME_RANGE
from superset.mcp_service.chart.registry import STATIC_VIZ_TYPE_BY_CHART_TYPE
from superset.mcp_service.chart.schemas import (
    BigNumberChartConfig,
    BoxPlotChartConfig,
    BubbleChartConfig,
    ChartCapabilities,
    ChartFilterConfig,
    ChartSemantics,
    ColumnRef,
    FunnelChartConfig,
    GaugeChartConfig,
    HandlebarsChartConfig,
    HeatmapChartConfig,
    MetricFilterConfig,
    MixedTimeseriesChartConfig,
    NullFilterConfig,
    PieChartConfig,
    PivotTableChartConfig,
    RangeFilterConfig,
    SankeyChartConfig,
    SunburstChartConfig,
    TableChartConfig,
    TimeFilterConfig,
    TreemapChartConfig,
    WordCloudChartConfig,
    WorldMapChartConfig,
    XYChartConfig,
)
from superset.mcp_service.utils.url_utils import get_superset_base_url
from superset.utils import json
from superset.utils.core import FilterOperator

logger = logging.getLogger(__name__)


@dataclass
class DatasetValidationResult:
    """Result of dataset accessibility validation."""

    is_valid: bool
    dataset_id: int | str | None
    dataset_name: str | None
    warnings: list[str]
    error: str | None = None


def validate_chart_dataset(
    chart: Any,
    check_access: bool = True,
) -> DatasetValidationResult:
    """
    Validate that a chart's dataset exists and is accessible.

    This shared utility should be called by MCP tools after creating or retrieving
    charts to detect issues like missing or deleted datasets early.

    Args:
        chart: A chart-like object with datasource_id, datasource_type attributes
        check_access: Whether to also check user permissions (default True)

    Returns:
        DatasetValidationResult with validation status and any warnings
    """
    from sqlalchemy.exc import SQLAlchemyError

    from superset.daos.dataset import DatasetDAO
    from superset.mcp_service.auth import has_dataset_access

    warnings: list[str] = []
    datasource_id = getattr(chart, "datasource_id", None)

    # Check if chart has a datasource reference
    if datasource_id is None:
        return DatasetValidationResult(
            is_valid=False,
            dataset_id=None,
            dataset_name=None,
            warnings=[],
            error="Chart has no dataset reference (datasource_id is None)",
        )

    # Try to look up the dataset
    try:
        dataset = DatasetDAO.find_by_id(datasource_id)

        if dataset is None:
            return DatasetValidationResult(
                is_valid=False,
                dataset_id=datasource_id,
                dataset_name=None,
                warnings=[],
                error=(
                    f"Dataset (ID: {datasource_id}) has been deleted or does not "
                    f"exist. The chart will not render correctly. "
                    f"Consider updating the chart to use a different dataset."
                ),
            )

        dataset_name = getattr(dataset, "table_name", None) or getattr(
            dataset, "name", None
        )

        # Check if it's a virtual dataset (SQL Lab query)
        is_virtual = bool(getattr(dataset, "sql", None))
        if is_virtual:
            warnings.append(
                f"This chart uses a virtual dataset (SQL-based). "
                f"If the dataset '{dataset_name}' is deleted, this chart will break."
            )

        # Check access permissions if requested
        if check_access and not has_dataset_access(dataset):
            return DatasetValidationResult(
                is_valid=False,
                dataset_id=datasource_id,
                dataset_name=dataset_name,
                warnings=warnings,
                error=(
                    f"Access denied to dataset '{dataset_name}' (ID: {datasource_id}). "
                    f"You do not have permission to view this dataset."
                ),
            )

        return DatasetValidationResult(
            is_valid=True,
            dataset_id=datasource_id,
            dataset_name=dataset_name,
            warnings=warnings,
            error=None,
        )

    except (AttributeError, ValueError, RuntimeError, SQLAlchemyError) as e:
        logger.exception("Error validating chart dataset %s: %s", datasource_id, e)
        return DatasetValidationResult(
            is_valid=False,
            dataset_id=datasource_id,
            dataset_name=None,
            warnings=[],
            error=f"Error validating dataset (ID: {datasource_id}): {str(e)}",
        )


def generate_explore_link(dataset_id: int | str, form_data: Dict[str, Any]) -> str:
    """Generate an explore link for the given dataset and form data."""
    from sqlalchemy.exc import SQLAlchemyError

    from superset.commands.exceptions import CommandException
    from superset.commands.explore.form_data.parameters import CommandParameters
    from superset.daos.dataset import DatasetDAO
    from superset.exceptions import SupersetException
    from superset.mcp_service.commands.create_form_data import (
        MCPCreateFormDataCommand,
    )
    from superset.utils.core import DatasourceType

    base_url = get_superset_base_url()
    numeric_dataset_id = None
    dataset = None

    try:
        if isinstance(dataset_id, int) or (
            isinstance(dataset_id, str) and dataset_id.isdigit()
        ):
            numeric_dataset_id = (
                int(dataset_id) if isinstance(dataset_id, str) else dataset_id
            )
            dataset = DatasetDAO.find_by_id(numeric_dataset_id)
        else:
            # Try UUID lookup using DAO flexible method
            dataset = DatasetDAO.find_by_id(dataset_id, id_column="uuid")
            if dataset:
                numeric_dataset_id = dataset.id

        if not dataset or numeric_dataset_id is None:
            # Fallback to basic explore URL
            return (
                f"{base_url}/explore/?datasource_type=table&datasource_id={dataset_id}"
            )

        # Add datasource to form_data
        form_data_with_datasource = {
            **form_data,
            "datasource": f"{numeric_dataset_id}__table",
        }

        # Try to create form_data in cache using MCP-specific CreateFormDataCommand
        cmd_params = CommandParameters(
            datasource_type=DatasourceType.TABLE,
            datasource_id=numeric_dataset_id,
            chart_id=0,  # 0 for new charts
            tab_id=None,
            form_data=json.dumps(form_data_with_datasource),
        )

        # Create the form_data cache entry and get the key
        form_data_key = MCPCreateFormDataCommand(cmd_params).run()

        # Return URL with just the form_data_key
        return f"{base_url}/explore/?form_data_key={form_data_key}"

    except (
        CommandException,
        SupersetException,
        SQLAlchemyError,
        KeyError,
        ValueError,
        AttributeError,
        TypeError,
    ) as e:
        # Fallback to basic explore URL with numeric ID if available
        logger.debug("Explore link generation fallback due to: %s", e)
        if numeric_dataset_id is not None:
            return (
                f"{base_url}/explore/?datasource_type=table"
                f"&datasource_id={numeric_dataset_id}"
            )
        return f"{base_url}/explore/?datasource_type=table&datasource_id={dataset_id}"


def is_column_truly_temporal(column_name: str, dataset_id: int | str | None) -> bool:
    """
    Check if a column is truly temporal based on its SQL data type.

    This is important because Superset may mark columns as is_dttm=True based on
    column name heuristics (e.g., "year", "month"), but if the actual SQL type is
    BIGINT or INTEGER, DATE_TRUNC will fail.

    Uses the database engine spec's column type mapping to determine the actual
    GenericDataType, bypassing the is_dttm flag which may be set incorrectly.

    Args:
        column_name: Name of the column to check
        dataset_id: Dataset ID to look up column metadata

    Returns:
        True if the column has a real temporal SQL type, False otherwise
    """
    from superset.daos.dataset import DatasetDAO
    from superset.utils.core import GenericDataType

    if not dataset_id:
        return True  # Default to temporal if we can't check (backward compatible)

    try:
        # Find dataset
        if isinstance(dataset_id, int) or (
            isinstance(dataset_id, str) and dataset_id.isdigit()
        ):
            dataset = DatasetDAO.find_by_id(int(dataset_id))
        else:
            dataset = DatasetDAO.find_by_id(dataset_id, id_column="uuid")

        if not dataset:
            return True  # Default to temporal if dataset not found

        # Find the column and check its actual type using db_engine_spec
        column_lower = column_name.lower()
        for col in dataset.columns:
            if col.column_name.lower() == column_lower:
                col_type = col.type
                if not col_type:
                    # No type info, trust is_dttm flag
                    return getattr(col, "is_dttm", False)

                # Use the db_engine_spec to get the actual GenericDataType
                # This bypasses the is_dttm flag and checks the real SQL type
                db_engine_spec = dataset.database.db_engine_spec
                column_spec = db_engine_spec.get_column_spec(col_type)

                if column_spec:
                    is_temporal = column_spec.generic_type == GenericDataType.TEMPORAL
                    if not is_temporal:
                        logger.debug(
                            "Column '%s' has type '%s' (generic: %s), "
                            "treating as non-temporal",
                            column_name,
                            col_type,
                            column_spec.generic_type,
                        )
                    return is_temporal

                # If no column_spec, trust is_dttm flag
                return getattr(col, "is_dttm", False)

        return True  # Default if column not found

    except (ValueError, AttributeError) as e:
        logger.warning(
            "Error checking column type for '%s' in dataset %s: %s",
            column_name,
            dataset_id,
            e,
        )
        return True  # Default to temporal on error (backward compatible)


def map_config_to_form_data(
    config: TableChartConfig
    | XYChartConfig
    | PieChartConfig
    | PivotTableChartConfig
    | MixedTimeseriesChartConfig
    | HandlebarsChartConfig
    | FunnelChartConfig
    | BigNumberChartConfig
    | GaugeChartConfig
    | HeatmapChartConfig
    | TreemapChartConfig
    | SunburstChartConfig
    | SankeyChartConfig
    | WordCloudChartConfig
    | WorldMapChartConfig
    | BoxPlotChartConfig
    | BubbleChartConfig,
    dataset_id: int | str | None = None,
) -> Dict[str, Any]:
    """Map chart config to Superset form_data."""
    mapper_by_type: dict[type[Any], Callable[[Any], Dict[str, Any]]] = {
        TableChartConfig: lambda current: map_table_config(current),
        XYChartConfig: lambda current: map_xy_config(current, dataset_id=dataset_id),
        PieChartConfig: lambda current: map_pie_config(current),
        PivotTableChartConfig: lambda current: map_pivot_table_config(current),
        MixedTimeseriesChartConfig: lambda current: map_mixed_timeseries_config(
            current,
            dataset_id=dataset_id,
        ),
        HandlebarsChartConfig: lambda current: map_handlebars_config(current),
        FunnelChartConfig: lambda current: map_funnel_config(current),
        BigNumberChartConfig: lambda current: map_big_number_config(current),
        GaugeChartConfig: lambda current: map_gauge_config(current),
        HeatmapChartConfig: lambda current: map_heatmap_config(current),
        TreemapChartConfig: lambda current: map_treemap_config(current),
        SunburstChartConfig: lambda current: map_sunburst_config(current),
        SankeyChartConfig: lambda current: map_sankey_config(current),
        WordCloudChartConfig: lambda current: map_word_cloud_config(current),
        WorldMapChartConfig: lambda current: map_world_map_config(current),
        BoxPlotChartConfig: lambda current: map_box_plot_config(current),
        BubbleChartConfig: lambda current: map_bubble_config(current),
    }
    mapper = mapper_by_type.get(type(config))
    if mapper is None:
        raise ValueError(f"Unsupported config type: {type(config)}")
    return mapper(config)


def _add_adhoc_filters(
    form_data: Dict[str, Any], filters: list[ChartFilterConfig] | None
) -> None:
    """Add adhoc filters to form_data if any are specified."""
    for filter_config in filters or []:
        if (
            isinstance(filter_config, TimeFilterConfig)
            and filter_config.time_grain is not None
        ):
            form_data["time_grain_sqla"] = filter_config.time_grain
    if adhoc_filters := chart_filters_to_adhoc_filters(filters):
        form_data["adhoc_filters"] = adhoc_filters


def chart_filters_to_adhoc_filters(
    filters: list[ChartFilterConfig] | None,
) -> list[Dict[str, Any]]:
    """Convert typed chart filter configs into Superset adhoc filter payloads."""
    if not filters:
        return []

    adhoc_filters: list[Dict[str, Any]] = []
    for filter_config in filters:
        if filter_config is None:
            continue
        if isinstance(filter_config, TimeFilterConfig):
            adhoc_filters.append(
                {
                    "clause": "WHERE",
                    "expressionType": "SIMPLE",
                    "subject": filter_config.column,
                    "operator": "TEMPORAL_RANGE",
                    "comparator": filter_config.time_range,
                }
            )
            continue

        comparator = getattr(filter_config, "value", None)
        if (
            isinstance(filter_config, (RangeFilterConfig, MetricFilterConfig))
            and filter_config.op == "BETWEEN"
        ):
            range_value = filter_config.value
            if isinstance(range_value, tuple):
                comparator = [range_value[0], range_value[1]]
            elif isinstance(range_value, list):
                comparator = range_value

        adhoc_filters.append(
            {
                "clause": (
                    "HAVING"
                    if isinstance(filter_config, MetricFilterConfig)
                    else "WHERE"
                ),
                "expressionType": "SIMPLE",
                "subject": filter_config.column,
                "operator": map_filter_operator(filter_config.op),
                "comparator": comparator,
            }
        )

    return adhoc_filters


def adhoc_filters_to_query_filters(
    adhoc_filters: list[Dict[str, Any]],
) -> list[Dict[str, Any]]:
    """Convert adhoc filter format to QueryObject filter format.

    Adhoc filters use ``{subject, operator, comparator}`` keys while
    ``QueryContextFactory`` expects ``{col, op, val}`` (QueryObjectFilterClause).
    Metric filters are preserved as metric labels so Superset can route them into
    HAVING clauses during query construction.
    """
    result: list[Dict[str, Any]] = []
    for f in adhoc_filters:
        if f.get("expressionType") == "SIMPLE":
            result.append(
                {
                    "col": f.get("subject"),
                    "op": f.get("operator"),
                    "val": f.get("comparator"),
                }
            )
    return result


def map_table_config(config: TableChartConfig) -> Dict[str, Any]:
    """Map table chart config to form_data with defensive validation."""
    # Early validation to prevent empty charts
    if not config.columns:
        raise ValueError("Table chart must have at least one column")

    # Use the viz_type from config (defaults to "table", can be "ag-grid-table")
    form_data: Dict[str, Any] = {
        "viz_type": config.viz_type,
    }

    # When query_mode is explicitly set to "raw", force raw mode for all columns.
    # Aggregate settings on individual columns are ignored in this case.
    if config.query_mode == "raw":
        column_names = [col.name for col in config.columns]
        form_data.update(
            {
                "all_columns": column_names,
                "columns": column_names,
                "query_mode": "raw",
                "include_time": False,
                "order_desc": True,
            }
        )
    else:
        # Auto-detect or explicit "aggregate": separate columns with aggregates
        # from raw columns and build the appropriate form_data.
        raw_columns = []
        aggregated_metrics = []

        for col in config.columns:
            if col.is_metric:
                # Saved metric or column with aggregation - treat as metric
                aggregated_metrics.append(create_metric_object(col))
            else:
                # No aggregation - treat as raw column
                raw_columns.append(col.name)

        # Final validation - ensure we have some data to display
        if not raw_columns and not aggregated_metrics:
            raise ValueError(
                "Table chart configuration resulted in no displayable columns"
            )

        # Handle raw columns (no aggregation)
        if raw_columns and not aggregated_metrics:
            # Pure raw columns - show individual rows
            # Include both "all_columns" (Superset table viz) and "columns"
            # (QueryContextFactory validation) to avoid "Empty query?" errors
            form_data.update(
                {
                    "all_columns": raw_columns,
                    "columns": raw_columns,
                    "query_mode": "raw",
                    "include_time": False,
                    "order_desc": True,
                }
            )

        # Handle aggregated columns only
        elif aggregated_metrics and not raw_columns:
            # Pure aggregation - show totals
            form_data.update(
                {
                    "metrics": aggregated_metrics,
                    "query_mode": "aggregate",
                }
            )

        # Handle mixed columns (raw + aggregated)
        else:
            # Mixed mode - group by raw columns, aggregate metrics
            form_data.update(
                {
                    "all_columns": raw_columns,
                    "metrics": aggregated_metrics,
                    "groupby": raw_columns,
                    "query_mode": "aggregate",
                }
            )

    _add_adhoc_filters(form_data, config.filters)

    if config.sort_by:
        form_data["order_by_cols"] = config.sort_by

    form_data["row_limit"] = config.row_limit

    return form_data


def create_metric_object(col: ColumnRef) -> Dict[str, Any] | str:
    """Create a metric object for a column with enhanced validation.

    For saved metrics, returns the metric name as a plain string which
    Superset's query engine resolves via its metrics_by_name lookup.
    For ad-hoc metrics, returns a SIMPLE expression dict.
    """
    if col.saved_metric:
        return col.name

    # Ensure aggregate is valid - default to SUM if not specified or invalid
    valid_aggregates = {
        "SUM",
        "COUNT",
        "AVG",
        "MIN",
        "MAX",
        "COUNT_DISTINCT",
        "STDDEV",
        "VAR",
        "MEDIAN",
        "PERCENTILE",
    }
    aggregate = col.aggregate or "SUM"

    # Validate aggregate function (final safety check)
    if aggregate.upper() not in valid_aggregates:
        aggregate = "SUM"  # Safe fallback

    return {
        "aggregate": aggregate.upper(),
        "column": {
            "column_name": col.name,
        },
        "expressionType": "SIMPLE",
        "label": col.label or f"{aggregate.upper()}({col.name})",
        "optionName": f"metric_{col.name}",
        "sqlExpression": None,
        "hasCustomLabel": bool(col.label),
        "datasourceWarning": False,
    }


def add_axis_config(form_data: Dict[str, Any], config: XYChartConfig) -> None:
    """Add axis configurations to form_data."""
    if config.x_axis:
        if config.x_axis.title:
            form_data["x_axis_title"] = config.x_axis.title
        if config.x_axis.format:
            form_data["x_axis_format"] = config.x_axis.format

    if config.y_axis:
        if config.y_axis.title:
            form_data["y_axis_title"] = config.y_axis.title
        if config.y_axis.format:
            form_data["y_axis_format"] = config.y_axis.format
        if config.y_axis.scale == "log":
            form_data["y_axis_scale"] = "log"


def add_legend_config(form_data: Dict[str, Any], config: XYChartConfig) -> None:
    """Add legend configuration to form_data."""
    if config.legend:
        if not config.legend.show:
            form_data["show_legend"] = False
        if config.legend.position:
            form_data["legend_orientation"] = config.legend.position


def add_orientation_config(form_data: Dict[str, Any], config: XYChartConfig) -> None:
    """Add orientation configuration to form_data for bar charts.

    Only applies when kind='bar' and an explicit orientation is set.
    When orientation is None (the default), Superset uses its own default
    (vertical bars).
    """
    if config.kind == "bar" and config.orientation:
        form_data["orientation"] = config.orientation


def configure_temporal_handling(
    form_data: Dict[str, Any],
    x_is_temporal: bool,
    time_grain: str | None,
) -> None:
    """Configure form_data based on whether x-axis column is temporal.

    For temporal columns, enables standard time series handling.
    For non-temporal columns (e.g., BIGINT year), disables DATE_TRUNC
    by setting categorical sorting options.

    Stores any warnings in ``form_data["_mcp_warnings"]``.
    """
    if x_is_temporal:
        form_data["granularity_sqla"] = form_data.get("x_axis")
        if time_grain:
            form_data["time_grain_sqla"] = time_grain
    else:
        # Non-temporal column - disable temporal handling to prevent DATE_TRUNC
        form_data["x_axis_sort_series_type"] = "name"
        form_data["x_axis_sort_series_ascending"] = True
        form_data["time_grain_sqla"] = None
        form_data["granularity_sqla"] = None
        if time_grain:
            form_data.setdefault("_mcp_warnings", []).append(
                f"time_grain='{time_grain}' was ignored because the x-axis "
                f"column is not a temporal type. time_grain only applies to "
                f"DATE/DATETIME/TIMESTAMP columns."
            )


def _ensure_temporal_adhoc_filter(form_data: Dict[str, Any], column: str) -> None:
    """Ensure a TEMPORAL_RANGE adhoc filter exists for the given column.

    Mirrors the Explore UI behavior: when a temporal column is set as
    the x-axis, a TEMPORAL_RANGE filter must be present so dashboard
    time-range filters can bind to it.  Without this filter, Explore
    shows a warning dialog asking the user to add it manually.
    """
    existing = form_data.get("adhoc_filters", [])
    if any(
        f.get("operator") == FilterOperator.TEMPORAL_RANGE.value
        and f.get("subject") == column
        for f in existing
    ):
        return
    existing.append(
        {
            "clause": "WHERE",
            "expressionType": "SIMPLE",
            "subject": column,
            "operator": FilterOperator.TEMPORAL_RANGE.value,
            "comparator": NO_TIME_RANGE,
        }
    )
    form_data["adhoc_filters"] = existing


def _resolve_default_x_axis(
    config: XYChartConfig, dataset_id: int | str | None
) -> XYChartConfig:
    """Resolve x-axis to the dataset's main_dttm_col when x is omitted."""
    if config.x is not None:
        return config

    if not dataset_id:
        raise ValueError("x-axis column is required when dataset_id is not provided")
    from superset.daos.dataset import DatasetDAO

    if isinstance(dataset_id, int) or (
        isinstance(dataset_id, str) and dataset_id.isdigit()
    ):
        dataset = DatasetDAO.find_by_id(int(dataset_id))
    else:
        dataset = DatasetDAO.find_by_id(dataset_id, id_column="uuid")

    if not dataset or not dataset.main_dttm_col:
        raise ValueError(
            "x-axis column is required: dataset has no primary datetime "
            "column (main_dttm_col). Please specify the x-axis column "
            "explicitly."
        )
    from superset.mcp_service.chart.schemas import ColumnRef

    return config.model_copy(update={"x": ColumnRef(name=dataset.main_dttm_col)})


def map_xy_config(
    config: XYChartConfig, dataset_id: int | str | None = None
) -> Dict[str, Any]:
    """Map XY chart config to form_data with defensive validation."""
    # Early validation to prevent empty charts
    if not config.y:
        raise ValueError("XY chart must have at least one Y-axis metric")

    # Resolve x-axis default: use dataset's main_dttm_col when x is omitted
    config = _resolve_default_x_axis(config, dataset_id)
    assert config.x is not None  # _resolve_default_x_axis guarantees x is set

    # Check if x-axis column is truly temporal (based on actual SQL type)
    x_is_temporal = is_column_truly_temporal(config.x.name, dataset_id)

    # Map chart kind to viz_type - always use the same viz types
    # The temporal vs non-temporal handling is done via form_data configuration
    viz_type_map = {
        "line": "echarts_timeseries_line",
        "bar": "echarts_timeseries_bar",
        "area": "echarts_area",
        "scatter": "echarts_timeseries_scatter",
    }

    if not x_is_temporal:
        logger.info(
            "X-axis column '%s' is not temporal (dataset_id=%s), "
            "configuring as categorical dimension",
            config.x.name,
            dataset_id,
        )

    # Convert Y columns to metrics with validation
    metrics = []
    for col in config.y:
        if not col.name.strip():  # Validate column name is not empty
            raise ValueError("Y-axis column name cannot be empty")
        metrics.append(create_metric_object(col))

    # Final validation - ensure we have metrics to display
    if not metrics:
        raise ValueError("XY chart configuration resulted in no displayable metrics")

    form_data: Dict[str, Any] = {
        "viz_type": viz_type_map.get(config.kind, "echarts_timeseries_line"),
        "metrics": metrics,
        "x_axis": config.x.name,
    }

    # Configure temporal handling based on whether column is truly temporal
    configure_temporal_handling(form_data, x_is_temporal, config.time_grain)

    # Only add groupby columns that differ from x_axis to avoid
    # "Duplicate column/metric labels" errors in Superset.
    if config.group_by:
        groupby_columns = [c.name for c in config.group_by if c.name != config.x.name]
        if groupby_columns:
            form_data["groupby"] = groupby_columns

    _add_adhoc_filters(form_data, config.filters)

    form_data["row_limit"] = config.row_limit

    # Add stacking configuration
    if getattr(config, "stacked", False):
        form_data["stack"] = "Stack"

    # Add configurations
    add_axis_config(form_data, config)
    add_legend_config(form_data, config)
    add_orientation_config(form_data, config)

    return form_data


def map_pie_config(config: PieChartConfig) -> Dict[str, Any]:
    """Map pie chart config to Superset form_data."""
    metric = create_metric_object(config.metric)

    form_data: Dict[str, Any] = {
        "viz_type": "pie",
        "groupby": [config.dimension.name],
        "metric": metric,
        "color_scheme": "supersetColors",
        "show_labels": config.show_labels,
        "show_legend": config.show_legend,
        "label_type": config.label_type,
        "number_format": config.number_format,
        "sort_by_metric": config.sort_by_metric,
        "row_limit": config.row_limit,
        "donut": config.donut,
        "show_total": config.show_total,
        "labels_outside": config.labels_outside,
        "outerRadius": config.outer_radius,
        "innerRadius": config.inner_radius,
        "date_format": "smart_date",
    }

    _add_adhoc_filters(form_data, config.filters)

    return form_data


def map_handlebars_config(config: HandlebarsChartConfig) -> Dict[str, Any]:
    """Map handlebars chart config to Superset form_data."""
    form_data: Dict[str, Any] = {
        "viz_type": "handlebars",
        "handlebars_template": config.handlebars_template,
        "row_limit": config.row_limit,
        "order_desc": config.order_desc,
    }

    if config.style_template:
        form_data["styleTemplate"] = config.style_template

    if config.query_mode == "raw":
        form_data["query_mode"] = "raw"
        if config.columns:
            form_data["all_columns"] = [col.name for col in config.columns]
    else:
        form_data["query_mode"] = "aggregate"
        if config.groupby:
            form_data["groupby"] = [col.name for col in config.groupby]
        if config.metrics:
            form_data["metrics"] = [create_metric_object(col) for col in config.metrics]
    _add_adhoc_filters(form_data, config.filters)

    return form_data


def map_funnel_config(config: FunnelChartConfig) -> Dict[str, Any]:
    """Map funnel chart config to Superset form_data."""
    form_data: Dict[str, Any] = {
        "viz_type": "funnel",
        "groupby": [config.dimension.name],
        "metric": create_metric_object(config.metric),
        "percent_calculation_type": config.percent_calculation_type,
        "show_labels": config.show_labels,
        "show_legend": config.show_legend,
        "show_tooltip_labels": config.show_tooltip_labels,
        "sort_by_metric": config.sort_by_metric,
        "number_format": config.number_format,
        "color_scheme": config.color_scheme,
        "row_limit": config.row_limit,
    }

    _add_adhoc_filters(form_data, config.filters)
    return form_data


def map_big_number_config(config: BigNumberChartConfig) -> Dict[str, Any]:
    """Map big number chart config to Superset form_data."""
    form_data: Dict[str, Any] = {
        "viz_type": "big_number" if config.show_trend_line else "big_number_total",
        "metric": create_metric_object(config.metric),
        "header_font_size": config.header_font_size,
        "subheader_font_size": config.subheader_font_size,
        "time_format": config.time_format,
        "y_axis_format": config.number_format,
    }

    if config.show_trend_line and config.x:
        form_data["show_trend_line"] = True
        form_data["x_axis"] = config.x.name
        form_data["start_y_axis_at_zero"] = config.start_y_axis_at_zero
        if config.time_grain:
            form_data["time_grain_sqla"] = config.time_grain

    _add_adhoc_filters(form_data, config.filters)
    return form_data


def map_gauge_config(config: GaugeChartConfig) -> Dict[str, Any]:
    """Map gauge chart config to Superset form_data."""
    form_data: Dict[str, Any] = {
        "viz_type": "gauge_chart",
        "metric": create_metric_object(config.metric),
        "row_limit": config.row_limit,
        "start_angle": config.start_angle,
        "end_angle": config.end_angle,
        "show_pointer": config.show_pointer,
        "show_progress": config.show_progress,
        "show_axis_tick": config.show_axis_tick,
        "show_split_line": config.show_split_line,
        "split_number": config.split_number,
        "font_size": config.font_size,
        "value_formatter": config.value_formatter,
        "overlap": config.overlap,
        "round_cap": config.round_cap,
        "color_scheme": config.color_scheme,
        "number_format": config.number_format,
    }

    if config.dimension:
        form_data["groupby"] = [config.dimension.name]

    _add_adhoc_filters(form_data, config.filters)
    return form_data


def map_heatmap_config(config: HeatmapChartConfig) -> Dict[str, Any]:
    """Map heatmap chart config to Superset form_data."""
    form_data: Dict[str, Any] = {
        "viz_type": "heatmap_v2",
        "x_axis": config.x.name,
        "groupby": config.y.name,
        "metric": create_metric_object(config.metric),
        "normalize_across": config.normalize_across,
        "show_legend": config.show_legend,
        "show_percentage": config.show_percentage,
        "show_values": config.show_values,
        "sort_x_axis": config.sort_x_axis,
        "sort_y_axis": config.sort_y_axis,
        "linear_color_scheme": config.linear_color_scheme,
        "row_limit": config.row_limit,
        "value_bounds": config.value_bounds,
        "x_axis_time_format": config.x_axis_time_format,
        "y_axis_format": config.number_format,
    }

    if config.time_grain:
        form_data["time_grain_sqla"] = config.time_grain

    _add_adhoc_filters(form_data, config.filters)
    return form_data


def map_treemap_config(config: TreemapChartConfig) -> Dict[str, Any]:
    """Map treemap chart config to Superset form_data."""
    form_data: Dict[str, Any] = {
        "viz_type": "treemap_v2",
        "groupby": [dimension.name for dimension in config.dimensions],
        "metric": create_metric_object(config.metric),
        "color_scheme": config.color_scheme,
        "label_type": config.label_type,
        "number_format": config.number_format,
        "row_limit": config.row_limit,
        "show_labels": config.show_labels,
        "show_upper_labels": config.show_upper_labels,
    }

    _add_adhoc_filters(form_data, config.filters)
    return form_data


def map_sunburst_config(config: SunburstChartConfig) -> Dict[str, Any]:
    """Map sunburst chart config to Superset form_data."""
    form_data: Dict[str, Any] = {
        "viz_type": "sunburst_v2",
        "columns": [dimension.name for dimension in config.dimensions],
        "metric": create_metric_object(config.metric),
        "label_type": config.label_type,
        "linear_color_scheme": config.linear_color_scheme,
        "number_format": config.number_format,
        "row_limit": config.row_limit,
        "show_labels": config.show_labels,
        "show_labels_threshold": config.show_labels_threshold,
    }

    _add_adhoc_filters(form_data, config.filters)
    return form_data


def map_sankey_config(config: SankeyChartConfig) -> Dict[str, Any]:
    """Map sankey chart config to Superset form_data."""
    form_data: Dict[str, Any] = {
        "viz_type": "sankey_v2",
        "source": config.source.name,
        "target": config.target.name,
        "metric": create_metric_object(config.metric),
        "row_limit": config.row_limit,
        "color_scheme": config.color_scheme,
    }

    _add_adhoc_filters(form_data, config.filters)
    return form_data


def map_word_cloud_config(config: WordCloudChartConfig) -> Dict[str, Any]:
    """Map word cloud chart config to Superset form_data."""
    form_data: Dict[str, Any] = {
        "viz_type": "word_cloud",
        "series": config.series.name,
        "metric": create_metric_object(config.metric),
        "rotation": config.rotation,
        "size_from": config.size_from,
        "size_to": config.size_to,
        "row_limit": config.row_limit,
        "color_scheme": config.color_scheme,
    }

    _add_adhoc_filters(form_data, config.filters)
    return form_data


def map_world_map_config(config: WorldMapChartConfig) -> Dict[str, Any]:
    """Map world map chart config to Superset form_data."""
    form_data: Dict[str, Any] = {
        "viz_type": "world_map",
        "entity": config.entity.name,
        "metric": create_metric_object(config.metric),
        "country_fieldtype": config.country_fieldtype,
        "show_bubbles": config.show_bubbles,
        "row_limit": config.row_limit,
    }

    if config.secondary_metric:
        form_data["secondary_metric"] = create_metric_object(config.secondary_metric)

    _add_adhoc_filters(form_data, config.filters)
    return form_data


def map_box_plot_config(config: BoxPlotChartConfig) -> Dict[str, Any]:
    """Map box plot chart config to Superset form_data."""
    form_data: Dict[str, Any] = {
        "viz_type": "box_plot",
        "columns": [config.x.name],
        "metrics": [create_metric_object(config.metric)],
        "number_format": config.number_format,
        "row_limit": config.row_limit,
        "whiskerOptions": config.whisker_options,
    }

    if config.group_by:
        form_data["groupby"] = [dimension.name for dimension in config.group_by]
    if config.time_grain:
        form_data["time_grain_sqla"] = config.time_grain

    _add_adhoc_filters(form_data, config.filters)
    return form_data


def map_bubble_config(config: BubbleChartConfig) -> Dict[str, Any]:
    """Map bubble chart config to Superset form_data."""
    form_data: Dict[str, Any] = {
        "viz_type": "bubble_v2",
        "x": create_metric_object(config.x),
        "y": create_metric_object(config.y),
        "size": create_metric_object(config.size),
        "series": config.series.name,
        "show_legend": config.show_legend,
        "legendOrientation": config.legend_orientation,
        "legendType": config.legend_type,
        "max_bubble_size": str(config.max_bubble_size),
        "opacity": config.opacity,
        "order_desc": config.order_desc,
        "row_limit": config.row_limit,
        "tooltipSizeFormat": config.tooltip_size_format,
        "truncateXAxis": config.truncate_x_axis,
        "xAxisFormat": config.x_axis_format,
        "y_axis_format": config.y_axis_format,
        "color_scheme": config.color_scheme,
        "y_axis_bounds": [None, None],
    }

    if config.entity:
        form_data["entity"] = config.entity.name

    _add_adhoc_filters(form_data, config.filters)
    return form_data


def map_pivot_table_config(config: PivotTableChartConfig) -> Dict[str, Any]:
    """Map pivot table config to Superset form_data."""
    if not config.rows:
        raise ValueError("Pivot table must have at least one row grouping column")
    if not config.metrics:
        raise ValueError("Pivot table must have at least one metric")

    metrics = [create_metric_object(col) for col in config.metrics]

    form_data: Dict[str, Any] = {
        "viz_type": "pivot_table_v2",
        "groupbyRows": [col.name for col in config.rows],
        "groupbyColumns": [col.name for col in config.columns]
        if config.columns
        else [],
        "metrics": metrics,
        "aggregateFunction": config.aggregate_function,
        "rowTotals": config.show_row_totals,
        "colTotals": config.show_column_totals,
        "transposePivot": config.transpose,
        "combineMetric": config.combine_metric,
        "valueFormat": config.value_format,
        "metricsLayout": "COLUMNS",
        "rowOrder": "key_a_to_z",
        "colOrder": "key_a_to_z",
        "row_limit": config.row_limit,
    }

    _add_adhoc_filters(form_data, config.filters)

    return form_data


_MIXED_SERIES_TYPE_MAP = {
    "line": "line",
    "bar": "bar",
    "area": "line",  # area uses line type with area=True
    "scatter": "scatter",
}


def _apply_axis_to_form_data(
    form_data: Dict[str, Any],
    axis_config: Any,
    title_key: str,
    format_key: str,
    log_key: str | None = None,
) -> None:
    """Apply a single axis configuration to form_data."""
    if not axis_config:
        return
    if axis_config.title:
        form_data[title_key] = axis_config.title
    if axis_config.format:
        form_data[format_key] = axis_config.format
    if log_key and axis_config.scale == "log":
        form_data[log_key] = True


def _add_mixed_axis_config(
    form_data: Dict[str, Any],
    config: MixedTimeseriesChartConfig,
) -> None:
    """Add axis configurations to mixed timeseries form_data."""
    _apply_axis_to_form_data(
        form_data, config.x_axis, "xAxisTitle", "x_axis_time_format"
    )
    _apply_axis_to_form_data(
        form_data, config.y_axis, "yAxisTitle", "y_axis_format", "logAxis"
    )
    _apply_axis_to_form_data(
        form_data,
        config.y_axis_secondary,
        "yAxisTitleSecondary",
        "y_axis_format_secondary",
        "logAxisSecondary",
    )


def map_mixed_timeseries_config(
    config: MixedTimeseriesChartConfig,
    dataset_id: int | str | None = None,
) -> Dict[str, Any]:
    """Map mixed timeseries chart config to Superset form_data."""
    if not config.y:
        raise ValueError("Mixed timeseries must have at least one primary metric")
    if not config.y_secondary:
        raise ValueError("Mixed timeseries must have at least one secondary metric")

    # Check if x-axis column is truly temporal
    x_is_temporal = is_column_truly_temporal(config.x.name, dataset_id)

    form_data: Dict[str, Any] = {
        "viz_type": "mixed_timeseries",
        "x_axis": config.x.name,
        # Query A
        "metrics": [create_metric_object(col) for col in config.y],
        "seriesType": _MIXED_SERIES_TYPE_MAP.get(config.primary_kind, "line"),
        "area": config.primary_kind == "area",
        "yAxisIndex": 0,
        # Query B
        "metrics_b": [create_metric_object(col) for col in config.y_secondary],
        "seriesTypeB": _MIXED_SERIES_TYPE_MAP.get(config.secondary_kind, "bar"),
        "areaB": config.secondary_kind == "area",
        "yAxisIndexB": 1,
        # Display
        "show_legend": config.show_legend,
        "zoomable": True,
        "rich_tooltip": True,
    }

    # Configure temporal handling
    configure_temporal_handling(form_data, x_is_temporal, config.time_grain)

    # Primary groupby (Query A)
    if config.group_by:
        groupby = [c.name for c in config.group_by if c.name != config.x.name]
        if groupby:
            form_data["groupby"] = groupby

    # Secondary groupby (Query B)
    if config.group_by_secondary:
        groupby_b = [
            c.name for c in config.group_by_secondary if c.name != config.x.name
        ]
        if groupby_b:
            form_data["groupby_b"] = groupby_b

    form_data["row_limit"] = config.row_limit

    _add_mixed_axis_config(form_data, config)

    _add_adhoc_filters(form_data, config.filters)

    return form_data


def map_filter_operator(op: str) -> str:
    """Map filter operator to Superset format."""
    operator_map = {
        "=": "==",
        ">": ">",
        "<": "<",
        ">=": ">=",
        "<=": "<=",
        "!=": "!=",
        "LIKE": "LIKE",
        "ILIKE": "ILIKE",
        "NOT LIKE": "NOT LIKE",
        "IN": "IN",
        "NOT IN": "NOT IN",
        "BETWEEN": "BETWEEN",
        "IS NULL": "IS NULL",
        "IS NOT NULL": "IS NOT NULL",
        "TEMPORAL_RANGE": "TEMPORAL_RANGE",
    }
    return operator_map.get(op, op)


def _humanize_column(col: ColumnRef) -> str:
    """Return a human-readable label for a column reference."""
    if col.label:
        return col.label
    name = col.name.replace("_", " ").title()
    if col.saved_metric:
        return name
    if col.aggregate:
        return f"{col.aggregate.capitalize()}({name})"
    return name


def _summarize_filters(
    filters: list[ChartFilterConfig] | None,
) -> str | None:
    """Extract a short context string from filter configs."""
    if not filters:
        return None
    parts: list[str] = []
    for f in filters[:2]:
        col = getattr(f, "column", "")
        if isinstance(f, TimeFilterConfig):
            time_value = f.time_range
            parts.append(f"{str(col).replace('_', ' ').title()} {time_value}")
            continue
        if isinstance(f, MetricFilterConfig):
            metric_value: Any = getattr(f, "value", "")
            if isinstance(metric_value, (list, tuple)):
                metric_value = ", ".join(str(v) for v in list(metric_value)[:3])
            parts.append(f"{str(col)} {f.op} {metric_value}")
            continue
        if isinstance(f, NullFilterConfig):
            parts.append(f"{str(col).replace('_', ' ').title()} {f.op}")
            continue
        filter_value: Any = getattr(f, "value", "")
        if isinstance(filter_value, (list, tuple)):
            filter_value = ", ".join(str(v) for v in list(filter_value)[:3])
        parts.append(f"{str(col).replace('_', ' ').title()} {filter_value}")
    return ", ".join(parts) if parts else None


def _truncate(name: str, max_length: int = 60) -> str:
    """Truncate to *max_length*, preserving the en-dash context portion."""
    if len(name) <= max_length:
        return name
    if " \u2013 " in name:
        what, _context = name.split(" \u2013 ", 1)
        if len(what) <= max_length:
            return what
    return name[: max_length - 1] + "\u2026"


def _table_chart_what(config: TableChartConfig, dataset_name: str | None) -> str:
    """Build the descriptive fragment for a table chart."""
    has_agg = any(col.is_metric for col in config.columns)
    if has_agg:
        metrics = [col for col in config.columns if col.is_metric]
        what = ", ".join(_humanize_column(m) for m in metrics[:2])
        return f"{what} Summary"
    if dataset_name:
        return f"{dataset_name} Records"
    cols = ", ".join(_humanize_column(c) for c in config.columns[:3])
    return f"{cols} Table"


def _xy_chart_what(config: XYChartConfig) -> str:
    """Build the descriptive fragment for an XY chart."""
    primary_metric = _humanize_column(config.y[0]) if config.y else "Value"
    dimension = _humanize_column(config.x) if config.x else "Dimension"

    if config.kind in ("line", "area") and not config.group_by:
        return f"{primary_metric} Over Time"
    if config.group_by:
        group_label = _humanize_column(config.group_by[0])
        return f"{primary_metric} by {group_label}"
    if config.kind == "scatter":
        return f"{primary_metric} vs {dimension}"
    return f"{primary_metric} by {dimension}"


_GRAIN_MAP: dict[str, str] = {
    "PT1H": "Hourly",
    "P1D": "Daily",
    "P1W": "Weekly",
    "P1M": "Monthly",
    "P3M": "Quarterly",
    "P1Y": "Yearly",
}


def _xy_chart_context(config: XYChartConfig) -> str | None:
    """Build context (time grain / filters) for an XY chart name."""
    parts: list[str] = []
    if config.time_grain:
        grain_val = (
            config.time_grain.value
            if hasattr(config.time_grain, "value")
            else str(config.time_grain)
        )
        grain_str = _GRAIN_MAP.get(grain_val, grain_val)
        parts.append(grain_str)
    if filter_ctx := _summarize_filters(config.filters):
        parts.append(filter_ctx)
    return ", ".join(parts) if parts else None


def _pie_chart_what(config: PieChartConfig) -> str:
    """Build the 'what' portion for a pie chart name."""
    dim = config.dimension.name
    metric_label = config.metric.label or config.metric.name
    return f"{dim} by {metric_label}"


def _pivot_table_what(config: PivotTableChartConfig) -> str:
    """Build the 'what' portion for a pivot table chart name."""
    row_names = ", ".join(r.name for r in config.rows)
    return f"Pivot Table \u2013 {row_names}"


def _mixed_timeseries_what(config: MixedTimeseriesChartConfig) -> str:
    """Build the 'what' portion for a mixed timeseries chart name."""
    primary = config.y[0].label or config.y[0].name if config.y else "primary"
    secondary = (
        config.y_secondary[0].label or config.y_secondary[0].name
        if config.y_secondary
        else "secondary"
    )
    return f"{primary} + {secondary}"


def _handlebars_chart_what(config: HandlebarsChartConfig) -> str:
    """Build the 'what' portion for a handlebars chart name.

    Uses parentheses instead of en-dash to avoid collision with
    ``generate_chart_name``'s ``\u2013`` context separator.
    """
    if config.query_mode == "raw" and config.columns:
        cols = ", ".join(col.name for col in config.columns[:3])
        return f"Handlebars ({cols})"
    elif config.metrics:
        metrics = ", ".join(col.name for col in config.metrics[:3])
        return f"Handlebars ({metrics})"
    return "Handlebars Chart"


def _funnel_chart_what(config: FunnelChartConfig) -> str:
    """Build the descriptive fragment for a funnel chart."""
    return f"{config.dimension.name} Funnel"


def _big_number_chart_what(config: BigNumberChartConfig) -> str:
    """Build the descriptive fragment for a big number chart."""
    metric_label = config.metric.label or config.metric.name
    return (
        f"{metric_label} KPI" if not config.show_trend_line else f"{metric_label} Trend"
    )


def _gauge_chart_what(config: GaugeChartConfig) -> str:
    """Build the descriptive fragment for a gauge chart."""
    metric_label = config.metric.label or config.metric.name
    if config.dimension:
        return f"{metric_label} by {config.dimension.name}"
    return f"{metric_label} Gauge"


def _heatmap_chart_what(config: HeatmapChartConfig) -> str:
    """Build the descriptive fragment for a heatmap chart."""
    return f"{config.metric.name} Heatmap"


def _treemap_chart_what(config: TreemapChartConfig) -> str:
    """Build the descriptive fragment for a treemap chart."""
    root_dimension = config.dimensions[0].name
    return f"{config.metric.name} by {root_dimension}"


def _sunburst_chart_what(config: SunburstChartConfig) -> str:
    """Build the descriptive fragment for a sunburst chart."""
    root_dimension = config.dimensions[0].name
    return f"{config.metric.name} Sunburst by {root_dimension}"


def _sankey_chart_what(config: SankeyChartConfig) -> str:
    """Build the descriptive fragment for a sankey chart."""
    return f"{config.source.name} to {config.target.name} Flow"


def _word_cloud_chart_what(config: WordCloudChartConfig) -> str:
    """Build the descriptive fragment for a word cloud chart."""
    return f"{config.series.name} Word Cloud"


def _world_map_chart_what(config: WorldMapChartConfig) -> str:
    """Build the descriptive fragment for a world map chart."""
    return f"{config.metric.name} by {config.entity.name}"


def _box_plot_chart_what(config: BoxPlotChartConfig) -> str:
    """Build the descriptive fragment for a box plot chart."""
    if config.group_by:
        return f"{config.metric.name} Distribution by {config.group_by[0].name}"
    return f"{config.metric.name} Distribution"


def _bubble_chart_what(config: BubbleChartConfig) -> str:
    """Build the descriptive fragment for a bubble chart."""
    return f"{config.x.name} vs {config.y.name}"


def _chart_name_parts(
    config: TableChartConfig
    | XYChartConfig
    | PieChartConfig
    | PivotTableChartConfig
    | MixedTimeseriesChartConfig
    | HandlebarsChartConfig
    | FunnelChartConfig
    | BigNumberChartConfig
    | GaugeChartConfig
    | HeatmapChartConfig
    | TreemapChartConfig
    | SunburstChartConfig
    | SankeyChartConfig
    | WordCloudChartConfig
    | WorldMapChartConfig
    | BoxPlotChartConfig
    | BubbleChartConfig,
    dataset_name: str | None = None,
) -> tuple[str, str | None] | None:
    """Return the title and optional context fragments for a chart config."""
    part_builders: dict[type[Any], Callable[[Any], tuple[str, str | None]]] = {
        TableChartConfig: lambda current: (
            _table_chart_what(current, dataset_name),
            _summarize_filters(current.filters),
        ),
        XYChartConfig: lambda current: (
            _xy_chart_what(current),
            _xy_chart_context(current),
        ),
        PieChartConfig: lambda current: (
            _pie_chart_what(current),
            _summarize_filters(current.filters),
        ),
        PivotTableChartConfig: lambda current: (
            _pivot_table_what(current),
            _summarize_filters(current.filters),
        ),
        MixedTimeseriesChartConfig: lambda current: (
            _mixed_timeseries_what(current),
            _summarize_filters(current.filters),
        ),
        HandlebarsChartConfig: lambda current: (
            _handlebars_chart_what(current),
            _summarize_filters(getattr(current, "filters", None)),
        ),
        FunnelChartConfig: lambda current: (
            _funnel_chart_what(current),
            _summarize_filters(current.filters),
        ),
        BigNumberChartConfig: lambda current: (
            _big_number_chart_what(current),
            _summarize_filters(current.filters),
        ),
        GaugeChartConfig: lambda current: (
            _gauge_chart_what(current),
            _summarize_filters(current.filters),
        ),
        HeatmapChartConfig: lambda current: (
            _heatmap_chart_what(current),
            _summarize_filters(current.filters),
        ),
        TreemapChartConfig: lambda current: (
            _treemap_chart_what(current),
            _summarize_filters(current.filters),
        ),
        SunburstChartConfig: lambda current: (
            _sunburst_chart_what(current),
            _summarize_filters(current.filters),
        ),
        SankeyChartConfig: lambda current: (
            _sankey_chart_what(current),
            _summarize_filters(current.filters),
        ),
        WordCloudChartConfig: lambda current: (
            _word_cloud_chart_what(current),
            _summarize_filters(current.filters),
        ),
        WorldMapChartConfig: lambda current: (
            _world_map_chart_what(current),
            _summarize_filters(current.filters),
        ),
        BoxPlotChartConfig: lambda current: (
            _box_plot_chart_what(current),
            _summarize_filters(current.filters),
        ),
        BubbleChartConfig: lambda current: (
            _bubble_chart_what(current),
            _summarize_filters(current.filters),
        ),
    }
    builder = part_builders.get(type(config))
    return builder(config) if builder else None


def generate_chart_name(
    config: TableChartConfig
    | XYChartConfig
    | PieChartConfig
    | PivotTableChartConfig
    | MixedTimeseriesChartConfig
    | HandlebarsChartConfig
    | FunnelChartConfig
    | BigNumberChartConfig
    | GaugeChartConfig
    | HeatmapChartConfig
    | TreemapChartConfig
    | SunburstChartConfig
    | SankeyChartConfig
    | WordCloudChartConfig
    | WorldMapChartConfig
    | BoxPlotChartConfig
    | BubbleChartConfig,
    dataset_name: str | None = None,
) -> str:
    """Generate a descriptive chart name following a standard format.

    Format conventions (by chart type):
      Aggregated (bar/scatter with group_by): [Metric] by [Dimension]
      Time-series (line/area, no group_by):   [Metric] Over Time
      Table (no aggregates):                  [Dataset] Records
      Table (with aggregates):                [Metric] Summary
      Pie:                                    [Dimension] by [Metric]
      Pivot Table:                            Pivot Table – [Row1, Row2]
      Mixed Timeseries:                       [Primary] + [Secondary]
    An en-dash followed by context (filters / time grain) is appended
    when such information is available.
    """
    parts = _chart_name_parts(config, dataset_name=dataset_name)
    if parts is None:
        return "Chart"
    what, context = parts

    name = what
    if context:
        name = f"{what} \u2013 {context}"
    return _truncate(name)


def _resolve_viz_type(config: Any) -> str:
    """Resolve the Superset viz_type from a chart config object."""
    chart_type = getattr(config, "chart_type", "unknown")
    if chart_type == "xy":
        kind = getattr(config, "kind", "line")
        viz_type_map = {
            "line": "echarts_timeseries_line",
            "bar": "echarts_timeseries_bar",
            "area": "echarts_area",
            "scatter": "echarts_timeseries_scatter",
        }
        return viz_type_map.get(kind, "echarts_timeseries_line")
    elif chart_type == "table":
        return getattr(config, "viz_type", "table")
    elif chart_type == "big_number":
        return (
            "big_number"
            if getattr(config, "show_trend_line", False)
            else "big_number_total"
        )
    elif chart_type in STATIC_VIZ_TYPE_BY_CHART_TYPE:
        return STATIC_VIZ_TYPE_BY_CHART_TYPE[chart_type]
    return "unknown"


def analyze_chart_capabilities(chart: Any | None, config: Any) -> ChartCapabilities:
    """Analyze chart capabilities based on type and configuration."""
    if chart:
        viz_type = getattr(chart, "viz_type", "unknown")
    else:
        viz_type = _resolve_viz_type(config)

    # Determine interaction capabilities based on chart type
    interactive_types = [
        "echarts_timeseries_line",
        "echarts_timeseries_bar",
        "echarts_area",
        "echarts_timeseries_scatter",
        "deck_scatter",
        "deck_hex",
        "ag-grid-table",  # AG Grid tables are interactive
    ]

    supports_interaction = viz_type in interactive_types
    supports_drill_down = viz_type in ["table", "pivot_table_v2", "ag-grid-table"]
    supports_real_time = viz_type in [
        "echarts_timeseries_line",
        "echarts_timeseries_bar",
    ]

    # Determine optimal formats
    optimal_formats = ["url"]  # Always include static image
    if supports_interaction:
        optimal_formats.extend(["interactive", "vega_lite"])
    optimal_formats.extend(["ascii", "table"])

    # Classify data types
    data_types = []
    if hasattr(config, "x") and config.x:
        data_types.append("categorical" if not config.x.is_metric else "metric")
    if hasattr(config, "y") and config.y:
        data_types.extend(["metric"] * len(config.y))
    if "time" in viz_type or "timeseries" in viz_type:
        data_types.append("time_series")

    return ChartCapabilities(
        supports_interaction=supports_interaction,
        supports_real_time=supports_real_time,
        supports_drill_down=supports_drill_down,
        supports_export=True,  # All charts can be exported
        optimal_formats=optimal_formats,
        data_types=list(set(data_types)),
    )


def analyze_chart_semantics(chart: Any | None, config: Any) -> ChartSemantics:
    """Generate semantic understanding of the chart."""
    if chart:
        viz_type = getattr(chart, "viz_type", "unknown")
    else:
        viz_type = _resolve_viz_type(config)

    # Generate primary insight based on chart type
    insights_map = {
        "echarts_timeseries_line": "Shows trends and changes over time",
        "echarts_timeseries_bar": "Compares values across categories or time periods",
        "table": "Displays detailed data in tabular format",
        "ag-grid-table": (
            "Interactive table with advanced features like column resizing, "
            "sorting, filtering, and server-side pagination"
        ),
        "pie": "Shows proportional relationships within a dataset",
        "echarts_area": "Emphasizes cumulative totals and part-to-whole relationships",
        "pivot_table_v2": (
            "Cross-tabulates data with rows, columns, and aggregated metrics "
            "for multi-dimensional analysis"
        ),
        "mixed_timeseries": (
            "Combines two different chart types on the same time axis "
            "for comparing related metrics with different scales"
        ),
        "handlebars": (
            "Renders data using a custom Handlebars HTML template for "
            "fully flexible layouts like KPI cards, leaderboards, and reports"
        ),
        "treemap_v2": "Shows hierarchical proportions with nested rectangular areas",
        "sunburst_v2": "Shows hierarchical proportions in concentric radial layers",
        "sankey_v2": "Shows how values flow between source and target categories",
        "word_cloud": "Highlights the most important terms or categories by size",
        "world_map": "Shows geographic variation across countries or regions",
        "box_plot": "Summarizes distributions, spread, and outliers across groups",
        "bubble_v2": "Compares two metrics and bubble size across grouped entities",
    }

    primary_insight = insights_map.get(
        viz_type, f"Visualizes data using {viz_type} format"
    )

    # Generate data story
    columns = []
    if hasattr(config, "x") and config.x:
        columns.append(config.x.name)
    if hasattr(config, "y") and config.y:
        columns.extend([col.name for col in config.y])

    if columns:
        ellipsis = "..." if len(columns) > 3 else ""
        data_story = (
            f"This {viz_type} chart analyzes {', '.join(columns[:3])}{ellipsis}"
        )
    else:
        data_story = "This chart provides insights into the selected dataset"

    # Generate recommended actions
    recommended_actions = [
        "Review data patterns and trends",
        "Consider filtering or drilling down for more detail",
        "Export chart for reporting or sharing",
    ]

    if viz_type in ["echarts_timeseries_line", "echarts_timeseries_bar"]:
        recommended_actions.append("Analyze seasonal patterns or cyclical trends")

    return ChartSemantics(
        primary_insight=primary_insight,
        data_story=data_story,
        recommended_actions=recommended_actions,
        anomalies=[],  # Would need actual data analysis to populate
        statistical_summary={},  # Would need actual data analysis to populate
    )
