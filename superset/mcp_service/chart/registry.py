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

"""Shared registry of typed MCP chart capabilities."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ChartTypeCapability:
    """Describes a typed chart type exposed through MCP."""

    chart_type: str
    label: str
    summary: str
    required_fields: tuple[str, ...]


CHART_TYPE_CAPABILITIES: dict[str, ChartTypeCapability] = {
    "xy": ChartTypeCapability(
        chart_type="xy",
        label="XY chart",
        summary="Line, bar, area, or scatter charts for time series and comparisons",
        required_fields=("x", "y"),
    ),
    "table": ChartTypeCapability(
        chart_type="table",
        label="Table chart",
        summary="Tabular data display with optional aggregated metrics",
        required_fields=("columns",),
    ),
    "pie": ChartTypeCapability(
        chart_type="pie",
        label="Pie chart",
        summary="Pie or donut chart for part-to-whole comparisons",
        required_fields=("dimension", "metric"),
    ),
    "pivot_table": ChartTypeCapability(
        chart_type="pivot_table",
        label="Pivot table",
        summary="Cross-tabulation with row groups, optional column groups, and metrics",
        required_fields=("rows", "metrics"),
    ),
    "mixed_timeseries": ChartTypeCapability(
        chart_type="mixed_timeseries",
        label="Mixed timeseries chart",
        summary="Dual-series time chart with separate primary and secondary metrics",
        required_fields=("x", "y", "y_secondary"),
    ),
    "handlebars": ChartTypeCapability(
        chart_type="handlebars",
        label="Handlebars chart",
        summary="Custom HTML template chart backed by aggregate or raw query results",
        required_fields=("handlebars_template",),
    ),
    "funnel": ChartTypeCapability(
        chart_type="funnel",
        label="Funnel chart",
        summary="Stage-based conversion funnel using one dimension and one metric",
        required_fields=("dimension", "metric"),
    ),
    "big_number": ChartTypeCapability(
        chart_type="big_number",
        label="Big number chart",
        summary="Single KPI with optional trendline over a temporal axis",
        required_fields=("metric",),
    ),
    "gauge": ChartTypeCapability(
        chart_type="gauge",
        label="Gauge chart",
        summary="Gauge-style KPI chart with optional category grouping",
        required_fields=("metric",),
    ),
    "heatmap": ChartTypeCapability(
        chart_type="heatmap",
        label="Heatmap chart",
        summary="Matrix heatmap comparing two dimensions with a metric",
        required_fields=("x", "y", "metric"),
    ),
    "treemap": ChartTypeCapability(
        chart_type="treemap",
        label="Treemap chart",
        summary="Hierarchical rectangles sized by a metric across nested dimensions",
        required_fields=("dimensions", "metric"),
    ),
    "sunburst": ChartTypeCapability(
        chart_type="sunburst",
        label="Sunburst chart",
        summary="Hierarchical radial chart sized by a metric across nested dimensions",
        required_fields=("dimensions", "metric"),
    ),
    "sankey": ChartTypeCapability(
        chart_type="sankey",
        label="Sankey chart",
        summary="Flow diagram connecting source and target categories with a metric",
        required_fields=("source", "target", "metric"),
    ),
    "word_cloud": ChartTypeCapability(
        chart_type="word_cloud",
        label="Word cloud chart",
        summary="Word cloud with a dimension sized by a metric",
        required_fields=("series", "metric"),
    ),
    "world_map": ChartTypeCapability(
        chart_type="world_map",
        label="World map chart",
        summary="Geographic map keyed by country/entity with a primary metric",
        required_fields=("entity", "metric"),
    ),
    "box_plot": ChartTypeCapability(
        chart_type="box_plot",
        label="Box plot chart",
        summary="Distribution summary across a dimension using a metric",
        required_fields=("x", "metric"),
    ),
    "bubble": ChartTypeCapability(
        chart_type="bubble",
        label="Bubble chart",
        summary="Bubble chart comparing x, y, and size metrics across categories",
        required_fields=("x", "y", "size", "series"),
    ),
}

SUPPORTED_TYPED_CHART_TYPES: tuple[str, ...] = tuple(CHART_TYPE_CAPABILITIES)

STATIC_VIZ_TYPE_BY_CHART_TYPE: dict[str, str] = {
    "pie": "pie",
    "pivot_table": "pivot_table_v2",
    "mixed_timeseries": "mixed_timeseries",
    "handlebars": "handlebars",
    "funnel": "funnel",
    "big_number": "big_number_total",
    "gauge": "gauge_chart",
    "heatmap": "heatmap_v2",
    "treemap": "treemap_v2",
    "sunburst": "sunburst_v2",
    "sankey": "sankey_v2",
    "word_cloud": "word_cloud",
    "world_map": "world_map",
    "box_plot": "box_plot",
    "bubble": "bubble_v2",
}
