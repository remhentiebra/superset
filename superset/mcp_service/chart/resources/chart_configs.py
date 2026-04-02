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
Chart resources for valid ChartConfig examples and templates
"""

import logging

from superset.mcp_service.app import mcp
from superset.mcp_service.auth import mcp_auth_hook

logger = logging.getLogger(__name__)


@mcp.resource("chart://configs")
@mcp_auth_hook
def get_chart_configs_resource() -> str:
    """
    Provide valid ChartConfig examples that match the exact schema.

    This resource gives LLMs access to:
    - Valid ChartConfig examples for XYChartConfig and TableChartConfig
    - Working configurations that pass schema validation
    - Examples with proper ColumnRef, FilterConfig, AxisConfig, and LegendConfig
    - Best practices for each chart type configuration
    """

    # XY chart examples covering all chart kinds and features
    xy_chart_configs = {
        "line_chart": {
            "description": "Line chart with daily time grain",
            "config": {
                "chart_type": "xy",
                "kind": "line",
                "x": {"name": "order_date", "label": "Date"},
                "y": [
                    {
                        "name": "revenue",
                        "aggregate": "SUM",
                        "label": "Daily Revenue",
                    }
                ],
                "time_grain": "P1D",
            },
            "use_cases": ["Time series trends", "Growth tracking"],
        },
        "bar_chart": {
            "description": "Bar chart for category comparison with axis formatting",
            "config": {
                "chart_type": "xy",
                "kind": "bar",
                "x": {"name": "category", "label": "Category"},
                "y": [{"name": "sales", "aggregate": "SUM", "label": "Total Sales"}],
                "x_axis": {"title": "Product Categories"},
                "y_axis": {"title": "Revenue ($)", "format": "$,.0f"},
            },
            "use_cases": ["Category comparison", "Rankings"],
        },
        "stacked_bar": {
            "description": "Stacked bar chart with group_by dimension",
            "config": {
                "chart_type": "xy",
                "kind": "bar",
                "x": {"name": "quarter", "label": "Quarter"},
                "y": [
                    {"name": "revenue", "aggregate": "SUM", "label": "Revenue"},
                ],
                "group_by": [{"name": "region", "label": "Region"}],
                "stacked": True,
                "legend": {"show": True, "position": "right"},
            },
            "use_cases": ["Composition analysis", "Regional breakdown"],
        },
        "multi_metric_line": {
            "description": "Multi-metric line chart with filters and monthly grain",
            "config": {
                "chart_type": "xy",
                "kind": "line",
                "x": {"name": "order_date", "label": "Date"},
                "y": [
                    {"name": "revenue", "aggregate": "SUM", "label": "Revenue"},
                    {
                        "name": "customer_id",
                        "aggregate": "COUNT_DISTINCT",
                        "label": "Unique Customers",
                    },
                ],
                "time_grain": "P1M",
                "legend": {"show": True, "position": "top"},
                "filters": [{"column": "status", "op": "=", "value": "active"}],
            },
            "use_cases": ["KPI tracking", "Multi-dimensional analysis"],
        },
        "scatter_plot": {
            "description": "Scatter plot for correlation analysis",
            "config": {
                "chart_type": "xy",
                "kind": "scatter",
                "x": {
                    "name": "ad_spend",
                    "aggregate": "AVG",
                    "label": "Avg Ad Spend",
                },
                "y": [
                    {
                        "name": "conversion_rate",
                        "aggregate": "AVG",
                        "label": "Avg Conversion Rate",
                    }
                ],
                "group_by": [{"name": "campaign_type", "label": "Campaign"}],
                "x_axis": {"format": "$,.0f"},
                "y_axis": {"format": ".2%"},
            },
            "use_cases": ["Correlation analysis", "Outlier detection"],
        },
        "horizontal_bar": {
            "description": "Horizontal bar chart for categories with long names",
            "config": {
                "chart_type": "xy",
                "kind": "bar",
                "orientation": "horizontal",
                "x": {"name": "department", "label": "Department"},
                "y": [
                    {
                        "name": "headcount",
                        "aggregate": "SUM",
                        "label": "Headcount",
                    }
                ],
                "y_axis": {"title": "Department"},
                "x_axis": {"title": "Number of Employees"},
            },
            "use_cases": [
                "Long category labels",
                "Rankings and leaderboards",
                "Survey results",
            ],
        },
        "stacked_area": {
            "description": "Stacked area chart for volume composition over time",
            "config": {
                "chart_type": "xy",
                "kind": "area",
                "x": {"name": "order_date", "label": "Date"},
                "y": [{"name": "signups", "aggregate": "SUM", "label": "Signups"}],
                "group_by": [{"name": "channel", "label": "Channel"}],
                "stacked": True,
                "time_grain": "P1W",
            },
            "use_cases": ["Volume trends", "Channel attribution"],
        },
    }

    # Table chart examples
    table_chart_configs = {
        "basic_table": {
            "description": "Standard table with dimensions and aggregated metrics",
            "config": {
                "chart_type": "table",
                "columns": [
                    {"name": "customer_name", "label": "Customer"},
                    {"name": "orders", "aggregate": "COUNT", "label": "Total Orders"},
                    {"name": "revenue", "aggregate": "SUM", "label": "Total Revenue"},
                ],
                "sort_by": ["Total Revenue"],
            },
            "use_cases": ["Detail views", "Customer lists"],
        },
        "aggregated_table": {
            "description": "Table with multiple aggregations and filters",
            "config": {
                "chart_type": "table",
                "columns": [
                    {"name": "region", "label": "Sales Region"},
                    {
                        "name": "sales_amount",
                        "aggregate": "SUM",
                        "label": "Total Sales",
                    },
                    {
                        "name": "sales_amount",
                        "aggregate": "AVG",
                        "label": "Average Sale",
                    },
                    {
                        "name": "customer_id",
                        "aggregate": "COUNT_DISTINCT",
                        "label": "Unique Customers",
                    },
                ],
                "filters": [
                    {"column": "status", "op": "!=", "value": "cancelled"},
                ],
                "sort_by": ["Total Sales"],
            },
            "use_cases": ["Summary reports", "Regional analysis"],
        },
        "ag_grid_table": {
            "description": "Interactive AG Grid table with advanced features",
            "config": {
                "chart_type": "table",
                "viz_type": "ag-grid-table",
                "columns": [
                    {"name": "product_name", "label": "Product"},
                    {"name": "category", "label": "Category"},
                    {"name": "quantity", "aggregate": "SUM", "label": "Qty Sold"},
                    {"name": "revenue", "aggregate": "SUM", "label": "Revenue"},
                ],
            },
            "use_cases": [
                "Interactive exploration",
                "Large datasets with client-side sorting/filtering",
            ],
        },
    }

    advanced_chart_configs = {
        "funnel_chart": {
            "description": "Funnel chart for stage conversion analysis",
            "config": {
                "chart_type": "funnel",
                "dimension": {"name": "status", "label": "Stage"},
                "metric": {"name": "opportunity_id", "aggregate": "COUNT"},
                "percent_calculation_type": "first_step",
                "show_labels": True,
                "filters": [
                    {
                        "filter_type": "time_filter",
                        "column": "created_at",
                        "time_range": "Last quarter",
                    }
                ],
            },
            "use_cases": ["Conversion funnels", "Pipeline stage drop-off"],
        },
        "big_number_chart": {
            "description": "Big number KPI with monthly trendline",
            "config": {
                "chart_type": "big_number",
                "metric": {"name": "revenue", "aggregate": "SUM", "label": "Revenue"},
                "show_trend_line": True,
                "x": {"name": "order_date", "label": "Order Date"},
                "time_grain": "P1M",
                "filters": [{"column": "status", "op": "!=", "value": "cancelled"}],
            },
            "use_cases": ["Executive KPIs", "Headline metrics with trend context"],
        },
        "gauge_chart": {
            "description": "Gauge chart for KPI thresholds by segment",
            "config": {
                "chart_type": "gauge",
                "metric": {"name": "satisfaction_score", "aggregate": "AVG"},
                "dimension": {"name": "segment", "label": "Segment"},
                "split_number": 8,
                "show_progress": True,
            },
            "use_cases": ["Target attainment", "Service level KPIs"],
        },
        "heatmap_chart": {
            "description": "Heatmap chart for two-dimensional comparisons",
            "config": {
                "chart_type": "heatmap",
                "x": {"name": "product_line", "label": "Product Line"},
                "y": {"name": "deal_size", "label": "Deal Size"},
                "metric": {"name": "revenue", "aggregate": "SUM"},
                "normalize_across": "heatmap",
                "show_values": True,
                "filters": [
                    {
                        "filter_type": "range_filter",
                        "column": "discount",
                        "op": ">=",
                        "value": 0,
                    }
                ],
            },
            "use_cases": ["Matrix comparisons", "Density and intensity patterns"],
        },
        "treemap_chart": {
            "description": "Treemap chart for hierarchical part-to-whole analysis",
            "config": {
                "chart_type": "treemap",
                "dimensions": [
                    {"name": "year", "label": "Year"},
                    {"name": "product_line", "label": "Product Line"},
                ],
                "metric": {"name": "count", "aggregate": "COUNT"},
                "label_type": "key_value",
                "show_upper_labels": True,
            },
            "use_cases": ["Hierarchies", "Nested category comparisons"],
        },
        "sunburst_chart": {
            "description": "Sunburst chart for radial hierarchy exploration",
            "config": {
                "chart_type": "sunburst",
                "dimensions": [
                    {"name": "year", "label": "Year"},
                    {"name": "product_line", "label": "Product Line"},
                ],
                "metric": {"name": "count", "aggregate": "COUNT"},
                "show_labels_threshold": 5,
            },
            "use_cases": ["Radial hierarchies", "Proportion by nested level"],
        },
        "sankey_chart": {
            "description": "Sankey chart for source-to-target flow analysis",
            "config": {
                "chart_type": "sankey",
                "source": {"name": "product_line", "label": "Source"},
                "target": {"name": "deal_size", "label": "Target"},
                "metric": {"name": "count", "aggregate": "COUNT"},
            },
            "use_cases": ["Flow diagrams", "Transition analysis"],
        },
        "word_cloud_chart": {
            "description": "Word cloud chart for term prominence",
            "config": {
                "chart_type": "word_cloud",
                "series": {"name": "customer_name", "label": "Word"},
                "metric": {"name": "count", "aggregate": "COUNT"},
                "rotation": "square",
                "size_from": 10,
                "size_to": 70,
            },
            "use_cases": ["Text prominence", "Category frequency"],
        },
        "world_map_chart": {
            "description": "World map chart for country-level comparisons",
            "config": {
                "chart_type": "world_map",
                "entity": {"name": "country_code", "label": "Country"},
                "metric": {"name": "population", "aggregate": "SUM"},
                "country_fieldtype": "cca3",
                "show_bubbles": True,
            },
            "use_cases": ["Geographic comparisons", "Country-level KPIs"],
        },
        "box_plot_chart": {
            "description": "Box plot chart for distribution and outlier analysis",
            "config": {
                "chart_type": "box_plot",
                "x": {"name": "order_date", "label": "Order Date"},
                "group_by": [{"name": "product_line", "label": "Product Line"}],
                "metric": {"name": "count", "aggregate": "COUNT"},
                "time_grain": "P1D",
                "whisker_options": "Tukey",
            },
            "use_cases": ["Distribution analysis", "Outlier detection"],
        },
        "bubble_chart": {
            "description": "Bubble chart for x/y/size metric comparisons",
            "config": {
                "chart_type": "bubble",
                "x": {"name": "quantity_ordered", "aggregate": "SUM"},
                "y": {"name": "country", "aggregate": "COUNT_DISTINCT"},
                "size": {"name": "count", "aggregate": "COUNT"},
                "series": {"name": "product_line", "label": "Product Line"},
                "entity": {"name": "deal_size", "label": "Deal Size"},
                "max_bubble_size": 25,
            },
            "use_cases": ["Multi-metric comparison", "Portfolio segmentation"],
        },
    }

    # Best practices
    best_practices = {
        "xy_charts": [
            "Use time_grain for temporal x-axis columns (P1D, P1W, P1M, P1Y)",
            "Limit Y-axis metrics to 3-5 maximum for readability",
            "Use group_by to split data into series for comparison",
            "Use stacked=true for bar/area charts showing composition",
            "Configure axis format for readability ($,.0f for currency, .2% for pct)",
            "Use orientation='horizontal' for bar charts with long category names",
        ],
        "table_charts": [
            "Include only essential columns to avoid clutter",
            "Use meaningful labels different from raw column names",
            "Apply sort_by to highlight important data",
            "Use ag-grid-table viz_type for large interactive datasets",
        ],
        "general": [
            "Always verify column names with get_dataset_info before charting",
            "Use generate_explore_link for preview, generate_chart for saving",
            "Each column label must be unique across the entire configuration",
            "Column names must match: ^[a-zA-Z0-9_][a-zA-Z0-9_ \\-\\.]*$",
            "Filters accept value_filter, range_filter, null_filter, "
            "time_filter, and metric_filter variants",
            "Use dimensions in order for hierarchy charts like treemap and sunburst",
            "Bubble charts expect x, y, and size metrics plus a grouping dimension",
        ],
    }

    resource_data = {
        "xy_chart_configs": xy_chart_configs,
        "table_chart_configs": table_chart_configs,
        "advanced_chart_configs": advanced_chart_configs,
        "best_practices": best_practices,
        "usage_notes": [
            "All examples are valid ChartConfig objects that pass validation",
            "Modify column names and labels to match your actual dataset",
            "Use get_dataset_info to verify column names before charting",
            "For complete schema details, see the generate_chart tool parameters",
            "Use typed filters instead of raw adhoc_filters or SQL expressions",
            "Use metric_filter for aggregate HAVING-style thresholds on chart metrics",
        ],
    }

    from superset.utils import json

    return json.dumps(resource_data, indent=2)
