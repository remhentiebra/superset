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

"""Promote raw SQL into a virtual dataset and chart."""

import logging
from time import perf_counter

from fastmcp import Context
from superset_core.mcp.decorators import tool, ToolAnnotations

from superset.commands.dataset.create import CreateDatasetCommand
from superset.extensions import event_logger
from superset.mcp_service.chart.performance_utils import (
    build_performance_metadata,
    merge_stage_durations,
    record_stage,
)
from superset.mcp_service.chart.schemas import GenerateChartRequest
from superset.mcp_service.chart.tool.generate_chart import generate_chart
from superset.mcp_service.dataset.schemas import DatasetError
from superset.mcp_service.dataset.utils import (
    build_create_dataset_payload,
    run_create_dataset_command,
    serialize_dataset,
)
from superset.mcp_service.sql_lab.schemas import (
    GenerateChartFromSqlRequest,
    GenerateChartFromSqlResponse,
)

logger = logging.getLogger(__name__)


@tool(
    tags=["mutate"],
    class_permission_name="Dataset",
    method_permission_name="write",
    annotations=ToolAnnotations(
        title="Generate chart from SQL",
        readOnlyHint=False,
        destructiveHint=False,
    ),
)
async def generate_chart_from_sql(
    request: GenerateChartFromSqlRequest,
    ctx: Context,
) -> GenerateChartFromSqlResponse:
    """Create a virtual dataset from raw SQL and build a chart on top of it."""
    total_start_time = perf_counter()
    stage_durations_ms: dict[str, int] = {}

    await ctx.info(
        "Generating chart from SQL: database_id=%s, chart_type=%s, table_name=%r"
        % (request.database_id, request.config.chart_type, request.table_name)
    )

    payload = build_create_dataset_payload(
        {
            "database_id": request.database_id,
            "table_name": request.table_name,
            "sql": request.sql,
            "schema_name": request.schema_name,
            "catalog": request.catalog,
            "description": request.dataset_description,
            "owners": request.owners,
            "template_params": request.template_params,
            "normalize_columns": request.normalize_columns,
            "always_filter_main_dttm": request.always_filter_main_dttm,
        }
    )

    with record_stage(stage_durations_ms, "dataset_create"):
        with event_logger.log_context(action="mcp.generate_chart_from_sql.create"):
            dataset = run_create_dataset_command(
                payload,
                command_factory=CreateDatasetCommand,
                action_label="Generate chart from SQL",
            )

    if isinstance(dataset, DatasetError):
        await ctx.warning(
            "SQL chart promotion failed at dataset creation: "
            "error_type=%s, error=%s" % (dataset.error_type, dataset.error)
        )
        return GenerateChartFromSqlResponse(
            dataset=None,
            dataset_error=dataset,
            chart_response=None,
        )

    dataset_info = serialize_dataset(dataset)
    with record_stage(stage_durations_ms, "chart_generation"):
        chart_response = await generate_chart(
            GenerateChartRequest(
                dataset_id=dataset.id,
                config=request.config,
                chart_name=request.chart_name,
                save_chart=request.save_chart,
                generate_preview=request.generate_preview,
                preview_formats=request.preview_formats,
            ),
            ctx,
        )

    response_assembly_start = perf_counter()
    existing_performance = chart_response.performance
    stage_durations_ms["response_assembly"] = int(
        (perf_counter() - response_assembly_start) * 1000
    )
    if existing_performance is not None:
        chart_response.performance = build_performance_metadata(
            total_start_time=total_start_time,
            cache_status=existing_performance.cache_status,
            optimization_suggestions=existing_performance.optimization_suggestions,
            stage_durations_ms=merge_stage_durations(
                stage_durations_ms,
                existing_performance.stage_durations_ms,
            ),
            compile_query_duration_ms=existing_performance.compile_query_duration_ms,
            estimated_cost=existing_performance.estimated_cost,
        )

    await ctx.info(
        "Chart generated from SQL: dataset_id=%s, chart_saved=%s"
        % (dataset_info.id, chart_response.success)
    )
    return GenerateChartFromSqlResponse(
        dataset=dataset_info,
        dataset_error=None,
        chart_response=chart_response,
    )
