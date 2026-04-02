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

"""Promote a saved query into a virtual dataset and chart."""

import logging

from fastmcp import Context
from superset_core.mcp.decorators import tool, ToolAnnotations

from superset.commands.dataset.create import CreateDatasetCommand
from superset.daos.query import SavedQueryDAO
from superset.errors import ErrorLevel, SupersetError, SupersetErrorType
from superset.exceptions import SupersetErrorException
from superset.extensions import event_logger
from superset.mcp_service.chart.schemas import GenerateChartRequest
from superset.mcp_service.chart.tool.generate_chart import generate_chart
from superset.mcp_service.dataset.schemas import DatasetError
from superset.mcp_service.dataset.utils import (
    build_create_dataset_payload,
    run_create_dataset_command,
    serialize_dataset,
)
from superset.mcp_service.sql_lab.schemas import (
    GenerateChartFromSavedQueryRequest,
    GenerateChartFromSavedQueryResponse,
)

logger = logging.getLogger(__name__)


@tool(
    tags=["mutate"],
    class_permission_name="Dataset",
    method_permission_name="write",
    annotations=ToolAnnotations(
        title="Generate chart from saved query",
        readOnlyHint=False,
        destructiveHint=False,
    ),
)
async def generate_chart_from_saved_query(
    request: GenerateChartFromSavedQueryRequest,
    ctx: Context,
) -> GenerateChartFromSavedQueryResponse:
    """Create a virtual dataset from a saved query and build a chart on top of it."""
    await ctx.info(
        "Generating chart from saved query: saved_query_id=%s, chart_type=%s"
        % (request.saved_query_id, request.config.chart_type)
    )

    with event_logger.log_context(action="mcp.generate_chart_from_saved_query.lookup"):
        saved_query = SavedQueryDAO.find_by_id(request.saved_query_id)

    if saved_query is None:
        raise SupersetErrorException(
            SupersetError(
                message=f"Saved query {request.saved_query_id} not found",
                error_type=SupersetErrorType.GENERIC_COMMAND_ERROR,
                level=ErrorLevel.ERROR,
            )
        )

    dataset_name = request.dataset_name or saved_query.label
    if not dataset_name or not dataset_name.strip():
        raise SupersetErrorException(
            SupersetError(
                message="Saved query label is empty; provide dataset_name explicitly",
                error_type=SupersetErrorType.INVALID_PAYLOAD_FORMAT_ERROR,
                level=ErrorLevel.ERROR,
            )
        )

    payload = build_create_dataset_payload(
        {
            "database_id": saved_query.db_id,
            "table_name": dataset_name,
            "sql": saved_query.sql,
            "schema_name": saved_query.schema or None,
            "catalog": saved_query.catalog,
            "description": request.dataset_description or saved_query.description,
            "owners": request.owners,
            "template_params": None,
            "normalize_columns": request.normalize_columns,
            "always_filter_main_dttm": request.always_filter_main_dttm,
        }
    )

    template_parameters = getattr(saved_query, "template_parameters", None)
    if template_parameters:
        payload["template_params"] = template_parameters

    with event_logger.log_context(action="mcp.generate_chart_from_saved_query.create"):
        dataset = run_create_dataset_command(
            payload,
            command_factory=CreateDatasetCommand,
            action_label="Generate chart from saved query",
        )

    if isinstance(dataset, DatasetError):
        await ctx.warning(
            "Saved-query chart promotion failed at dataset creation: "
            "error_type=%s, error=%s" % (dataset.error_type, dataset.error)
        )
        return GenerateChartFromSavedQueryResponse(
            dataset=None,
            dataset_error=dataset,
            chart_response=None,
        )

    dataset_info = serialize_dataset(dataset)
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

    await ctx.info(
        "Chart generated from saved query: dataset_id=%s, chart_saved=%s"
        % (dataset_info.id, chart_response.success)
    )
    return GenerateChartFromSavedQueryResponse(
        dataset=dataset_info,
        dataset_error=None,
        chart_response=chart_response,
    )
