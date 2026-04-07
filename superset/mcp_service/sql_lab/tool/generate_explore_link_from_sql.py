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

"""Promote raw SQL into a virtual dataset and explore link."""

import logging

from fastmcp import Context
from superset_core.mcp.decorators import tool, ToolAnnotations

from superset.commands.dataset.create import CreateDatasetCommand
from superset.extensions import event_logger
from superset.mcp_service.chart.schemas import GenerateExploreLinkRequest
from superset.mcp_service.dataset.schemas import DatasetError
from superset.mcp_service.dataset.utils import (
    build_create_dataset_payload,
    run_create_dataset_command,
    serialize_dataset,
)
from superset.mcp_service.explore.tool.generate_explore_link import (
    generate_explore_link,
)
from superset.mcp_service.sql_lab.schemas import (
    ExploreLinkResponse,
    GenerateExploreLinkFromSqlRequest,
    GenerateExploreLinkFromSqlResponse,
)

logger = logging.getLogger(__name__)


@tool(
    tags=["mutate"],
    class_permission_name="Dataset",
    method_permission_name="write",
    annotations=ToolAnnotations(
        title="Generate explore link from SQL",
        readOnlyHint=False,
        destructiveHint=False,
    ),
)
async def generate_explore_link_from_sql(
    request: GenerateExploreLinkFromSqlRequest,
    ctx: Context,
) -> GenerateExploreLinkFromSqlResponse:
    """Create a virtual dataset from raw SQL and build an explore URL."""
    await ctx.info(
        "Generating explore link from SQL: database_id=%s, chart_type=%s, "
        "table_name=%r"
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

    with event_logger.log_context(action="mcp.generate_explore_link_from_sql.create"):
        dataset = run_create_dataset_command(
            payload,
            command_factory=CreateDatasetCommand,
            action_label="Generate explore link from SQL",
        )

    if isinstance(dataset, DatasetError):
        await ctx.warning(
            "SQL explore-link promotion failed at dataset creation: "
            "error_type=%s, error=%s" % (dataset.error_type, dataset.error)
        )
        return GenerateExploreLinkFromSqlResponse(
            dataset=None,
            dataset_error=dataset,
            explore_response=None,
        )

    dataset_info = serialize_dataset(dataset)
    explore_response = await generate_explore_link(
        GenerateExploreLinkRequest(
            dataset_id=dataset.id,
            config=request.config,
            use_cache=request.use_cache,
            force_refresh=request.force_refresh,
            cache_form_data=request.cache_form_data,
        ),
        ctx,
    )

    await ctx.info(
        "Explore link generated from SQL: dataset_id=%s, has_error=%s"
        % (dataset_info.id, bool(explore_response.get("error")))
    )
    return GenerateExploreLinkFromSqlResponse(
        dataset=dataset_info,
        dataset_error=None,
        explore_response=ExploreLinkResponse.model_validate(explore_response),
    )
