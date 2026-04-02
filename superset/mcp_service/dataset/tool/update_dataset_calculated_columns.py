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

"""Update dataset calculated columns MCP tool."""

import logging

from fastmcp import Context
from superset_core.mcp.decorators import tool, ToolAnnotations

from superset.commands.dataset.update import UpdateDatasetCommand
from superset.extensions import event_logger
from superset.mcp_service.dataset.schemas import (
    DatasetInfo,
    UpdateDatasetCalculatedColumnsRequest,
)
from superset.mcp_service.dataset.utils import (
    build_calculated_column_update_payload,
    get_dataset_by_identifier,
    serialize_dataset,
)

logger = logging.getLogger(__name__)


@tool(
    tags=["mutate"],
    class_permission_name="Dataset",
    method_permission_name="write",
    annotations=ToolAnnotations(
        title="Update dataset calculated columns",
        readOnlyHint=False,
        destructiveHint=False,
    ),
)
async def update_dataset_calculated_columns(
    request: UpdateDatasetCalculatedColumnsRequest,
    ctx: Context,
) -> DatasetInfo:
    """Create, update, or remove calculated columns on a dataset."""
    await ctx.info(
        "Updating dataset calculated columns: identifier=%r" % (request.identifier,)
    )

    dataset = get_dataset_by_identifier(request.identifier)
    columns_payload = build_calculated_column_update_payload(
        dataset=dataset,
        columns=request.columns,
        remove_columns=request.remove_columns,
        replace_calculated_columns=request.replace_calculated_columns,
    )

    with event_logger.log_context(
        action="mcp.update_dataset_calculated_columns.update"
    ):
        updated_dataset = UpdateDatasetCommand(
            dataset.id,
            {"columns": columns_payload},
        ).run()

    result = serialize_dataset(updated_dataset)
    await ctx.info(
        "Dataset calculated columns updated: dataset_id=%s, column_count=%s"
        % (result.id, len(result.columns))
    )
    return result
