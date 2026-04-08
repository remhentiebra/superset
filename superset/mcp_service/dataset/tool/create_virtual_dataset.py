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

"""Create virtual dataset MCP tool."""

import logging

from fastmcp import Context
from superset_core.mcp.decorators import tool, ToolAnnotations

from superset.commands.dataset.create import CreateDatasetCommand
from superset.extensions import event_logger
from superset.mcp_service.dataset.schemas import (
    CreateVirtualDatasetRequest,
    DatasetError,
    DatasetInfo,
)
from superset.mcp_service.dataset.utils import (
    build_create_dataset_payload,
    run_create_dataset_command,
    serialize_created_dataset,
)

logger = logging.getLogger(__name__)


@tool(
    tags=["mutate", "dataset"],
    class_permission_name="Dataset",
    method_permission_name="write",
    annotations=ToolAnnotations(
        title="Create virtual dataset",
        readOnlyHint=False,
        destructiveHint=False,
    ),
)
async def create_virtual_dataset(
    request: CreateVirtualDatasetRequest, ctx: Context
) -> DatasetInfo | DatasetError:
    """Create a virtual dataset from typed SQL input."""
    await ctx.info(
        "Creating virtual dataset: database_id=%s, table_name=%r"
        % (request.database_id, request.table_name)
    )

    payload = build_create_dataset_payload(request.model_dump())

    with event_logger.log_context(action="mcp.create_virtual_dataset.create"):
        dataset = run_create_dataset_command(
            payload,
            command_factory=CreateDatasetCommand,
            action_label="Create virtual dataset",
        )

    if isinstance(dataset, DatasetError):
        await ctx.warning(
            "Virtual dataset creation failed: error_type=%s, error=%s"
            % (dataset.error_type, dataset.error)
        )
        return dataset

    result = serialize_created_dataset(
        dataset,
        action_label="Create virtual dataset",
    )
    if isinstance(result, DatasetError):
        await ctx.warning(
            "Virtual dataset response serialization failed: error_type=%s, error=%s"
            % (result.error_type, result.error)
        )
        return result

    await ctx.info(
        "Virtual dataset created: dataset_id=%s, table_name=%r"
        % (result.id, result.table_name)
    )
    return result
