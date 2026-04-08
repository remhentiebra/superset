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

"""Preferred MCP tool for creating SQL-backed datasets."""

import logging

from fastmcp import Context
from superset_core.mcp.decorators import tool, ToolAnnotations

from superset.commands.dataset.create import CreateDatasetCommand
from superset.extensions import event_logger
from superset.mcp_service.dataset.schemas import (
    CreateDatasetRequest,
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
        title="Create dataset",
        readOnlyHint=False,
        destructiveHint=False,
    ),
)
async def create_dataset(
    request: CreateDatasetRequest,
    ctx: Context,
) -> DatasetInfo | DatasetError:
    """Create a physical table-backed dataset or a virtual SQL dataset.

    Preferred public name for MCP dataset authoring. The legacy
    ``create_virtual_dataset`` tool remains available for compatibility.
    """
    await ctx.info(
        "Creating dataset: database_id=%s, table_name=%r"
        % (request.database_id, request.table_name)
    )

    payload = build_create_dataset_payload(request.model_dump())

    with event_logger.log_context(action="mcp.create_dataset.create"):
        dataset = run_create_dataset_command(
            payload,
            command_factory=CreateDatasetCommand,
            action_label="Create dataset",
        )

    if isinstance(dataset, DatasetError):
        await ctx.warning(
            "Dataset creation failed: error_type=%s, error=%s"
            % (dataset.error_type, dataset.error)
        )
        return dataset

    result = serialize_created_dataset(dataset, action_label="Create dataset")
    if isinstance(result, DatasetError):
        await ctx.warning(
            "Dataset response serialization failed: error_type=%s, error=%s"
            % (result.error_type, result.error)
        )
        return result

    await ctx.info(
        "Dataset created: dataset_id=%s, table_name=%r" % (result.id, result.table_name)
    )
    return result
