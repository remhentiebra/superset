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

"""Update dataset metadata MCP tool."""

import logging

from fastmcp import Context
from superset_core.mcp.decorators import tool, ToolAnnotations

from superset.commands.dataset.update import UpdateDatasetCommand
from superset.extensions import event_logger
from superset.mcp_service.dataset.schemas import (
    DatasetInfo,
    UpdateDatasetMetadataRequest,
)
from superset.mcp_service.dataset.utils import (
    build_dataset_metadata_update_payload,
    ensure_dataset_supports_requested_updates,
    get_dataset_by_identifier,
    refetch_dataset_for_response,
    serialize_dataset,
    sync_dataset_custom_tags,
)

logger = logging.getLogger(__name__)


@tool(
    tags=["mutate"],
    class_permission_name="Dataset",
    method_permission_name="write",
    annotations=ToolAnnotations(
        title="Update dataset metadata",
        readOnlyHint=False,
        destructiveHint=False,
    ),
)
async def update_dataset_metadata(
    request: UpdateDatasetMetadataRequest,
    ctx: Context,
) -> DatasetInfo:
    """Update dataset metadata and virtual dataset SQL using typed payloads."""
    await ctx.info("Updating dataset metadata: identifier=%r" % (request.identifier,))

    dataset = get_dataset_by_identifier(request.identifier)
    request_data = request.model_dump(exclude_unset=True, by_alias=False)
    ensure_dataset_supports_requested_updates(dataset, set(request_data))

    tag_names = request_data.pop("tag_names", None)
    payload = build_dataset_metadata_update_payload(request_data)

    with event_logger.log_context(action="mcp.update_dataset_metadata.update"):
        updated_dataset = UpdateDatasetCommand(dataset.id, payload).run()
        if tag_names is not None:
            sync_dataset_custom_tags(updated_dataset, tag_names)
        refreshed_dataset = refetch_dataset_for_response(updated_dataset.id)

    result = serialize_dataset(refreshed_dataset)
    await ctx.info(
        "Dataset metadata updated: dataset_id=%s, table_name=%r"
        % (result.id, result.table_name)
    )
    return result
