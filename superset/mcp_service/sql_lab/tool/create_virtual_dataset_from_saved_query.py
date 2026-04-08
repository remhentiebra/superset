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

"""Promote a saved query into a virtual dataset."""

import logging

from fastmcp import Context
from superset_core.mcp.decorators import tool, ToolAnnotations

from superset.commands.dataset.create import CreateDatasetCommand
from superset.daos.query import SavedQueryDAO
from superset.errors import ErrorLevel, SupersetError, SupersetErrorType
from superset.exceptions import SupersetErrorException
from superset.extensions import event_logger
from superset.mcp_service.dataset.schemas import DatasetError, DatasetInfo
from superset.mcp_service.dataset.utils import (
    build_create_dataset_payload,
    run_create_dataset_command,
    serialize_created_dataset,
)
from superset.mcp_service.sql_lab.schemas import (
    CreateVirtualDatasetFromSavedQueryRequest,
)

logger = logging.getLogger(__name__)


@tool(
    tags=["mutate"],
    class_permission_name="Dataset",
    method_permission_name="write",
    annotations=ToolAnnotations(
        title="Create virtual dataset from saved query",
        readOnlyHint=False,
        destructiveHint=False,
    ),
)
async def create_virtual_dataset_from_saved_query(
    request: CreateVirtualDatasetFromSavedQueryRequest, ctx: Context
) -> DatasetInfo | DatasetError:
    """Create a virtual dataset by promoting a saved SQL Lab query."""
    await ctx.info(
        "Creating virtual dataset from saved query: saved_query_id=%s"
        % (request.saved_query_id,)
    )

    with event_logger.log_context(
        action="mcp.create_virtual_dataset_from_saved_query.lookup"
    ):
        saved_query = SavedQueryDAO.find_by_id(request.saved_query_id)

    if saved_query is None:
        raise SupersetErrorException(
            SupersetError(
                message=f"Saved query {request.saved_query_id} not found",
                error_type=SupersetErrorType.GENERIC_COMMAND_ERROR,
                level=ErrorLevel.ERROR,
            )
        )

    dataset_name = request.table_name or saved_query.label
    if not dataset_name or not dataset_name.strip():
        raise SupersetErrorException(
            SupersetError(
                message="Saved query label is empty; provide table_name explicitly",
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
            "description": request.description or saved_query.description,
            "owners": request.owners,
            "template_params": None,
            "normalize_columns": request.normalize_columns,
            "always_filter_main_dttm": request.always_filter_main_dttm,
        }
    )

    template_parameters = getattr(saved_query, "template_parameters", None)
    if template_parameters:
        payload["template_params"] = template_parameters

    with event_logger.log_context(
        action="mcp.create_virtual_dataset_from_saved_query.create"
    ):
        dataset = run_create_dataset_command(
            payload,
            command_factory=CreateDatasetCommand,
            action_label="Create virtual dataset from saved query",
        )

    if isinstance(dataset, DatasetError):
        await ctx.warning(
            "Saved query promotion failed: error_type=%s, error=%s"
            % (dataset.error_type, dataset.error)
        )
        return dataset

    result = serialize_created_dataset(
        dataset,
        action_label="Create virtual dataset from saved query",
    )
    if isinstance(result, DatasetError):
        await ctx.warning(
            "Saved query promotion response serialization failed: "
            "error_type=%s, error=%s" % (result.error_type, result.error)
        )
        return result

    await ctx.info(
        "Virtual dataset created from saved query: dataset_id=%s, table_name=%r"
        % (result.id, result.table_name)
    )
    return result
