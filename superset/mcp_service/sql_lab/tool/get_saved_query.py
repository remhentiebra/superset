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

"""Get saved query MCP tool."""

import logging

from fastmcp import Context
from superset_core.mcp.decorators import tool, ToolAnnotations

from superset.daos.query import SavedQueryDAO
from superset.errors import ErrorLevel, SupersetError, SupersetErrorType
from superset.exceptions import SupersetErrorException
from superset.extensions import event_logger
from superset.mcp_service.sql_lab.schemas import GetSavedQueryRequest, SavedQueryInfo
from superset.mcp_service.sql_lab.tool.list_saved_queries import (
    _serialize_saved_query,
)

logger = logging.getLogger(__name__)


@tool(
    tags=["discovery"],
    class_permission_name="SavedQuery",
    method_permission_name="read",
    annotations=ToolAnnotations(
        title="Get saved query",
        readOnlyHint=True,
        destructiveHint=False,
    ),
)
async def get_saved_query(
    request: GetSavedQueryRequest, ctx: Context
) -> SavedQueryInfo:
    """Retrieve one saved query owned by the current user."""
    await ctx.info("Fetching saved query: identifier=%s" % (request.identifier,))

    with event_logger.log_context(action="mcp.get_saved_query.lookup"):
        saved_query = SavedQueryDAO.find_by_id(request.identifier)

    if saved_query is None:
        raise SupersetErrorException(
            SupersetError(
                message=f"Saved query {request.identifier} not found",
                error_type=SupersetErrorType.GENERIC_COMMAND_ERROR,
                level=ErrorLevel.ERROR,
            )
        )

    return _serialize_saved_query(saved_query)
