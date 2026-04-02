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

"""MCP tool: get one accessible database."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastmcp import Context
from superset_core.mcp.decorators import tool, ToolAnnotations

from superset.extensions import event_logger
from superset.mcp_service.system.discovery_utils import (
    get_accessible_database,
    serialize_database_info,
)
from superset.mcp_service.system.schemas import (
    DatabaseError,
    DatabaseInfo,
    GetDatabaseInfoRequest,
)

logger = logging.getLogger(__name__)


@tool(
    tags=["discovery"],
    class_permission_name="Database",
    annotations=ToolAnnotations(
        title="Get database info",
        readOnlyHint=True,
        destructiveHint=False,
    ),
)
async def get_database_info(
    request: GetDatabaseInfoRequest, ctx: Context
) -> DatabaseInfo | DatabaseError:
    """Get one accessible database by ID."""
    await ctx.info(
        "Retrieving database information: database_id=%s" % request.database_id
    )

    with event_logger.log_context(action="mcp.get_database_info.lookup"):
        database = get_accessible_database(request.database_id)

    if database is None:
        return DatabaseError(
            error=f"Database {request.database_id} not found or not accessible",
            error_type="NotFound",
            timestamp=datetime.now(timezone.utc),
        )

    return serialize_database_info(database)
