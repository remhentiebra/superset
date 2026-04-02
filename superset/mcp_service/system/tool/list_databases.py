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

"""MCP tool: list accessible databases."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastmcp import Context
from superset_core.mcp.decorators import tool, ToolAnnotations

from superset.extensions import event_logger
from superset.mcp_service.system.discovery_utils import (
    list_accessible_databases,
    serialize_database_summary,
)
from superset.mcp_service.system.schemas import (
    ListDatabasesRequest,
    ListDatabasesResponse,
    PaginationInfo,
)

logger = logging.getLogger(__name__)


@tool(
    tags=["core"],
    class_permission_name="Database",
    annotations=ToolAnnotations(
        title="List databases",
        readOnlyHint=True,
        destructiveHint=False,
    ),
)
async def list_databases(
    request: ListDatabasesRequest, ctx: Context
) -> ListDatabasesResponse:
    """List accessible databases for SQL and virtual-dataset workflows."""
    await ctx.info(
        "Listing databases: page=%s, page_size=%s, search=%s, backend=%s"
        % (request.page, request.page_size, request.search, request.backend)
    )

    with event_logger.log_context(action="mcp.list_databases.query"):
        databases = list_accessible_databases(
            search=request.search,
            backend=request.backend,
            order_column=request.order_column,
            order_direction=request.order_direction,
        )

    total_count = len(databases)
    start = (request.page - 1) * request.page_size
    end = start + request.page_size
    paged_databases = databases[start:end]
    total_pages = (
        (total_count + request.page_size - 1) // request.page_size
        if request.page_size > 0
        else 0
    )

    return ListDatabasesResponse(
        databases=[
            serialize_database_summary(database) for database in paged_databases
        ],
        count=len(paged_databases),
        total_count=total_count,
        page=request.page,
        page_size=request.page_size,
        total_pages=total_pages,
        has_previous=request.page > 1,
        has_next=request.page < total_pages,
        pagination=PaginationInfo(
            page=request.page,
            page_size=request.page_size,
            total_count=total_count,
            total_pages=total_pages,
            has_next=request.page < total_pages,
            has_previous=request.page > 1,
        ),
        timestamp=datetime.now(timezone.utc),
    )
