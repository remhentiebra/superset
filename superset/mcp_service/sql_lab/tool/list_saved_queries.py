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

"""List saved queries MCP tool."""

import logging
from typing import Any, cast, TYPE_CHECKING

from fastmcp import Context
from superset_core.mcp.decorators import tool, ToolAnnotations

from superset.daos.base import ColumnOperator
from superset.daos.query import SavedQueryDAO
from superset.extensions import event_logger
from superset.mcp_service.sql_lab.schemas import (
    ListSavedQueriesRequest,
    SavedQueryInfo,
    SavedQueryListResponse,
)
from superset.mcp_service.utils.url_utils import get_superset_base_url

if TYPE_CHECKING:
    from superset.models.sql_lab import SavedQuery

logger = logging.getLogger(__name__)


def _serialize_saved_query(saved_query: Any) -> SavedQueryInfo:
    typed_saved_query = cast("SavedQuery", saved_query)
    base_url = get_superset_base_url()
    url = f"{base_url}/sqllab?savedQueryId={typed_saved_query.id}"
    database = typed_saved_query.database
    return SavedQueryInfo(
        id=typed_saved_query.id,
        label=typed_saved_query.label,
        sql=typed_saved_query.sql,
        database_id=typed_saved_query.db_id,
        database_name=getattr(database, "database_name", None),
        schema_name=typed_saved_query.schema,
        catalog=typed_saved_query.catalog,
        description=typed_saved_query.description,
        template_parameters=typed_saved_query.template_parameters,
        changed_on=typed_saved_query.changed_on,
        created_on=typed_saved_query.created_on,
        url=url,
    )


@tool(
    tags=["discovery"],
    class_permission_name="SavedQuery",
    method_permission_name="read",
    annotations=ToolAnnotations(
        title="List saved queries",
        readOnlyHint=True,
        destructiveHint=False,
    ),
)
async def list_saved_queries(
    request: ListSavedQueriesRequest, ctx: Context
) -> SavedQueryListResponse:
    """List the current user's saved queries with typed filters."""
    await ctx.info(
        "Listing saved queries: page=%s, page_size=%s, search=%r"
        % (request.page, request.page_size, request.search)
    )

    filters: list[ColumnOperator] = []
    if request.database_id is not None:
        filters.append(ColumnOperator(col="db_id", opr="eq", value=request.database_id))
    if request.schema_name is not None:
        filters.append(
            ColumnOperator(col="schema", opr="eq", value=request.schema_name)
        )

    with event_logger.log_context(action="mcp.list_saved_queries.query"):
        items, total_count = SavedQueryDAO.list(
            column_operators=filters,
            order_column=request.order_column,
            order_direction=request.order_direction,
            page=max(request.page - 1, 0),
            page_size=request.page_size,
            search=request.search,
            search_columns=["label", "schema", "description", "sql"],
        )

    saved_queries = [_serialize_saved_query(item) for item in items]
    total_pages = max((total_count + request.page_size - 1) // request.page_size, 1)
    return SavedQueryListResponse(
        saved_queries=saved_queries,
        count=len(saved_queries),
        total_count=total_count,
        page=request.page,
        page_size=request.page_size,
        total_pages=total_pages,
        has_previous=request.page > 1,
        has_next=request.page < total_pages,
    )
