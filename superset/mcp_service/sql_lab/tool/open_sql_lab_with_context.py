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

"""
Open SQL Lab with Context MCP Tool

Tool for generating SQL Lab URLs with pre-populated sql and context.
"""

import logging
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from fastmcp import Context
from superset_core.mcp.decorators import tool, ToolAnnotations

from superset.extensions import db, event_logger
from superset.mcp_service.sql_lab.schemas import (
    OpenSqlLabRequest,
    SqlLabResponse,
)
from superset.mcp_service.utils import sanitize_for_llm_context
from superset.mcp_service.utils.url_utils import get_superset_base_url

logger = logging.getLogger(__name__)

SQL_LAB_QUERY_PARAMS_TO_SANITIZE = frozenset({"sql", "title"})


def _sanitize_sql_lab_url_for_llm_context(url: str) -> str:
    """Wrap user-controlled SQL Lab query values while preserving navigation."""
    if not url:
        return url

    parsed = urlsplit(url)
    query_params = parse_qsl(parsed.query, keep_blank_values=True)
    if not query_params:
        return url

    sanitized_params = [
        (
            name,
            sanitize_for_llm_context(value, field_path=(name,))
            if name in SQL_LAB_QUERY_PARAMS_TO_SANITIZE
            else value,
        )
        for name, value in query_params
    ]
    return urlunsplit(parsed._replace(query=urlencode(sanitized_params)))


def _sanitize_sql_lab_response_for_llm_context(
    response: SqlLabResponse,
) -> SqlLabResponse:
    """Wrap user-controlled SQL Lab response content before LLM exposure."""
    payload = response.model_dump(mode="python")
    payload["url"] = _sanitize_sql_lab_url_for_llm_context(payload.get("url", ""))

    for field_name in ("title", "error"):
        payload[field_name] = sanitize_for_llm_context(
            payload.get(field_name),
            field_path=(field_name,),
        )

    return SqlLabResponse.model_validate(payload)


def _build_context_sql(
    database_name: str,
    dataset_name: str,
    schema_name: str | None,
) -> str:
    context_comment = (
        f"-- Context: Working with dataset '{dataset_name}'\n"
        f"-- Database: {database_name}\n"
    )
    if schema_name:
        context_comment += f"-- Schema: {schema_name}\n"
        table_reference = f"{schema_name}.{dataset_name}"
    else:
        table_reference = dataset_name

    return f"{context_comment}\nSELECT * FROM {table_reference} LIMIT 100;"


def _build_sql_lab_params(
    request: OpenSqlLabRequest,
    database_name: str,
) -> dict[str, str]:
    params = {
        "dbid": str(request.database_connection_id),
    }
    if request.schema_name:
        params["schema"] = request.schema_name
    if request.sql:
        params["sql"] = request.sql
    if request.title:
        params["title"] = request.title
    if request.dataset_in_context and not request.sql:
        params["sql"] = _build_context_sql(
            database_name,
            request.dataset_in_context,
            request.schema_name,
        )
    return params


def _get_accessible_database(database_id: int) -> tuple[Any | None, str | None]:
    from superset import security_manager
    from superset.daos.database import DatabaseDAO

    with event_logger.log_context(action="mcp.open_sql_lab.db_validation"):
        database = DatabaseDAO.find_by_id(database_id)
    if not database:
        return (
            None,
            f"Database with ID {database_id} not found."
            " Use list_databases to get valid database IDs.",
        )
    if not security_manager.can_access_database(database):
        return None, f"Access denied to database {database.database_name}"
    return database, None


@tool(
    tags=["explore"],
    class_permission_name="SQLLab",
    method_permission_name="read",
    annotations=ToolAnnotations(
        title="Open SQL Lab with context",
        readOnlyHint=True,
        destructiveHint=False,
    ),
)
def open_sql_lab_with_context(
    request: OpenSqlLabRequest, ctx: Context
) -> SqlLabResponse:
    """Generate SQL Lab URL with pre-populated sql and context.

    Pass the sql parameter to pre-fill the editor. Returns URL for direct navigation.
    """
    try:
        database, error = _get_accessible_database(request.database_connection_id)
        if error:
            return _sanitize_sql_lab_response_for_llm_context(
                SqlLabResponse(
                    url="",
                    database_id=request.database_connection_id,
                    schema_name=request.schema_name,
                    title=request.title,
                    error=error,
                )
            )
        assert database is not None

        params = _build_sql_lab_params(request, database.database_name)

        # Construct SQL Lab URL with full base URL
        query_string = urlencode(params)
        url = f"{get_superset_base_url()}/sqllab?{query_string}"

        logger.info(
            "Generated SQL Lab URL for database %s", request.database_connection_id
        )

        return _sanitize_sql_lab_response_for_llm_context(
            SqlLabResponse(
                url=url,
                database_id=request.database_connection_id,
                schema_name=request.schema_name,
                title=request.title,
                error=None,
            )
        )

    except Exception as e:
        try:
            db.session.rollback()  # pylint: disable=consider-using-transaction
        except Exception:  # noqa: BLE001
            # Broad catch: the DB connection itself may be broken (e.g.,
            # SSL drop), so even rollback can fail with non-SQLAlchemy
            # errors. This is a cleanup path — swallow and log.
            logger.warning(
                "Database rollback failed during error handling", exc_info=True
            )
        logger.error("Error generating SQL Lab URL: %s", e)
        return _sanitize_sql_lab_response_for_llm_context(
            SqlLabResponse(
                url="",
                database_id=request.database_connection_id,
                schema_name=request.schema_name,
                title=request.title,
                error=f"Failed to generate SQL Lab URL: {str(e)}",
            )
        )
