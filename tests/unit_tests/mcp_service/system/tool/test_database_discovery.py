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

"""Tests for MCP database discovery tools."""

from unittest.mock import MagicMock, patch

import pytest
from fastmcp import Client

from superset.mcp_service.app import mcp
from superset.utils import json


@pytest.fixture
def mcp_server():
    return mcp


@pytest.fixture(autouse=True)
def mock_auth():
    """Mock authentication for all tests."""
    from unittest.mock import Mock

    with patch("superset.mcp_service.auth.get_user_from_request") as mock_get_user:
        mock_user = Mock()
        mock_user.id = 1
        mock_user.username = "admin"
        mock_get_user.return_value = mock_user
        yield mock_get_user


def _make_database(
    database_id: int,
    name: str,
    backend: str,
    *,
    allow_file_upload: bool = False,
    allows_virtual_datasets: bool = True,
    explore_database_id: int | None = None,
) -> MagicMock:
    database = MagicMock()
    database.id = database_id
    database.database_name = name
    database.backend = backend
    database.allow_file_upload = allow_file_upload
    database.allows_virtual_table_explore = allows_virtual_datasets
    database.explore_database_id = explore_database_id or database_id
    return database


@pytest.mark.asyncio
async def test_list_databases_returns_paginated_accessible_results(mcp_server) -> None:
    first = _make_database(1, "warehouse", "postgresql")
    second = _make_database(2, "events", "clickhouse")
    event_logger = MagicMock()
    event_logger.log_context.return_value.__enter__ = MagicMock()
    event_logger.log_context.return_value.__exit__ = MagicMock(return_value=False)

    with (
        patch(
            "superset.mcp_service.system.tool.list_databases.list_accessible_databases",
            return_value=[first, second],
        ),
        patch(
            "superset.mcp_service.system.tool.list_databases.event_logger",
            event_logger,
        ),
    ):
        async with Client(mcp_server) as client:
            result = await client.call_tool(
                "list_databases",
                {"request": {"page": 1, "page_size": 1}},
            )

    data = json.loads(result.content[0].text)
    assert data["count"] == 1
    assert data["total_count"] == 2
    assert data["databases"][0] == {
        "id": 1,
        "database_name": "warehouse",
        "backend": "postgresql",
    }


@pytest.mark.asyncio
async def test_get_database_info_returns_expected_fields(mcp_server) -> None:
    database = _make_database(
        7,
        "warehouse",
        "clickhouse",
        allow_file_upload=True,
        allows_virtual_datasets=True,
        explore_database_id=11,
    )
    event_logger = MagicMock()
    event_logger.log_context.return_value.__enter__ = MagicMock()
    event_logger.log_context.return_value.__exit__ = MagicMock(return_value=False)

    with (
        patch(
            "superset.mcp_service.system.tool.get_database_info.get_accessible_database",
            return_value=database,
        ),
        patch(
            "superset.mcp_service.system.tool.get_database_info.event_logger",
            event_logger,
        ),
    ):
        async with Client(mcp_server) as client:
            result = await client.call_tool(
                "get_database_info",
                {"request": {"database_id": 7}},
            )

    data = json.loads(result.content[0].text)
    assert data == {
        "id": 7,
        "database_name": "warehouse",
        "backend": "clickhouse",
        "allow_file_upload": True,
        "allows_virtual_datasets": True,
        "explore_database_id": 11,
    }


@pytest.mark.asyncio
async def test_get_database_info_returns_clear_error_for_inaccessible_database(
    mcp_server,
) -> None:
    event_logger = MagicMock()
    event_logger.log_context.return_value.__enter__ = MagicMock()
    event_logger.log_context.return_value.__exit__ = MagicMock(return_value=False)

    with (
        patch(
            "superset.mcp_service.system.tool.get_database_info.get_accessible_database",
            return_value=None,
        ),
        patch(
            "superset.mcp_service.system.tool.get_database_info.event_logger",
            event_logger,
        ),
    ):
        async with Client(mcp_server) as client:
            result = await client.call_tool(
                "get_database_info",
                {"request": {"database_id": 99}},
            )

    data = json.loads(result.content[0].text)
    assert data["error_type"] == "NotFound"
    assert "not found or not accessible" in data["error"]
