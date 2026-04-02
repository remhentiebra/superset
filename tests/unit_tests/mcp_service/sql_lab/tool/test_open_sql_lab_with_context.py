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

"""Tests for SQL Lab URL generation consistency."""

from unittest.mock import MagicMock, patch

from superset.mcp_service.sql_lab.schemas import OpenSqlLabRequest
from superset.mcp_service.sql_lab.tool.open_sql_lab_with_context import (
    open_sql_lab_with_context,
)


def test_open_sql_lab_with_context_denies_inaccessible_database() -> None:
    database = MagicMock()
    database.database_name = "warehouse"

    with (
        patch(
            "superset.daos.database.DatabaseDAO.find_by_id",
            return_value=database,
        ),
        patch(
            "superset.security_manager.can_access_database",
            return_value=False,
        ),
    ):
        response = open_sql_lab_with_context(
            OpenSqlLabRequest(database_connection_id=7),
            ctx=MagicMock(),
        )

    assert response.url == ""
    assert response.error == "Access denied to database warehouse"
