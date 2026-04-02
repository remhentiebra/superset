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

"""Tests for MCP discovery utility helpers."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from superset.mcp_service.system.discovery_utils import (
    list_accessible_databases,
    list_accessible_dataset_summaries,
)


def _make_database(database_id: int, name: str, backend: str) -> MagicMock:
    database = MagicMock()
    database.id = database_id
    database.database_name = name
    database.backend = backend
    database.allow_file_upload = False
    database.allows_virtual_table_explore = True
    database.explore_database_id = database_id
    return database


def _make_dataset(
    dataset_id: int,
    table_name: str,
    changed_on: datetime,
    *,
    schema: str = "example_schema",
    database_id: int = 1,
) -> MagicMock:
    dataset = MagicMock()
    dataset.id = dataset_id
    dataset.table_name = table_name
    dataset.schema = schema
    dataset.database_id = database_id
    dataset.changed_on = changed_on
    return dataset


def test_list_accessible_databases_filters_and_sorts() -> None:
    first = _make_database(1, "Warehouse", "postgresql")
    second = _make_database(2, "Click Analytics", "clickhouse")
    third = _make_database(3, "Click Events", "clickhouse")

    with (
        patch("superset.db.session.query") as query,
        patch("superset.security_manager.can_access_database") as can_access,
    ):
        query.return_value.all.return_value = [first, second, third]
        can_access.side_effect = lambda database: database.id != 1

        result = list_accessible_databases(
            search="click",
            backend="clickhouse",
            order_column="id",
            order_direction="desc",
        )

    assert [database.id for database in result] == [3, 2]


def test_list_accessible_dataset_summaries_filters_by_access() -> None:
    older = _make_dataset(
        1,
        "sample_events_v1",
        datetime(2026, 4, 1, tzinfo=timezone.utc),
    )
    newest = _make_dataset(
        2,
        "sample_events_v2",
        datetime(2026, 4, 2, tzinfo=timezone.utc),
    )

    with (
        patch("superset.daos.dataset.DatasetDAO.find_all") as find_all,
        patch(
            "superset.mcp_service.system.discovery_utils.has_dataset_access"
        ) as has_access,
    ):
        find_all.return_value = [older, newest]
        has_access.side_effect = lambda dataset: dataset.id == 2

        result = list_accessible_dataset_summaries()

    assert len(result) == 1
    assert result[0].id == 2
    assert result[0].table_name == "sample_events_v2"
    assert result[0].schema_name == "example_schema"
