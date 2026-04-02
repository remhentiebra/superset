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

"""Shared discovery helpers for MCP system metadata and database tools."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from superset.mcp_service.auth import has_dataset_access
from superset.mcp_service.system.schemas import (
    AvailableDatasetSummary,
    DatabaseInfo,
    DatabaseSummary,
)

logger = logging.getLogger(__name__)


def _get_database_backend(database: Any) -> str | None:
    """Read the database backend defensively."""
    try:
        return database.backend
    except Exception as ex:  # pylint: disable=broad-except
        logger.debug("Could not determine backend for database %s: %s", database, ex)
        return None


def serialize_database_summary(database: Any) -> DatabaseSummary:
    """Serialize a database into the shared MCP summary shape."""
    return DatabaseSummary(
        id=database.id,
        database_name=database.database_name,
        backend=_get_database_backend(database),
    )


def serialize_database_info(database: Any) -> DatabaseInfo:
    """Serialize a database into the shared MCP detail shape."""
    return DatabaseInfo(
        id=database.id,
        database_name=database.database_name,
        backend=_get_database_backend(database),
        allow_file_upload=bool(getattr(database, "allow_file_upload", False)),
        allows_virtual_datasets=bool(
            getattr(database, "allows_virtual_table_explore", False)
        ),
        explore_database_id=getattr(database, "explore_database_id", None),
    )


def list_accessible_databases(
    search: str | None = None,
    backend: str | None = None,
    order_column: str = "database_name",
    order_direction: str = "asc",
) -> list[Any]:
    """Return accessible databases using the same permission check as execute_sql."""
    from superset import db, security_manager
    from superset.models.core import Database

    search_term = search.lower() if search else None
    backend_filter = backend.lower() if backend else None

    databases = db.session.query(Database).all()
    accessible_databases = [
        database
        for database in databases
        if security_manager.can_access_database(database)
    ]

    if backend_filter:
        accessible_databases = [
            database
            for database in accessible_databases
            if (_get_database_backend(database) or "").lower() == backend_filter
        ]

    if search_term:
        accessible_databases = [
            database
            for database in accessible_databases
            if search_term in database.database_name.lower()
            or search_term in (_get_database_backend(database) or "").lower()
        ]

    def _sort_key(database: Any) -> Any:
        if order_column == "id":
            return database.id
        if order_column == "backend":
            return (_get_database_backend(database) or "").lower()
        return database.database_name.lower()

    accessible_databases.sort(
        key=_sort_key,
        reverse=order_direction == "desc",
    )
    return accessible_databases


def get_accessible_database(database_id: int) -> Any | None:
    """Return one accessible database or None if absent/inaccessible."""
    from superset import security_manager
    from superset.daos.database import DatabaseDAO

    database = DatabaseDAO.find_by_id(database_id, skip_base_filter=True)
    if database is None or not security_manager.can_access_database(database):
        return None
    return database


def list_accessible_dataset_summaries(limit: int = 20) -> list[AvailableDatasetSummary]:
    """Return recently modified accessible datasets for convenience summaries."""
    from superset.daos.dataset import DatasetDAO

    datasets = DatasetDAO.find_all()
    accessible_datasets = [
        dataset for dataset in datasets if has_dataset_access(dataset)
    ]
    accessible_datasets.sort(
        key=lambda dataset: (
            getattr(dataset, "changed_on", None)
            or datetime.min.replace(tzinfo=timezone.utc)
        ),
        reverse=True,
    )
    return [
        AvailableDatasetSummary(
            id=dataset.id,
            table_name=dataset.table_name,
            schema=getattr(dataset, "schema", None),
            database_id=getattr(dataset, "database_id", None),
        )
        for dataset in accessible_datasets[:limit]
    ]
