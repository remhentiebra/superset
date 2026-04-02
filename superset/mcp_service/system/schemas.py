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
Pydantic schemas for system-level (instance/info) responses

This module contains Pydantic models for serializing Superset instance metadata and
system-level info.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, PositiveInt

from superset.mcp_service.constants import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE


class HealthCheckResponse(BaseModel):
    """Response model for health check.

    Used by health check tool to return service status and system information.
    """

    status: str
    timestamp: str
    service: str
    version: str
    python_version: str
    platform: str
    uptime_seconds: float | None = None


class GetSupersetInstanceInfoRequest(BaseModel):
    """
    Request schema for get_instance_info tool.

    Currently has no parameters but provides consistent API for future extensibility.
    """

    model_config = ConfigDict(
        extra="forbid",
    )


class InstanceSummary(BaseModel):
    total_dashboards: int
    total_charts: int
    total_datasets: int
    total_databases: int
    total_users: int
    total_roles: int
    total_tags: int
    avg_charts_per_dashboard: float


class RecentActivity(BaseModel):
    dashboards_created_last_30_days: int
    charts_created_last_30_days: int
    datasets_created_last_30_days: int
    dashboards_modified_last_7_days: int
    charts_modified_last_7_days: int
    datasets_modified_last_7_days: int


class DashboardBreakdown(BaseModel):
    published: int
    unpublished: int
    certified: int
    with_charts: int
    without_charts: int


class DatabaseBreakdown(BaseModel):
    by_type: Dict[str, int]


class PopularContent(BaseModel):
    top_tags: List[str] = Field(default_factory=list)
    top_creators: List[str] = Field(default_factory=list)


class FeatureAvailability(BaseModel):
    """Dynamic feature availability for the current user and deployment.

    Menus are detected at request time from the security manager,
    so they reflect the actual permissions of the requesting user.
    """

    accessible_menus: List[str] = Field(
        default_factory=list,
        description=(
            "UI menu items accessible to the current user, "
            "derived from FAB role permissions"
        ),
    )


class DatabaseSummary(BaseModel):
    """Minimal database metadata for discovery workflows."""

    id: int = Field(..., description="Database ID")
    database_name: str = Field(..., description="Database name")
    backend: str | None = Field(None, description="SQLAlchemy backend name")


class DatabaseInfo(DatabaseSummary):
    """Detailed database metadata for SQL and virtual-dataset workflows."""

    allow_file_upload: bool = Field(
        ..., description="Whether the database allows file uploads"
    )
    allows_virtual_datasets: bool = Field(
        ...,
        description="Whether the database supports virtual dataset exploration",
    )
    explore_database_id: int | None = Field(
        None,
        description="Database ID to use for explore workflows when different from id",
    )


class AvailableDatasetSummary(BaseModel):
    """Compact dataset metadata used by instance convenience resources."""

    model_config = ConfigDict(populate_by_name=True)

    id: int = Field(..., description="Dataset ID")
    table_name: str = Field(..., description="Dataset table name")
    schema_name: str | None = Field(None, description="Schema name", alias="schema")
    database_id: int | None = Field(None, description="Database ID")


class InstanceInfo(BaseModel):
    instance_summary: InstanceSummary
    recent_activity: RecentActivity
    dashboard_breakdown: DashboardBreakdown
    database_breakdown: DatabaseBreakdown
    popular_content: PopularContent
    current_user: UserInfo | None = Field(
        None,
        description=(
            "Use current_user.id with created_by_fk filter to find your own assets."
        ),
    )
    feature_availability: FeatureAvailability
    timestamp: datetime


class InstanceMetadata(InstanceInfo):
    """Extended instance metadata resource payload."""

    available_databases: List[DatabaseSummary] = Field(
        default_factory=list,
        description="Accessible databases available for SQL-first workflows",
    )
    available_datasets: List[AvailableDatasetSummary] = Field(
        default_factory=list,
        description="Recently modified accessible datasets",
    )


class UserInfo(BaseModel):
    id: int | None = None
    username: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None
    active: bool | None = None
    roles: List[str] = Field(
        default_factory=list,
        description=(
            "Role names assigned to the user (e.g., Admin, Alpha, Gamma, Viewer). "
            "Use this to determine what actions the user can perform."
        ),
    )


def serialize_user_object(user: Any) -> UserInfo | None:
    """Serialize a user ORM object to UserInfo, extracting role names as strings."""
    if not user:
        return None

    user_roles: list[str] = []
    if (raw_roles := getattr(user, "roles", None)) is not None:
        try:
            user_roles = [role.name for role in raw_roles if hasattr(role, "name")]
        except TypeError:
            user_roles = []

    return UserInfo(
        id=getattr(user, "id", None),
        username=getattr(user, "username", None),
        first_name=getattr(user, "first_name", None),
        last_name=getattr(user, "last_name", None),
        email=getattr(user, "email", None),
        active=getattr(user, "active", None),
        roles=user_roles,
    )


class TagInfo(BaseModel):
    id: int | None = None
    name: str | None = None
    type: str | None = None
    description: str | None = None


class RoleInfo(BaseModel):
    id: int | None = None
    name: str | None = None
    permissions: List[str] | None = None


class ListDatabasesRequest(BaseModel):
    """Request schema for listing accessible databases."""

    search: str | None = Field(
        None,
        description="Case-insensitive search across database name and backend",
    )
    backend: str | None = Field(
        None,
        description="Optional backend filter, for example postgres or clickhouse",
    )
    order_column: Literal["id", "database_name", "backend"] = Field(
        default="database_name",
        description="Sort column",
    )
    order_direction: Literal["asc", "desc"] = Field(
        default="asc",
        description="Sort direction",
    )
    page: PositiveInt = Field(default=1, description="1-based page number")
    page_size: PositiveInt = Field(
        default=DEFAULT_PAGE_SIZE,
        description="Results per page",
    )

    @field_validator("search", "backend")
    @classmethod
    def strip_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        return value or None

    @field_validator("page_size")
    @classmethod
    def clamp_page_size(cls, value: int) -> int:
        return min(value, MAX_PAGE_SIZE)


class ListDatabasesResponse(BaseModel):
    """Paginated database discovery response."""

    databases: List[DatabaseSummary] = Field(
        default_factory=list,
        description="Accessible databases matching the request",
    )
    count: int = Field(..., description="Number of databases in this page")
    total_count: int = Field(..., description="Total number of matching databases")
    page: int = Field(..., description="1-based page number")
    page_size: int = Field(..., description="Results per page")
    total_pages: int = Field(..., description="Total pages for the query")
    has_previous: bool = Field(..., description="Whether a previous page exists")
    has_next: bool = Field(..., description="Whether a next page exists")
    pagination: PaginationInfo | None = None
    timestamp: datetime | None = None
    model_config = ConfigDict(ser_json_timedelta="iso8601")


class GetDatabaseInfoRequest(BaseModel):
    """Request schema for retrieving one database by ID."""

    database_id: int = Field(..., description="Database ID")


class DatabaseError(BaseModel):
    """Error payload for database discovery tools."""

    error: str = Field(..., description="Error message")
    error_type: str = Field(..., description="Type of error")
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Error timestamp",
    )


class PaginationInfo(BaseModel):
    page: int
    page_size: int
    total_count: int
    total_pages: int
    has_next: bool
    has_previous: bool
    model_config = ConfigDict(ser_json_timedelta="iso8601")
