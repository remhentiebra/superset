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
Pydantic schemas for dashboard-related responses

This module contains Pydantic models for serializing dashboard data
in a consistent and type-safe manner.

Example usage:
    # For detailed dashboard info
    dashboard_info = DashboardInfo(
        id=1,
        dashboard_title="Sales Dashboard",
        published=True,
        tags=[TagInfo(id=1, name="sales")],
        charts=[DashboardChartSummary(id=1, slice_name="Sales Chart")],
    )

    # For dashboard list responses
    dashboard_list = DashboardList(
        dashboards=[
            DashboardInfo(
                id=1,
                dashboard_title="Sales Dashboard",
                published=True,
                tags=[TagInfo(id=1, name="sales")]
            )
        ],
        count=1,
        total_count=1,
        page=0,
        page_size=10,
        total_pages=1,
        has_next=False,
        has_previous=False,
        columns_requested=["id", "dashboard_title"],
        columns_loaded=["id", "dashboard_title", "published"],
        filters_applied={"published": True},
        pagination=PaginationInfo(
            page=0,
            page_size=10,
            total_count=1,
            total_pages=1,
            has_next=False,
            has_previous=False
        ),
        timestamp=datetime.now(timezone.utc)
    )
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Dict, List, Literal, TYPE_CHECKING

import humanize
from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_serializer,
    model_validator,
    PositiveInt,
)

if TYPE_CHECKING:
    from superset.models.dashboard import Dashboard

from superset.daos.base import ColumnOperator, ColumnOperatorEnum
from superset.mcp_service.chart.schemas import (
    ChartFilterConfig,
    MetricFilterConfig,
)
from superset.mcp_service.common.cache_schemas import (
    CreatedByMeMixin,
    MetadataCacheControl,
    OwnedByMeMixin,
)
from superset.mcp_service.constants import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from superset.mcp_service.privacy import (
    filter_user_directory_fields,
    user_can_view_data_model_metadata,
)
from superset.mcp_service.system.schemas import (
    PaginationInfo,
    RoleInfo,
    TagInfo,
)
from superset.mcp_service.utils import (
    escape_llm_context_delimiters,
    sanitize_for_llm_context,
)
from superset.mcp_service.utils.sanitization import (
    sanitize_user_input,
    sanitize_user_input_with_changes,
)
from superset.utils.json import loads as json_loads


class DashboardError(BaseModel):
    """Error response for dashboard operations"""

    error: str = Field(..., description="Error message")
    error_type: str = Field(..., description="Type of error")
    timestamp: str | datetime | None = Field(None, description="Error timestamp")

    model_config = ConfigDict(ser_json_timedelta="iso8601")

    @field_validator("error")
    @classmethod
    def sanitize_error_for_llm_context(cls, value: str) -> str:
        """Wrap error text before it is exposed to LLM context."""
        return sanitize_for_llm_context(value, field_path=("error",))

    @classmethod
    def create(cls, error: str, error_type: str) -> "DashboardError":
        """Create a standardized DashboardError with timestamp."""
        from datetime import datetime

        return cls(error=error, error_type=error_type, timestamp=datetime.now())


def serialize_tag_object(tag: Any) -> TagInfo | None:
    """Serialize a tag object to TagInfo"""
    if not tag:
        return None

    return TagInfo(
        id=getattr(tag, "id", None),
        name=getattr(tag, "name", None),
        type=getattr(tag, "type", None),
        description=getattr(tag, "description", None),
    )


def serialize_role_object(role: Any) -> RoleInfo | None:
    """Serialize a role object to RoleInfo"""
    if not role:
        return None

    return RoleInfo(
        id=getattr(role, "id", None),
        name=getattr(role, "name", None),
        permissions=[perm.name for perm in getattr(role, "permissions", [])]
        if hasattr(role, "permissions")
        else None,
    )


class DashboardFilter(ColumnOperator):
    """
    Filter object for dashboard listing.
    col: The column to filter on. Must be one of the allowed filter fields.
    opr: The operator to use. Must be one of the supported operators.
    value: The value to filter by (type depends on col and opr).
    """

    col: Literal[
        "dashboard_title",
        "published",
        "created_by_fk",
        "owner",
        "favorite",
    ] = Field(
        ...,
        description=(
            "Column to filter on. Use "
            "get_schema(model_type='dashboard') for available filter columns."
        ),
    )
    opr: ColumnOperatorEnum = Field(
        ...,
        description="Operator to use. Use get_schema(model_type='dashboard') for "
        "available operators.",
    )
    value: str | int | float | bool | List[str | int | float | bool] = Field(
        ..., description="Value to filter by (type depends on col and opr)"
    )


class ListDashboardsRequest(OwnedByMeMixin, CreatedByMeMixin, MetadataCacheControl):
    """Request schema for list_dashboards with clear, unambiguous types."""

    filters: Annotated[
        List[DashboardFilter],
        Field(
            default_factory=list,
            description="List of filter objects (column, operator, value). Each "
            "filter is an object with 'col', 'opr', and 'value' properties. "
            "Cannot be used together with 'search'.",
        ),
    ]
    select_columns: Annotated[
        List[str],
        Field(
            default_factory=list,
            description="List of columns to select. Defaults to common columns "
            "if not specified.",
        ),
    ]

    @field_validator("filters", mode="before")
    @classmethod
    def parse_filters(cls, v: Any) -> List[DashboardFilter]:
        """
        Parse filters from JSON string or list.

        Handles Claude Code bug where objects are double-serialized as strings.
        See: https://github.com/anthropics/claude-code/issues/5504
        """
        from superset.mcp_service.utils.schema_utils import parse_json_or_model_list

        return parse_json_or_model_list(v, DashboardFilter, "filters")

    @field_validator("select_columns", mode="before")
    @classmethod
    def parse_select_columns(cls, v: Any) -> List[str]:
        """
        Parse select_columns from JSON string, list, or CSV string.

        Handles Claude Code bug where arrays are double-serialized as strings.
        See: https://github.com/anthropics/claude-code/issues/5504
        """
        from superset.mcp_service.utils.schema_utils import parse_json_or_list

        return parse_json_or_list(v, "select_columns")

    search: Annotated[
        str | None,
        Field(
            default=None,
            description="Text search string to match against dashboard fields. "
            "Cannot be used together with 'filters'.",
        ),
    ]
    order_column: Annotated[
        str | None, Field(default=None, description="Column to order results by")
    ]
    order_direction: Annotated[
        Literal["asc", "desc"],
        Field(
            default="asc", description="Direction to order results ('asc' or 'desc')"
        ),
    ]
    page: Annotated[
        PositiveInt,
        Field(default=1, description="Page number for pagination (1-based)"),
    ]
    page_size: Annotated[
        int,
        Field(
            default=DEFAULT_PAGE_SIZE,
            gt=0,
            le=MAX_PAGE_SIZE,
            description=f"Number of items per page (max {MAX_PAGE_SIZE})",
        ),
    ]

    @model_validator(mode="after")
    def validate_search_and_filters(self) -> "ListDashboardsRequest":
        """Prevent using both search and filters simultaneously."""
        if self.search and self.filters:
            raise ValueError(
                "Cannot use both 'search' and 'filters' parameters simultaneously. "
                "Use either 'search' for text-based searching across multiple fields, "
                "or 'filters' for precise column-based filtering, but not both."
            )
        return self


class GetDashboardInfoRequest(MetadataCacheControl):
    """Request schema for get_dashboard_info with support for ID, UUID, or slug.

    When permalink_key is provided, the tool will retrieve the dashboard's filter
    state from the permalink, allowing you to see what filters the user has applied
    (not just the default filter state). This is useful when a user applies filters
    in a dashboard but the URL contains a permalink_key.
    """

    identifier: Annotated[
        int | str,
        Field(
            description="Dashboard identifier - can be numeric ID, UUID string, or slug"
        ),
    ]
    permalink_key: str | None = Field(
        default=None,
        description=(
            "Optional permalink key for retrieving dashboard filter state. When a "
            "user applies filters in a dashboard, the state can be persisted in a "
            "permalink. If provided, the tool returns the filter configuration "
            "from that permalink."
        ),
    )


class NativeFilterSummary(BaseModel):
    """Lightweight summary of a dashboard native filter."""

    id: str | None = Field(None, description="Native filter ID")
    name: str | None = Field(None, description="Native filter name")
    filter_type: str | None = Field(None, description="Native filter type")
    targets: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Native filter targets",
    )


class DashboardChartSummary(BaseModel):
    """Serialize only the core chart fields needed for dashboard context."""

    id: int | None = Field(None, description="Chart ID")
    slice_name: str | None = Field(None, description="Chart name")
    viz_type: str | None = Field(None, description="Visualization type")
    datasource_name: str | None = Field(None, description="Datasource name")
    url: str | None = Field(None, description="Chart explore page URL")
    description: str | None = Field(None, description="Chart description")


class DashboardInfo(BaseModel):
    id: int | None = None
    dashboard_title: str | None = None
    slug: str | None = None
    description: str | None = None
    css: str | None = None
    certified_by: str | None = None
    certification_details: str | None = None
    published: bool | None = None
    is_managed_externally: bool | None = None
    external_url: str | None = None
    created_on: str | datetime | None = None
    changed_on: str | datetime | None = None
    uuid: str | None = None
    url: str | None = None
    created_on_humanized: str | None = None
    changed_on_humanized: str | None = None
    chart_count: int = 0
    tags: List[TagInfo] = Field(default_factory=list)
    charts: List[DashboardChartSummary] = Field(default_factory=list)

    # Structured filter information extracted from json_metadata
    native_filters: List[NativeFilterSummary] = Field(
        default_factory=list,
        description=(
            "Native filters configured on this dashboard. Extracted from "
            "json_metadata for LLM consumption. Includes filter name/type, "
            "and target columns only when data-model metadata is allowed."
        ),
    )
    cross_filters_enabled: bool | None = Field(
        None,
        description="Whether cross-filtering between charts is enabled.",
    )

    # Omission metadata — tells the agent what was stripped and why
    omitted_fields: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Fields omitted from this response to reduce size. Keys are field "
            "names, values describe what was omitted and how to access the full "
            "data. Useful filter information has been extracted into "
            "native_filters and cross_filters_enabled above."
        ),
    )

    # Fields for permalink/filter state support
    permalink_key: str | None = Field(
        None,
        description=(
            "Permalink key used to retrieve filter state. When present, indicates "
            "the filter_state came from a permalink rather than the default dashboard."
        ),
    )
    filter_state: Dict[str, Any] | None = Field(
        None,
        description=(
            "Filter state from permalink. Contains dataMask (native filter values), "
            "activeTabs, anchor, and urlParams. When present, represents the actual "
            "filters the user has applied to the dashboard. For users without "
            "data-model metadata access, dataMask and chartStates are omitted."
        ),
    )
    is_permalink_state: bool = Field(
        default=False,
        description=(
            "True if the filter_state came from a permalink rather than the default "
            "dashboard configuration. When true, the filter_state reflects what the "
            "user sees in the dashboard, not the default filter state."
        ),
    )

    model_config = ConfigDict(from_attributes=True, ser_json_timedelta="iso8601")

    @model_serializer(mode="wrap")
    def _filter_fields_by_context(self, serializer: Any, info: Any) -> Dict[str, Any]:
        """Filter fields based on serialization context.

        If context contains 'select_columns', only include those fields.
        Otherwise, include all fields (default behavior).
        """
        # Get full serialization
        data = filter_user_directory_fields(serializer(self))

        # Check if we have a context with select_columns
        if info.context and isinstance(info.context, dict):
            select_columns = info.context.get("select_columns")
            if select_columns:
                # Filter to only requested fields
                return {k: v for k, v in data.items() if k in select_columns}

        return data


def serialize_chart_summary(
    chart: Any,
    *,
    include_data_model_metadata: bool = False,
) -> DashboardChartSummary | None:
    """Serialize only the core chart fields needed for dashboard context."""
    if not chart:
        return None

    from superset.mcp_service.utils.url_utils import get_superset_base_url

    chart_id = getattr(chart, "id", None)
    chart_url = None
    if chart_id is not None:
        chart_url = f"{get_superset_base_url()}/explore/?slice_id={chart_id}"

    return DashboardChartSummary(
        id=chart_id,
        slice_name=getattr(chart, "slice_name", None),
        viz_type=getattr(chart, "viz_type", None),
        datasource_name=getattr(chart, "datasource_name", None)
        if include_data_model_metadata
        else None,
        url=chart_url,
        description=getattr(chart, "description", None),
    )


class DashboardList(BaseModel):
    dashboards: List[DashboardInfo]
    count: int
    total_count: int
    page: int
    page_size: int
    total_pages: int
    has_previous: bool
    has_next: bool
    columns_requested: List[str] = Field(
        default_factory=list,
        description="Requested columns for the response",
    )
    columns_loaded: List[str] = Field(
        default_factory=list,
        description="Columns that were actually loaded for each dashboard",
    )
    columns_available: List[str] = Field(
        default_factory=list,
        description="All columns available for selection via select_columns parameter",
    )
    sortable_columns: List[str] = Field(
        default_factory=list,
        description="Columns that can be used with order_column parameter",
    )
    filters_applied: List[DashboardFilter] = Field(
        default_factory=list,
        description="List of advanced filter dicts applied to the query.",
    )
    pagination: PaginationInfo | None = None
    timestamp: datetime | None = None
    model_config = ConfigDict(ser_json_timedelta="iso8601")


class AddChartToDashboardRequest(BaseModel):
    """Request schema for adding a chart to an existing dashboard."""

    dashboard_id: int = Field(
        ..., description="ID of the dashboard to add the chart to"
    )
    chart_id: int = Field(..., description="ID of the chart to add to the dashboard")
    target_tab: str | None = Field(
        None, description="Target tab name (if dashboard has tabs)"
    )


class AddChartToDashboardResponse(BaseModel):
    """Response schema for adding chart to dashboard."""

    dashboard: DashboardInfo | None = Field(
        None, description="The updated dashboard info, if successful"
    )
    dashboard_url: str | None = Field(
        None, description="URL to view the updated dashboard"
    )
    position: dict[str, Any] | None = Field(
        None, description="Position information for the added chart"
    )
    error: str | None = Field(None, description="Error message, if operation failed")
    permission_denied: bool = Field(
        default=False,
        description=(
            "True when the operation failed because the current user does not "
            "have edit rights on the target dashboard. When True, inform the "
            "user and ask if they would like a new dashboard created instead. "
            "Do NOT silently create a new dashboard — always confirm first."
        ),
    )


class GenerateDashboardRequest(BaseModel):
    """Request schema for generating a dashboard."""

    model_config = ConfigDict(populate_by_name=True)

    chart_ids: List[int] = Field(
        ..., description="List of chart IDs to include in the dashboard", min_length=1
    )
    dashboard_title: str | None = Field(
        None,
        description=(
            "Title for the new dashboard. When omitted a descriptive title "
            "is generated from the included chart names."
        ),
        validation_alias=AliasChoices("dashboard_title", "title", "name"),
    )
    description: str | None = Field(None, description="Description for the dashboard")
    published: bool = Field(
        default=False, description="Whether to publish the dashboard"
    )
    sanitization_warnings: List[str] = Field(
        default_factory=list,
        description=(
            "Internal: warnings emitted when user input was altered by "
            "sanitization. Populated by the ``mode='before'`` validator "
            "before dashboard_title is rewritten, so the tool can surface "
            "a notice to the caller instead of silently dropping content."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _detect_dashboard_title_sanitization(cls, data: Any) -> Any:
        """Reject empty-after-sanitization titles and warn on partial strip.

        Runs before the ``dashboard_title`` field validator rewrites the
        value. If the caller supplied a non-empty title and sanitization
        would strip it entirely (XSS-only content), we raise so the caller
        gets a clear error instead of a blank-titled dashboard. When the
        sanitizer only trims part of the title, we record a warning the
        tool can return alongside the successful result.

        ``sanitization_warnings`` is a server-only field — any value the
        caller supplied is discarded here so the tool cannot be tricked
        into echoing attacker-controlled text back through the response.
        """
        if not isinstance(data, dict):
            return data
        data["sanitization_warnings"] = []
        for key in ("dashboard_title", "title", "name"):
            if key in data:
                raw = data[key]
                break
        else:
            raw = None
        if not isinstance(raw, str) or not raw.strip():
            return data
        sanitized, was_modified = sanitize_user_input_with_changes(
            raw, "Dashboard title", max_length=500, allow_empty=True
        )
        if was_modified and not sanitized:
            raise ValueError(
                "dashboard_title contained only disallowed content "
                "(HTML/script/URL schemes) and was removed entirely by "
                "sanitization. Provide a dashboard_title with plain text, "
                "or omit it to auto-generate one from chart names."
            )
        if was_modified:
            data["sanitization_warnings"].append(
                "dashboard_title was modified during sanitization to "
                "remove potentially unsafe content; the stored title "
                "differs from the input."
            )
        return data

    @field_validator("dashboard_title")
    @classmethod
    def sanitize_dashboard_title(cls, v: str | None) -> str | None:
        """Sanitize dashboard title to prevent XSS.

        Preserves an explicit empty string (caller-provided ``""``) rather
        than collapsing it to ``None``, since the tool treats ``None`` as
        "auto-generate a title from charts" but an explicit empty string
        as an intentional blank title.
        """
        if v is None or v == "":
            return v
        return sanitize_user_input(
            v, "Dashboard title", max_length=500, allow_empty=True
        )


class GenerateDashboardResponse(BaseModel):
    """Response schema for dashboard generation."""

    dashboard: DashboardInfo | None = Field(
        None, description="The created dashboard info, if successful"
    )
    dashboard_url: str | None = Field(None, description="URL to view the dashboard")
    error: str | None = Field(None, description="Error message, if creation failed")
    warnings: List[str] = Field(
        default_factory=list,
        description=(
            "Non-fatal advisory messages about the created dashboard — "
            "for example, that the supplied title was altered by "
            "sanitization."
        ),
    )


class DashboardMutationResponse(BaseModel):
    """Shared response for dashboard mutation tools."""

    dashboard: DashboardInfo | None = Field(
        None, description="Updated dashboard information, if successful"
    )
    dashboard_url: str | None = Field(
        None, description="URL to view the updated dashboard"
    )
    error: DashboardError | None = Field(
        None, description="Structured error details, if operation failed"
    )


class DashboardInsertEmptyRowAction(BaseModel):
    """Insert an empty row container into the dashboard grid."""

    action: Literal["insert_empty"] = Field(
        ...,
        description="Insert an empty row container at row_index",
    )
    row_index: int = Field(
        ...,
        ge=0,
        description=(
            "Zero-based row index where the empty row should be inserted. "
            "Use the current row count to append a row."
        ),
    )


class DashboardMoveRowAction(BaseModel):
    """Move an existing row container to a different grid index."""

    action: Literal["move_row"] = Field(
        ...,
        description="Move an existing row to target_row_index",
    )
    row_index: int = Field(
        ...,
        ge=0,
        description="Zero-based source row index in the current layout",
    )
    target_row_index: int = Field(
        ...,
        ge=0,
        description="Zero-based destination row index after the move",
    )

    @model_validator(mode="after")
    def validate_target_index(self) -> "DashboardMoveRowAction":
        if self.row_index == self.target_row_index:
            raise ValueError("row_index and target_row_index must differ")
        return self


class DashboardRemoveEmptyRowAction(BaseModel):
    """Remove an empty row container from the dashboard grid."""

    action: Literal["remove_empty"] = Field(
        ...,
        description="Remove an empty row container at row_index",
    )
    row_index: int = Field(
        ...,
        ge=0,
        description="Zero-based row index of the empty row to remove",
    )


DashboardRowAction = Annotated[
    DashboardInsertEmptyRowAction
    | DashboardMoveRowAction
    | DashboardRemoveEmptyRowAction,
    Field(discriminator="action"),
]


class UpdateDashboardRequest(BaseModel):
    """Typed request schema for updating dashboard metadata and layout."""

    identifier: int | str = Field(
        ...,
        description="Dashboard identifier - can be numeric ID, UUID string, or slug",
    )
    dashboard_title: str | None = Field(
        None,
        description="Updated dashboard title",
        max_length=500,
    )
    description: str | None = Field(
        None,
        description="Updated dashboard description",
        max_length=5000,
    )
    slug: str | None = Field(
        None,
        description="Updated dashboard slug",
        max_length=255,
    )
    css: str | None = Field(
        None,
        description="Updated dashboard CSS",
        max_length=50000,
    )
    published: bool | None = Field(
        None,
        description="Whether the dashboard should be published",
    )
    cross_filters_enabled: bool | None = Field(
        None,
        description="Toggle dashboard cross-filter interactions",
    )
    chart_ids: List[int] | None = Field(
        None,
        description=(
            "Optional exact chart IDs and order for rebuilding the dashboard "
            "layout as a simple auto-grid. When omitted, the current layout is kept."
        ),
        min_length=1,
    )
    layout_rows: List["DashboardLayoutRow"] | None = Field(
        None,
        description=(
            "Optional explicit row layout. Each row lists chart IDs to place "
            "together. When provided, this replaces chart_ids auto-grid layout."
        ),
        min_length=1,
    )
    chart_dimensions: List["DashboardChartDimensions"] | None = Field(
        None,
        description=(
            "Optional chart size updates applied to the current or rebuilt layout "
            "without requiring raw position_json edits."
        ),
        min_length=1,
    )
    chart_moves: List["DashboardChartMove"] | None = Field(
        None,
        description=(
            "Optional chart move/reorder actions applied to the current layout "
            "without rebuilding the full dashboard."
        ),
        min_length=1,
    )
    row_actions: List[DashboardRowAction] | None = Field(
        None,
        description=(
            "Optional row/container actions applied to the current layout, such "
            "as inserting an empty row, moving a row, or removing an empty row."
        ),
        min_length=1,
    )

    @field_validator("chart_ids")
    @classmethod
    def validate_unique_chart_ids(cls, value: List[int] | None) -> List[int] | None:
        if value and len(value) != len(set(value)):
            raise ValueError("chart_ids must not contain duplicates")
        return value

    @model_validator(mode="after")
    def validate_layout_inputs(self) -> "UpdateDashboardRequest":
        if self.chart_ids is not None and self.layout_rows is not None:
            raise ValueError("Use either chart_ids or layout_rows, not both")

        if self.layout_rows:
            flattened = [
                chart_id for row in self.layout_rows for chart_id in row.chart_ids
            ]
            if len(flattened) != len(set(flattened)):
                raise ValueError("layout_rows must not contain duplicate chart IDs")

        if self.chart_dimensions:
            chart_ids = [item.chart_id for item in self.chart_dimensions]
            if len(chart_ids) != len(set(chart_ids)):
                raise ValueError(
                    "chart_dimensions must not contain duplicate chart IDs"
                )

        if self.chart_moves:
            chart_ids = [item.chart_id for item in self.chart_moves]
            if len(chart_ids) != len(set(chart_ids)):
                raise ValueError("chart_moves must not contain duplicate chart IDs")
            if self.chart_ids is not None or self.layout_rows is not None:
                raise ValueError(
                    "chart_moves cannot be combined with chart_ids or layout_rows"
                )

        if self.row_actions and (
            self.chart_ids is not None or self.layout_rows is not None
        ):
            raise ValueError(
                "row_actions cannot be combined with chart_ids or layout_rows"
            )

        return self


class RemoveChartFromDashboardRequest(BaseModel):
    """Typed request schema for removing a chart from a dashboard."""

    identifier: int | str = Field(
        ...,
        description="Dashboard identifier - can be numeric ID, UUID string, or slug",
    )
    chart_id: int = Field(..., description="Chart ID to remove from the dashboard")


class NativeFilterTarget(BaseModel):
    """Dataset/column target for a native filter."""

    dataset_id: int | str = Field(
        ...,
        description="Target dataset identifier (ID or UUID)",
    )
    column: str = Field(..., description="Target column name", min_length=1)


class DashboardLayoutRow(BaseModel):
    """Typed row definition for explicit dashboard layouts."""

    chart_ids: List[int] = Field(
        ...,
        description="Chart IDs to place in this row from left to right",
        min_length=1,
    )

    @field_validator("chart_ids")
    @classmethod
    def validate_unique_chart_ids(cls, value: List[int]) -> List[int]:
        if len(value) != len(set(value)):
            raise ValueError("Dashboard row chart_ids must not contain duplicates")
        return value


class DashboardChartDimensions(BaseModel):
    """Typed chart sizing update."""

    chart_id: int = Field(..., description="Chart ID to resize in the layout")
    width: int | None = Field(
        None,
        description="Chart width in dashboard grid columns (1-12)",
        ge=1,
        le=12,
    )
    height: int | None = Field(
        None,
        description="Chart height in dashboard grid units",
        ge=1,
        le=100,
    )

    @model_validator(mode="after")
    def validate_dimensions_present(self) -> "DashboardChartDimensions":
        if self.width is None and self.height is None:
            raise ValueError("At least one of width or height must be provided")
        return self


class DashboardChartMove(BaseModel):
    """Typed chart move/reorder action for dashboard layouts."""

    chart_id: int = Field(..., description="Chart ID to move in the dashboard layout")
    row_index: int = Field(
        ...,
        ge=0,
        description=(
            "Zero-based destination row index. Use the current row count to append "
            "a row."
        ),
    )
    column_index: int = Field(
        ...,
        ge=0,
        description="Zero-based destination column index within the target row.",
    )


class DashboardNativeFilterConfig(BaseModel):
    """Typed MCP representation of a dashboard native filter."""

    id: str | None = Field(
        None,
        description="Existing native filter ID. Omit to create a new filter.",
    )
    name: str = Field(..., description="Filter display name", min_length=1)
    description: str = Field("", description="Optional filter description")
    filter_type: Literal["filter_select", "filter_range", "filter_time"] = Field(
        ...,
        description="Native filter control type",
    )
    target: NativeFilterTarget = Field(..., description="Target dataset and column")
    charts_in_scope: List[int] = Field(
        default_factory=list,
        description="Chart IDs included in this filter's scope",
    )
    excluded_charts: List[int] = Field(
        default_factory=list,
        description="Chart IDs excluded from the scope root",
    )
    tabs_in_scope: List[str] = Field(
        default_factory=list,
        description="Optional dashboard tab component IDs included in scope",
    )
    root_path: List[str] = Field(
        default_factory=lambda: ["ROOT_ID"],
        description=(
            "Dashboard layout root path for this filter scope. Use ROOT_ID for the "
            "whole dashboard or include tab/container IDs for narrower scope."
        ),
        min_length=1,
    )
    cascade_parent_ids: List[str] = Field(
        default_factory=list,
        description="Native filter IDs that act as cascade parents",
    )
    default_value: str | int | float | bool | List[str | int | float | bool] | None = (
        Field(
            None,
            description=(
                "Optional default filter value. Select filters accept a scalar or "
                "list. Range filters should use a two-item list."
            ),
        )
    )
    default_time_range: str | None = Field(
        None,
        description="Optional default time range for filter_time controls",
        max_length=255,
    )
    enable_empty_filter: bool = False
    default_to_first_item: bool = False
    creatable: bool = False
    multi_select: bool = False
    inverse_selection: bool = False
    search_all_options: bool = False
    sort_ascending: bool = True
    sort_metric: str | None = Field(
        None,
        description="Optional metric label used to sort select filter options",
        max_length=255,
    )
    time_range: str | None = Field(
        None,
        description="Optional time range applied when fetching filter options",
        max_length=255,
    )
    granularity_sqla: str | None = Field(
        None,
        description="Optional temporal column used with time_range prefiltering",
        max_length=255,
    )
    adhoc_filters: List[ChartFilterConfig] = Field(
        default_factory=list,
        description="Optional typed row-level prefilters for filter option queries",
    )

    def _validate_duplicate_scope_inputs(self) -> None:
        if len(self.charts_in_scope) != len(set(self.charts_in_scope)):
            raise ValueError("charts_in_scope must not contain duplicates")
        if len(self.excluded_charts) != len(set(self.excluded_charts)):
            raise ValueError("excluded_charts must not contain duplicates")
        if set(self.charts_in_scope).intersection(self.excluded_charts):
            raise ValueError("charts_in_scope and excluded_charts must not overlap")
        if len(self.root_path) != len(set(self.root_path)):
            raise ValueError("root_path must not contain duplicates")

    def _validate_time_filter_defaults(self) -> None:
        if self.filter_type != "filter_time":
            if self.default_time_range is not None:
                raise ValueError(
                    "default_time_range is only valid for filter_time filters"
                )
            return

        if self.default_value is not None:
            raise ValueError(
                "Use default_time_range instead of default_value for filter_time"
            )
        if self.time_range is not None or self.granularity_sqla is not None:
            raise ValueError(
                "time_range and granularity_sqla are not valid for filter_time filters"
            )
        if self.adhoc_filters:
            raise ValueError("adhoc_filters are not valid for filter_time filters")

    def _validate_select_only_controls(self) -> None:
        if self.filter_type == "filter_select":
            return
        if self.creatable:
            raise ValueError("creatable is only valid for filter_select filters")
        if self.sort_metric is not None:
            raise ValueError("sort_metric is only valid for filter_select filters")

    def _validate_adhoc_prefilters(self) -> None:
        for filter_config in self.adhoc_filters:
            if isinstance(filter_config, MetricFilterConfig):
                raise ValueError(
                    "metric_filter is not supported in native filter adhoc_filters"
                )

    @model_validator(mode="after")
    def validate_scope_and_defaults(self) -> "DashboardNativeFilterConfig":
        self._validate_duplicate_scope_inputs()
        self._validate_time_filter_defaults()
        self._validate_select_only_controls()
        self._validate_adhoc_prefilters()
        return self


class UpsertDashboardNativeFiltersRequest(BaseModel):
    """Typed request schema for upserting dashboard native filters."""

    identifier: int | str = Field(
        ...,
        description="Dashboard identifier - can be numeric ID, UUID string, or slug",
    )
    filters: List[DashboardNativeFilterConfig] = Field(
        ...,
        description="Typed native filters to create or update",
        min_length=1,
    )
    replace_existing: bool = Field(
        False,
        description=(
            "When true, delete existing native filters that are not present "
            "in the provided filters list."
        ),
    )


class DashboardNativeFiltersMutationResponse(DashboardMutationResponse):
    """Dashboard mutation response that also exposes updated native filter IDs."""

    native_filter_ids: List[str] = Field(
        default_factory=list,
        description="IDs of the native filters present after the update",
    )


def redact_filter_state_data_model_metadata(
    filter_state: Dict[str, Any],
) -> Dict[str, Any]:
    """Remove permalink filter state fields that expose data-model metadata."""
    return {
        key: value
        for key, value in filter_state.items()
        if key not in {"dataMask", "chartStates"}
    }


def _sanitize_dashboard_info_for_llm_context(
    dashboard_info: DashboardInfo,
) -> DashboardInfo:
    """Wrap dashboard read-path descriptive fields before LLM exposure."""
    payload = dashboard_info.model_dump(mode="python")

    for field_name in (
        "dashboard_title",
        "description",
        "css",
        "certified_by",
        "certification_details",
    ):
        payload[field_name] = sanitize_for_llm_context(
            payload.get(field_name),
            field_path=(field_name,),
        )

    payload["native_filters"] = [
        {
            **native_filter,
            "name": sanitize_for_llm_context(
                native_filter.get("name"),
                field_path=("native_filters", str(index), "name"),
            ),
            "targets": sanitize_for_llm_context(
                native_filter.get("targets", []),
                field_path=("native_filters", str(index), "targets"),
                excluded_field_names=frozenset(),
            ),
        }
        for index, native_filter in enumerate(payload.get("native_filters", []))
    ]

    payload["charts"] = [
        {
            **chart,
            "slice_name": sanitize_for_llm_context(
                chart.get("slice_name"),
                field_path=("charts", str(index), "slice_name"),
            ),
            "description": sanitize_for_llm_context(
                chart.get("description"),
                field_path=("charts", str(index), "description"),
            ),
            "datasource_name": escape_llm_context_delimiters(
                chart.get("datasource_name"),
            ),
        }
        for index, chart in enumerate(payload.get("charts", []))
    ]

    if payload.get("filter_state") is not None:
        payload["filter_state"] = sanitize_for_llm_context(
            payload["filter_state"],
            field_path=("filter_state",),
            excluded_field_names=frozenset(),
        )

    payload["tags"] = [
        {
            **tag,
            "name": sanitize_for_llm_context(
                tag.get("name"),
                field_path=("tags", str(index), "name"),
            ),
            "description": sanitize_for_llm_context(
                tag.get("description"),
                field_path=("tags", str(index), "description"),
            ),
        }
        for index, tag in enumerate(payload.get("tags", []))
    ]

    return DashboardInfo.model_validate(payload)


def dashboard_serializer(dashboard: "Dashboard") -> DashboardInfo:
    from superset.mcp_service.utils.url_utils import get_superset_base_url

    include_data_model_metadata = user_can_view_data_model_metadata()
    base_url = get_superset_base_url()
    relative_url = dashboard.url  # e.g. "/superset/dashboard/{slug_or_id}/"
    absolute_url = f"{base_url}{relative_url}" if relative_url else None
    json_metadata_str = getattr(dashboard, "json_metadata", None)
    position_json_str = getattr(dashboard, "position_json", None)

    return _sanitize_dashboard_info_for_llm_context(
        DashboardInfo(
            id=dashboard.id,
            dashboard_title=dashboard.dashboard_title or "Untitled",
            slug=dashboard.slug or "",
            description=dashboard.description,
            css=dashboard.css,
            certified_by=dashboard.certified_by,
            certification_details=dashboard.certification_details,
            published=dashboard.published,
            is_managed_externally=dashboard.is_managed_externally,
            external_url=dashboard.external_url,
            created_on=dashboard.created_on,
            changed_on=dashboard.changed_on,
            uuid=str(dashboard.uuid) if dashboard.uuid else None,
            url=absolute_url,
            created_on_humanized=dashboard.created_on_humanized,
            changed_on_humanized=dashboard.changed_on_humanized,
            chart_count=len(dashboard.slices) if dashboard.slices else 0,
            native_filters=_extract_native_filters(
                json_metadata_str,
                include_data_model_metadata=include_data_model_metadata,
            ),
            cross_filters_enabled=_extract_cross_filters_enabled(json_metadata_str),
            omitted_fields=_build_omitted_fields(
                json_metadata_str,
                position_json_str,
            ),
            tags=[
                TagInfo.model_validate(tag, from_attributes=True)
                for tag in dashboard.tags
            ]
            if dashboard.tags
            else [],
            charts=[
                summary
                for chart in dashboard.slices
                if (
                    summary := serialize_chart_summary(
                        chart,
                        include_data_model_metadata=include_data_model_metadata,
                    )
                )
                is not None
            ]
            if dashboard.slices
            else [],
        )
    )


def _humanize_timestamp(dt: datetime | None) -> str | None:
    """Convert a datetime to a humanized string like '2 hours ago'."""
    if dt is None:
        return None
    return humanize.naturaltime(datetime.now() - dt)


def _parse_json_metadata(json_metadata_str: str | None) -> Dict[str, Any] | None:
    """Parse json_metadata into a dict, returning None for invalid input."""
    if not json_metadata_str:
        return None
    try:
        metadata = json_loads(json_metadata_str)
    except (TypeError, ValueError):
        return None
    return metadata if isinstance(metadata, dict) else None


def _extract_native_filters(
    json_metadata_str: str | None,
    *,
    include_data_model_metadata: bool = False,
) -> List[NativeFilterSummary]:
    """Extract native filter summaries from raw json_metadata string."""
    metadata = _parse_json_metadata(json_metadata_str)
    if metadata is None:
        return []

    native_filters = metadata.get("native_filter_configuration", [])
    if not isinstance(native_filters, list):
        return []

    summaries: List[NativeFilterSummary] = []
    for native_filter in native_filters:
        if not isinstance(native_filter, dict):
            continue
        raw_targets = native_filter.get("targets", [])
        if not isinstance(raw_targets, list):
            raw_targets = []
        targets = (
            [target for target in raw_targets if isinstance(target, dict)]
            if include_data_model_metadata
            else []
        )
        summaries.append(
            NativeFilterSummary(
                id=native_filter.get("id"),
                name=native_filter.get("name"),
                filter_type=native_filter.get("filterType"),
                targets=targets,
            )
        )
    return summaries


def _extract_cross_filters_enabled(json_metadata_str: str | None) -> bool | None:
    """Extract the cross_filters_enabled flag from dashboard metadata."""
    metadata = _parse_json_metadata(json_metadata_str)
    if metadata is None:
        return None
    value = metadata.get("cross_filters_enabled")
    return value if isinstance(value, bool) else None


def _build_omitted_fields(
    json_metadata_str: str | None, position_json_str: str | None
) -> Dict[str, str]:
    """Build omission metadata describing which fields were stripped and why."""
    from superset.mcp_service.utils.response_utils import OmittedFieldsBuilder

    return (
        OmittedFieldsBuilder()
        .add_raw_field(
            "position_json",
            raw_value=position_json_str,
            reason=(
                "Internal layout tree with component positions/hierarchy. "
                "Not useful for analysis or LLM context."
            ),
        )
        .add_extracted_field(
            "json_metadata",
            raw_value=json_metadata_str,
            reason=(
                "native_filters and cross_filters_enabled extracted into "
                "dedicated fields above."
            ),
        )
        .build()
    )


def serialize_dashboard_object(dashboard: Any) -> DashboardInfo:
    """Simple dashboard serializer that safely handles object attributes."""
    from superset.mcp_service.utils.url_utils import get_superset_base_url

    # Construct URL from id/slug (the model's @property isn't available on
    # column-only query tuples returned by DAO.list with select_columns)
    dashboard_id = getattr(dashboard, "id", None)
    slug = getattr(dashboard, "slug", None)
    dashboard_url = None
    if dashboard_id is not None:
        dashboard_url = (
            f"{get_superset_base_url()}/superset/dashboard/{slug or dashboard_id}/"
        )

    json_metadata_str = getattr(dashboard, "json_metadata", None)
    position_json_str = getattr(dashboard, "position_json", None)
    include_data_model_metadata = user_can_view_data_model_metadata()
    return _sanitize_dashboard_info_for_llm_context(
        DashboardInfo(
            id=dashboard_id,
            dashboard_title=getattr(dashboard, "dashboard_title", None),
            slug=slug or "",
            url=dashboard_url,
            published=getattr(dashboard, "published", None),
            changed_on=getattr(dashboard, "changed_on", None),
            changed_on_humanized=_humanize_timestamp(
                getattr(dashboard, "changed_on", None)
            ),
            created_on=getattr(dashboard, "created_on", None),
            created_on_humanized=_humanize_timestamp(
                getattr(dashboard, "created_on", None)
            ),
            description=getattr(dashboard, "description", None),
            css=getattr(dashboard, "css", None),
            certified_by=getattr(dashboard, "certified_by", None),
            certification_details=getattr(dashboard, "certification_details", None),
            native_filters=_extract_native_filters(
                json_metadata_str,
                include_data_model_metadata=include_data_model_metadata,
            ),
            cross_filters_enabled=_extract_cross_filters_enabled(json_metadata_str),
            omitted_fields=_build_omitted_fields(json_metadata_str, position_json_str),
            is_managed_externally=getattr(dashboard, "is_managed_externally", None),
            external_url=getattr(dashboard, "external_url", None),
            uuid=str(getattr(dashboard, "uuid", ""))
            if getattr(dashboard, "uuid", None)
            else None,
            chart_count=len(getattr(dashboard, "slices", [])),
            tags=[
                TagInfo.model_validate(tag, from_attributes=True)
                for tag in getattr(dashboard, "tags", [])
            ]
            if getattr(dashboard, "tags", None)
            else [],
            charts=[
                summary
                for chart in getattr(dashboard, "slices", [])
                if (
                    summary := serialize_chart_summary(
                        chart,
                        include_data_model_metadata=include_data_model_metadata,
                    )
                )
                is not None
            ]
            if getattr(dashboard, "slices", None)
            else [],
        )
    )
