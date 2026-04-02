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

"""Schemas for SQL Lab MCP tools."""

from datetime import datetime
from typing import Any, Literal

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
    PositiveInt,
)

from superset.mcp_service.chart.schemas import ChartConfig, GenerateChartResponse
from superset.mcp_service.common.cache_schemas import FormDataCacheControl
from superset.mcp_service.dataset.schemas import DatasetError, DatasetInfo


class ExecuteSqlRequest(BaseModel):
    """Request schema for executing SQL queries."""

    model_config = ConfigDict(populate_by_name=True)

    database_id: int = Field(
        ..., description="Database connection ID to execute query against"
    )
    sql: str = Field(
        ...,
        description="SQL query to execute (supports Jinja2 {{ var }} template syntax)",
        validation_alias=AliasChoices("sql", "query"),
    )
    schema_name: str | None = Field(
        None, description="Schema to use for query execution", alias="schema"
    )
    catalog: str | None = Field(None, description="Catalog name for query execution")
    limit: int | None = Field(
        default=None,
        description=(
            "Maximum number of rows to return. "
            "If not specified, respects the LIMIT in your SQL query. "
            "If specified, overrides any SQL LIMIT clause."
        ),
        ge=1,
        le=10000,
    )
    timeout: int = Field(
        default=30, description="Query timeout in seconds", ge=1, le=300
    )
    template_params: dict[str, Any] | None = Field(
        None, description="Jinja2 template parameters for SQL rendering"
    )
    dry_run: bool = Field(
        default=False,
        description="Return transformed SQL without executing (for debugging)",
    )
    force_refresh: bool = Field(
        default=False,
        description=(
            "Bypass cache and re-execute query. "
            "IMPORTANT: Only set to true when the user EXPLICITLY requests "
            "fresh/updated data (e.g., 'refresh', 'get latest', 're-run'). "
            "Default to false to reduce database load."
        ),
    )

    @field_validator("sql")
    @classmethod
    def sql_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("SQL query cannot be empty")
        return v.strip()


class ColumnInfo(BaseModel):
    """Column metadata information."""

    name: str = Field(..., description="Column name")
    type: str = Field(..., description="Column data type")
    is_nullable: bool | None = Field(None, description="Whether column allows NULL")


class StatementData(BaseModel):
    """Row data and column metadata for a single SQL statement."""

    rows: list[dict[str, Any]] = Field(
        ..., description="Result rows as list of dictionaries"
    )
    columns: list[ColumnInfo] = Field(..., description="Column metadata information")


class StatementInfo(BaseModel):
    """Information about a single SQL statement execution."""

    original_sql: str = Field(..., description="Original SQL as submitted")
    executed_sql: str = Field(
        ..., description="SQL after transformations (RLS, mutations, limits)"
    )
    row_count: int = Field(..., description="Number of rows returned/affected")
    execution_time_ms: float | None = Field(
        None, description="Statement execution time in milliseconds"
    )
    data: StatementData | None = Field(
        None,
        description=(
            "Row data and column metadata for this statement. "
            "Present for data-bearing statements (e.g., SELECT), "
            "absent for DML/DDL statements (e.g., SET, UPDATE)."
        ),
    )


class ExecuteSqlResponse(BaseModel):
    """Response schema for SQL execution results."""

    success: bool = Field(..., description="Whether query executed successfully")
    rows: list[dict[str, Any]] | None = Field(
        None, description="Query result rows as list of dictionaries"
    )
    columns: list[ColumnInfo] | None = Field(
        None, description="Column metadata information"
    )
    row_count: int | None = Field(None, description="Number of rows returned")
    affected_rows: int | None = Field(
        None, description="Number of rows affected (for DML queries)"
    )
    execution_time: float | None = Field(
        None, description="Query execution time in seconds"
    )
    error: str | None = Field(None, description="Error message if query failed")
    error_type: str | None = Field(None, description="Type of error if failed")
    statements: list[StatementInfo] | None = Field(
        None, description="Per-statement execution info (for multi-statement queries)"
    )
    multi_statement_warning: str | None = Field(
        None,
        description=(
            "Warning when multiple data-bearing statements were executed. "
            "The top-level rows/columns contain only the last "
            "data-bearing statement's results. "
            "Check each entry in the statements array for per-statement data."
        ),
    )


class SaveSqlQueryRequest(BaseModel):
    """Request schema for saving a SQL query."""

    database_id: int = Field(
        ..., description="Database connection ID the query runs against"
    )
    label: str = Field(
        ...,
        description="Name for the saved query (shown in Saved Queries list)",
        min_length=1,
        max_length=256,
    )
    sql: str = Field(
        ...,
        description="SQL query text to save",
    )
    schema_name: str | None = Field(
        None,
        description="Schema the query targets",
        alias="schema",
    )
    catalog: str | None = Field(None, description="Catalog name (if applicable)")
    description: str | None = Field(
        None, description="Optional description of the query"
    )

    @field_validator("sql")
    @classmethod
    def sql_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("SQL query cannot be empty")
        return v.strip()

    @field_validator("label")
    @classmethod
    def label_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Label cannot be empty")
        return v.strip()


class SaveSqlQueryResponse(BaseModel):
    """Response schema for a saved SQL query."""

    id: int = Field(..., description="Saved query ID")
    label: str = Field(..., description="Query name")
    sql: str = Field(..., description="SQL query text")
    database_id: int = Field(..., description="Database ID")
    schema_name: str | None = Field(None, description="Schema name", alias="schema")
    catalog: str | None = Field(None, description="Catalog name (if applicable)")
    description: str | None = Field(None, description="Query description")
    url: str = Field(
        ...,
        description=(
            "URL to open this saved query in SQL Lab (e.g., /sqllab?savedQueryId=42)"
        ),
    )


class OpenSqlLabRequest(BaseModel):
    """Request schema for opening SQL Lab with context."""

    model_config = ConfigDict(populate_by_name=True)

    database_connection_id: int = Field(
        ...,
        description="Database connection ID to use in SQL Lab",
        validation_alias=AliasChoices("database_connection_id", "database_id"),
    )
    schema_name: str | None = Field(
        None, description="Default schema to select in SQL Lab", alias="schema"
    )
    dataset_in_context: str | None = Field(
        None, description="Dataset name/table to provide as context"
    )
    sql: str | None = Field(
        None,
        description="SQL to pre-populate in the editor",
        validation_alias=AliasChoices("sql", "query"),
    )
    title: str | None = Field(None, description="Title for the SQL Lab tab/query")


class SqlLabResponse(BaseModel):
    """Response schema for SQL Lab URL generation."""

    model_config = ConfigDict(populate_by_name=True)

    url: str = Field(..., description="URL to open SQL Lab with context")
    database_id: int = Field(..., description="Database ID used")
    schema_name: str | None = Field(None, description="Schema selected", alias="schema")
    title: str | None = Field(None, description="Query title")
    error: str | None = Field(None, description="Error message if failed")


class SavedQueryInfo(BaseModel):
    """Saved query metadata returned by MCP SQL tools."""

    model_config = ConfigDict(populate_by_name=True)

    id: int = Field(..., description="Saved query ID")
    label: str = Field(..., description="Saved query label")
    sql: str = Field(..., description="Saved SQL text")
    database_id: int | None = Field(None, description="Database ID", alias="db_id")
    database_name: str | None = Field(None, description="Database name")
    schema_name: str | None = Field(None, description="Schema name", alias="schema")
    catalog: str | None = Field(None, description="Catalog name")
    description: str | None = Field(None, description="Saved query description")
    template_parameters: str | None = Field(
        None, description="Raw saved template parameter JSON"
    )
    changed_on: datetime | None = Field(None, description="Last modified timestamp")
    created_on: datetime | None = Field(None, description="Creation timestamp")
    url: str = Field(
        ...,
        description="URL to open this saved query in SQL Lab",
    )


class ListSavedQueriesRequest(BaseModel):
    """Request schema for listing the current user's saved queries."""

    search: str | None = Field(
        None,
        description="Text search across label, schema, description, and SQL",
    )
    database_id: int | None = Field(
        None,
        description="Filter saved queries by database ID",
    )
    schema_name: str | None = Field(
        None,
        alias="schema",
        description="Filter saved queries by schema",
    )
    order_column: str = Field(
        default="changed_on",
        description="Sort column (changed_on, created_on, label, schema)",
    )
    order_direction: str = Field(
        default="desc",
        description="Sort direction (asc or desc)",
    )
    page: PositiveInt = Field(default=1, description="1-based page number")
    page_size: PositiveInt = Field(default=10, description="Results per page")

    @field_validator("search", "schema_name")
    @classmethod
    def strip_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        return value or None

    @field_validator("order_column")
    @classmethod
    def validate_order_column(cls, value: str) -> str:
        allowed = {"changed_on", "created_on", "label", "schema"}
        if value not in allowed:
            raise ValueError(
                "order_column must be one of: changed_on, created_on, label, schema"
            )
        return value

    @field_validator("order_direction")
    @classmethod
    def validate_order_direction(cls, value: str) -> str:
        normalized = value.lower()
        if normalized not in {"asc", "desc"}:
            raise ValueError("order_direction must be 'asc' or 'desc'")
        return normalized


class SavedQueryListResponse(BaseModel):
    """Paginated saved-query list response."""

    saved_queries: list[SavedQueryInfo] = Field(
        default_factory=list,
        description="Saved queries visible to the current user",
    )
    count: int = Field(..., description="Number of saved queries in this page")
    total_count: int = Field(..., description="Total number of matching saved queries")
    page: int = Field(..., description="1-based page number")
    page_size: int = Field(..., description="Results per page")
    total_pages: int = Field(..., description="Total pages for the query")
    has_previous: bool = Field(..., description="Whether a previous page exists")
    has_next: bool = Field(..., description="Whether a next page exists")


class GetSavedQueryRequest(BaseModel):
    """Request schema for retrieving a saved query by ID."""

    identifier: int = Field(..., description="Saved query ID")


class CreateVirtualDatasetFromSavedQueryRequest(BaseModel):
    """Request schema for promoting a saved query into a virtual dataset."""

    model_config = ConfigDict(populate_by_name=True)

    saved_query_id: int = Field(..., description="Saved query ID to promote")
    table_name: str | None = Field(
        None,
        min_length=1,
        max_length=250,
        description="Optional dataset name. Defaults to the saved query label.",
    )
    description: str | None = Field(
        None,
        description="Optional dataset description override",
    )
    owners: list[int] = Field(
        default_factory=list,
        description="Optional owner user IDs for the created dataset",
    )
    normalize_columns: bool = Field(
        default=False,
        description="Whether Superset should normalize column names on fetch",
    )
    always_filter_main_dttm: bool = Field(
        default=False,
        description="Whether the main datetime column should always be filtered",
    )

    @field_validator("table_name")
    @classmethod
    def strip_optional_table_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("table_name cannot be empty")
        return value


class GenerateChartFromSavedQueryRequest(BaseModel):
    """Request schema for promoting a saved query into a chart workflow."""

    model_config = ConfigDict(populate_by_name=True)

    saved_query_id: int = Field(..., description="Saved query ID to promote")
    config: ChartConfig = Field(..., description="Typed chart configuration")
    dataset_name: str | None = Field(
        None,
        min_length=1,
        max_length=250,
        description="Optional intermediate virtual dataset name",
    )
    dataset_description: str | None = Field(
        None,
        description="Optional intermediate dataset description override",
    )
    chart_name: str | None = Field(
        None,
        max_length=255,
        description="Optional chart name override",
    )
    owners: list[int] = Field(
        default_factory=list,
        description="Optional owner user IDs for the created dataset",
    )
    save_chart: bool = Field(
        default=False,
        description="Save the generated chart permanently in Superset",
    )
    generate_preview: bool = Field(
        default=True,
        description="Generate chart previews alongside the response",
    )
    preview_formats: list[Literal["url", "ascii", "vega_lite", "table"]] = Field(
        default_factory=lambda: ["url"],
        description="Preview formats to request from the generated chart",
    )
    normalize_columns: bool = Field(
        default=False,
        description="Whether Superset should normalize column names on dataset fetch",
    )
    always_filter_main_dttm: bool = Field(
        default=False,
        description="Whether the main datetime column should always be filtered",
    )

    @field_validator("dataset_name", "chart_name")
    @classmethod
    def strip_optional_names(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("Optional names cannot be empty strings")
        return value

    @model_validator(mode="after")
    def validate_chart_output_request(self) -> "GenerateChartFromSavedQueryRequest":
        if not self.save_chart and not self.generate_preview:
            raise ValueError(
                "At least one of save_chart or generate_preview must be true"
            )
        return self


class GenerateExploreLinkFromSavedQueryRequest(FormDataCacheControl):
    """Request schema for promoting a saved query into an explore-link workflow."""

    model_config = ConfigDict(populate_by_name=True)

    saved_query_id: int = Field(..., description="Saved query ID to promote")
    config: ChartConfig = Field(..., description="Typed chart configuration")
    dataset_name: str | None = Field(
        None,
        min_length=1,
        max_length=250,
        description="Optional intermediate virtual dataset name",
    )
    dataset_description: str | None = Field(
        None,
        description="Optional intermediate dataset description override",
    )
    owners: list[int] = Field(
        default_factory=list,
        description="Optional owner user IDs for the created dataset",
    )
    normalize_columns: bool = Field(
        default=False,
        description="Whether Superset should normalize column names on dataset fetch",
    )
    always_filter_main_dttm: bool = Field(
        default=False,
        description="Whether the main datetime column should always be filtered",
    )

    @field_validator("dataset_name")
    @classmethod
    def strip_optional_dataset_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("dataset_name cannot be an empty string")
        return value


class GenerateChartFromSavedQueryResponse(BaseModel):
    """Combined response for saved-query dataset and chart promotion."""

    dataset: DatasetInfo | None = Field(
        None,
        description="The intermediate virtual dataset created from the saved query",
    )
    dataset_error: DatasetError | None = Field(
        None,
        description="Structured dataset creation error, if promotion failed early",
    )
    chart_response: GenerateChartResponse | None = Field(
        None,
        description="The chart response generated from that dataset",
    )


class ExploreLinkResponse(BaseModel):
    """Typed explore-link payload returned by explore URL workflows."""

    url: str = Field(..., description="Superset Explore URL")
    form_data: dict[str, Any] = Field(
        default_factory=dict,
        description="Resolved Superset form_data used to build the explore URL",
    )
    form_data_key: str | None = Field(
        None,
        description="Cached form_data key embedded in the explore URL, if available",
    )
    error: str | None = Field(
        None,
        description="Error message when explore-link generation fails",
    )


class GenerateExploreLinkFromSavedQueryResponse(BaseModel):
    """Combined response for saved-query dataset and explore-link promotion."""

    dataset: DatasetInfo | None = Field(
        None,
        description="The intermediate virtual dataset created from the saved query",
    )
    dataset_error: DatasetError | None = Field(
        None,
        description="Structured dataset creation error, if promotion failed early",
    )
    explore_response: ExploreLinkResponse | None = Field(
        None,
        description="The explore-link response generated from that dataset",
    )
