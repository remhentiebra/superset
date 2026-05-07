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
MCP tool: update_chart
"""

import logging
import time
from typing import Any

from fastmcp import Context
from sqlalchemy.exc import InvalidRequestError, SQLAlchemyError
from sqlalchemy.orm import joinedload
from superset_core.mcp.decorators import tool, ToolAnnotations

from superset.commands.exceptions import CommandException
from superset.exceptions import OAuth2Error, OAuth2RedirectError
from superset.extensions import event_logger
from superset.mcp_service.chart.chart_helpers import (
    extract_form_data_key_from_url,
    find_chart_by_identifier,
)
from superset.mcp_service.chart.chart_utils import (
    analyze_chart_capabilities,
    analyze_chart_semantics,
    generate_chart_name,
    map_config_to_form_data,
)
from superset.mcp_service.chart.compile import validate_and_compile
from superset.mcp_service.chart.performance_utils import (
    build_performance_metadata,
    record_stage,
)
from superset.mcp_service.chart.schemas import (
    AccessibilityMetadata,
    GenerateChartResponse,
    serialize_chart_object,
    UpdateChartRequest,
)
from superset.mcp_service.utils.oauth2_utils import (
    build_oauth2_redirect_message,
    OAUTH2_CONFIG_ERROR_MESSAGE,
)
from superset.mcp_service.utils.url_utils import get_superset_base_url
from superset.utils import json

logger = logging.getLogger(__name__)


def _reload_chart_for_response(chart: Any) -> Any:
    """Re-fetch a chart with relationships for response serialization."""
    from superset import db
    from superset.daos.chart import ChartDAO
    from superset.models.slice import Slice

    chart_id = getattr(chart, "id", None)
    if chart_id is None:
        return chart

    try:
        try:
            db.session.expunge(chart)
        except InvalidRequestError:
            pass
        db.session.expire_all()
        return (
            ChartDAO.find_by_id(
                chart_id,
                query_options=[
                    joinedload(Slice.owners),
                    joinedload(Slice.tags),
                ],
            )
            or chart
        )
    except SQLAlchemyError:
        logger.warning(
            "Re-fetch of chart %s failed; returning command result",
            chart_id,
            exc_info=True,
        )
        try:
            db.session.rollback()  # pylint: disable=consider-using-transaction
        except SQLAlchemyError:
            logger.warning(
                "Database rollback failed during chart re-fetch error handling",
                exc_info=True,
            )
        return chart


def _prefer_command_result_fields(reloaded_chart: Any, command_result: Any) -> Any:
    """Preserve core identity fields when the post-save reload returns stale data."""
    if reloaded_chart is None:
        return command_result
    for field_name in ("slice_name", "viz_type", "uuid"):
        command_value = getattr(command_result, field_name, None)
        if command_value is not None:
            try:
                setattr(reloaded_chart, field_name, command_value)
            except AttributeError:
                return command_result
    return reloaded_chart


def _validation_error_response(message: str, details: str) -> GenerateChartResponse:
    return GenerateChartResponse.model_validate(
        {
            "chart": None,
            "error": {
                "error_type": "ValidationError",
                "message": message,
                "details": details,
            },
            "success": False,
            "schema_version": "2.0",
            "api_version": "v1",
        }
    )


def _missing_config_or_name_error() -> GenerateChartResponse:
    return _validation_error_response(
        message="Either 'config' or 'chart_name' must be provided.",
        details=(
            "Either 'config' or 'chart_name' must be provided. "
            "Use config for visualization changes, chart_name for renaming."
        ),
    )


def _build_update_payload(
    request: UpdateChartRequest,
    chart: Any,
    parsed_config: Any = None,
) -> dict[str, Any] | GenerateChartResponse:
    """Build the update payload for a chart update.

    Returns a dict payload on success, or a GenerateChartResponse error
    when neither config nor chart_name is provided.
    ``parsed_config`` is the pre-parsed chart config from the caller.
    """
    if parsed_config is not None:
        dataset_id = chart.datasource_id if chart.datasource_id else None
        new_form_data = map_config_to_form_data(parsed_config, dataset_id=dataset_id)
        new_form_data.pop("_mcp_warnings", None)

        chart_name = (
            request.chart_name
            if request.chart_name
            else chart.slice_name or generate_chart_name(parsed_config)
        )

        return {
            "slice_name": chart_name,
            "viz_type": new_form_data["viz_type"],
            "params": json.dumps(new_form_data),
            # Clear stale query_context so get_chart_data uses the updated params.
            "query_context": None,
        }

    # Name-only update: keep existing visualization, just rename
    if not request.chart_name:
        return _missing_config_or_name_error()
    return {"slice_name": request.chart_name}


def _build_preview_form_data(
    request: UpdateChartRequest,
    chart: Any,
    parsed_config: Any = None,
) -> dict[str, Any] | GenerateChartResponse:
    """Merge the existing chart's form_data with the requested changes.

    Used by the preview-first flow so the user can review edits in Explore
    before clicking Save. Returns the merged form_data dict on success, or a
    GenerateChartResponse error when neither config nor chart_name is given.
    ``parsed_config`` is the pre-parsed chart config from the caller.
    """
    existing_form_data: dict[str, Any] = {}
    if getattr(chart, "params", None):
        try:
            existing_form_data = json.loads(chart.params) or {}
        except (ValueError, TypeError):
            logger.warning(
                "Failed to parse existing chart.params for chart %s", chart.id
            )
            existing_form_data = {}

    if parsed_config is not None:
        dataset_id = chart.datasource_id if chart.datasource_id else None
        new_form_data = map_config_to_form_data(parsed_config, dataset_id=dataset_id)
        new_form_data.pop("_mcp_warnings", None)
        merged = {**existing_form_data, **new_form_data}
    else:
        if not request.chart_name:
            return _missing_config_or_name_error()
        merged = dict(existing_form_data)

    if request.chart_name:
        merged["slice_name"] = request.chart_name
    elif chart.slice_name:
        merged["slice_name"] = chart.slice_name

    merged["slice_id"] = chart.id
    if chart.datasource_id:
        merged["datasource"] = f"{chart.datasource_id}__table"

    return merged


def _validate_update_against_dataset(
    parsed_config: Any,
    form_data: dict[str, Any],
    chart: Any,
) -> GenerateChartResponse | None:
    """Run Tier 1 (schema) + Tier 2 (compile) validation against the chart's
    dataset. Returns ``None`` on success, or a :class:`GenerateChartResponse`
    error envelope on failure that callers should return as-is.
    """
    from superset.daos.dataset import DatasetDAO

    dataset = getattr(chart, "datasource", None)
    if dataset is None and getattr(chart, "datasource_id", None) is not None:
        dataset = DatasetDAO.find_by_id(chart.datasource_id)
    if dataset is None:
        return GenerateChartResponse.model_validate(
            {
                "chart": None,
                "error": {
                    "error_type": "DatasetNotAccessible",
                    "message": "Chart's dataset is not accessible",
                    "details": (
                        f"Dataset {getattr(chart, 'datasource_id', None)} "
                        "is missing or inaccessible."
                    ),
                },
                "success": False,
                "schema_version": "2.0",
                "api_version": "v1",
            }
        )

    compile_result = validate_and_compile(
        parsed_config, form_data, dataset, run_compile_check=True
    )
    if compile_result.success:
        return None

    logger.warning(
        "update_chart validation failed for chart %s: %s",
        getattr(chart, "id", None),
        compile_result.error,
    )
    if compile_result.error_obj is not None:
        error_payload = compile_result.error_obj.model_dump()
    else:
        error_payload = {
            "error_type": "validation_error",
            "message": "Chart update validation failed",
            "details": compile_result.error or "",
            "error_code": compile_result.error_code,
            "suggestions": [],
        }
    return GenerateChartResponse.model_validate(
        {
            "chart": None,
            "error": error_payload,
            "success": False,
            "schema_version": "2.0",
            "api_version": "v1",
        }
    )


def _create_preview_url(
    chart: Any, form_data: dict[str, Any]
) -> tuple[str, str | None, list[str]]:
    """Cache form_data and return (explore_url, form_data_key, warnings)."""
    from superset.commands.explore.form_data.parameters import CommandParameters
    from superset.mcp_service.commands.create_form_data import MCPCreateFormDataCommand
    from superset.utils.core import DatasourceType

    base_url = get_superset_base_url()

    if not chart.datasource_id:
        warning = (
            "Chart has no datasource; the preview URL shows the saved chart state, "
            "not the pending changes. Open the URL and apply the changes manually."
        )
        logger.warning(
            "Chart %s has no datasource_id; preview URL cannot embed form_data.",
            chart.id,
        )
        return f"{base_url}/explore/?slice_id={chart.id}", None, [warning]

    cmd_params = CommandParameters(
        datasource_type=DatasourceType.TABLE,
        datasource_id=chart.datasource_id,
        chart_id=chart.id,
        tab_id=None,
        form_data=json.dumps(form_data),
    )
    form_data_key = MCPCreateFormDataCommand(cmd_params).run()
    explore_url = (
        f"{base_url}/explore/?form_data_key={form_data_key}&slice_id={chart.id}"
    )
    return explore_url, form_data_key, []


@tool(
    tags=["mutate"],
    class_permission_name="Chart",
    annotations=ToolAnnotations(
        title="Update chart",
        readOnlyHint=False,
        destructiveHint=True,
    ),
)
async def update_chart(  # noqa: C901
    request: UpdateChartRequest, ctx: Context
) -> GenerateChartResponse:
    """Update existing chart with new configuration.

    IMPORTANT BEHAVIOR:
    - By default (generate_preview=True), a preview explore URL is returned
      so the user can review changes and click Save to overwrite the original
      chart (if they have permission).
    - Set generate_preview=False to persist the update immediately.
    - LLM clients MUST display the returned explore URL to users.
    - Use numeric ID or UUID string to identify the chart (NOT chart name).
    - config is optional — omit it to rename a chart without changing its visualization

    Example usage (preview, default):
    ```json
    {
        "identifier": 123,
        "config": {
            "chart_type": "xy",
            "x": {"name": "date"},
            "y": [{"name": "sales", "aggregate": "SUM"}],
            "kind": "line"
        }
    }
    ```

    Rename only (no config required):
    ```json
    {
        "identifier": 123,
        "chart_name": "Q1 Revenue"
    }
    ```

    Example usage (persist immediately):
    ```json
    {
        "identifier": 123,
        "generate_preview": false,
        "config": {"chart_type": "table", "columns": [{"name": "region"}]}
    }
    ```

    Use when:
    - Modifying existing saved chart
    - Updating title, filters, or visualization settings
    - Changing chart type or data columns

    Returns:
    - Updated chart info, form_data (reflects what was saved), and metadata
    - Preview URL and explore URL for further editing
    """
    total_start_time = time.perf_counter()
    stage_durations_ms: dict[str, int] = {}

    try:
        with record_stage(stage_durations_ms, "chart_lookup"):
            with event_logger.log_context(action="mcp.update_chart.chart_lookup"):
                chart = find_chart_by_identifier(request.identifier)

        if not chart:
            return GenerateChartResponse.model_validate(
                {
                    "chart": None,
                    "error": {
                        "error_type": "NotFound",
                        "message": (
                            f"No chart found with identifier: {request.identifier}"
                        ),
                        "details": (
                            f"No chart found with identifier: {request.identifier}"
                        ),
                    },
                    "success": False,
                    "schema_version": "2.0",
                    "api_version": "v1",
                }
            )

        # Validate dataset access before allowing update.
        # check_chart_data_access is the centralized data-level
        # permission check that complements the class-level RBAC
        # enforced by mcp_auth_hook.
        from superset.mcp_service.auth import check_chart_data_access

        validation_result = check_chart_data_access(chart)
        if not validation_result.is_valid:
            error_msg = validation_result.error or "Chart's dataset is not accessible"
            return GenerateChartResponse.model_validate(
                {
                    "chart": None,
                    "error": {
                        "error_type": "DatasetNotAccessible",
                        "message": error_msg,
                        "details": error_msg,
                    },
                    "success": False,
                    "schema_version": "2.0",
                    "api_version": "v1",
                }
            )

        updated_chart: Any = None
        explore_url: str
        form_data_key: str | None = None
        warnings: list[str] = []
        saved = False
        new_form_data: dict[str, Any] | None = None

        # config is already a typed ChartConfig | None (validated by Pydantic)
        parsed_config = request.config

        if not request.generate_preview:
            from superset.commands.chart.update import UpdateChartCommand

            with record_stage(stage_durations_ms, "form_data_mapping"):
                payload_or_error = _build_update_payload(request, chart, parsed_config)
            if isinstance(payload_or_error, GenerateChartResponse):
                return payload_or_error

            # Extract form_data — present only for config updates, None for renames.
            if "params" in payload_or_error:
                new_form_data = json.loads(payload_or_error["params"])

            # Validate before persisting — catches bad column refs and runtime
            # SQL errors so we don't commit a chart that can't be queried.
            # Renames (no parsed_config) skip validation since form_data is
            # untouched.
            if parsed_config is not None and new_form_data is not None:
                with record_stage(stage_durations_ms, "validation"):
                    with event_logger.log_context(action="mcp.update_chart.validation"):
                        validation_error = _validate_update_against_dataset(
                            parsed_config, new_form_data, chart
                        )
                if validation_error is not None:
                    return validation_error

            with record_stage(stage_durations_ms, "save"):
                with event_logger.log_context(action="mcp.update_chart.db_write"):
                    command = UpdateChartCommand(chart.id, payload_or_error)
                    command_result = command.run()
                    updated_chart = _prefer_command_result_fields(
                        _reload_chart_for_response(command_result),
                        command_result,
                    )
            saved = True
            explore_url = (
                f"{get_superset_base_url()}/explore/?slice_id={updated_chart.id}"
            )
        else:
            with record_stage(stage_durations_ms, "form_data_mapping"):
                preview_or_error = _build_preview_form_data(
                    request, chart, parsed_config
                )
            if isinstance(preview_or_error, GenerateChartResponse):
                return preview_or_error

            # Validate before caching the form_data — same rationale as above.
            if parsed_config is not None:
                with record_stage(stage_durations_ms, "validation"):
                    with event_logger.log_context(action="mcp.update_chart.validation"):
                        validation_error = _validate_update_against_dataset(
                            parsed_config, preview_or_error, chart
                        )
                if validation_error is not None:
                    return validation_error

            with record_stage(stage_durations_ms, "preview_link"):
                with event_logger.log_context(action="mcp.update_chart.preview_link"):
                    explore_url, form_data_key, warnings = _create_preview_url(
                        chart, preview_or_error
                    )

        chart_for_analysis = updated_chart if saved else chart
        capabilities = analyze_chart_capabilities(chart_for_analysis, parsed_config)
        semantics = analyze_chart_semantics(chart_for_analysis, parsed_config)

        chart_name = (
            updated_chart.slice_name
            if saved and updated_chart and hasattr(updated_chart, "slice_name")
            else (
                request.chart_name
                or (chart.slice_name if hasattr(chart, "slice_name") else None)
                or (
                    generate_chart_name(parsed_config)
                    if parsed_config
                    else "Updated chart"
                )
            )
        )
        accessibility = AccessibilityMetadata(
            color_blind_safe=True,
            alt_text=(
                f"Updated chart showing {chart_name}"
                if saved
                else f"Updated chart preview showing {chart_name}"
            ),
            high_contrast_available=False,
        )

        previews: dict[str, Any] = {}
        if saved and updated_chart and request.preview_formats:
            try:
                with record_stage(stage_durations_ms, "preview_generation"):
                    with event_logger.log_context(action="mcp.update_chart.preview"):
                        from superset.mcp_service.chart.tool.get_chart_preview import (
                            _get_chart_preview_internal,
                            GetChartPreviewRequest,
                        )

                        for format_type in request.preview_formats:
                            preview_request = GetChartPreviewRequest(
                                identifier=str(updated_chart.id),
                                format=format_type,
                            )
                            preview_result = await _get_chart_preview_internal(
                                preview_request, ctx
                            )

                            if hasattr(preview_result, "content"):
                                previews[format_type] = preview_result.content

            except (
                OAuth2RedirectError,
                OAuth2Error,
                CommandException,
                SQLAlchemyError,
                KeyError,
                ValueError,
                TypeError,
                AttributeError,
            ) as e:
                logger.warning("Preview generation failed: %s", e)

        # Fallback: extract form_data_key from explore_url if not set
        if form_data_key is None:
            form_data_key = extract_form_data_key_from_url(explore_url)

        chart_id = updated_chart.id if saved and updated_chart else chart.id
        chart_uuid = (
            str(updated_chart.uuid)
            if saved and updated_chart and updated_chart.uuid
            else (str(chart.uuid) if chart.uuid else None)
        )
        viz_type = updated_chart.viz_type if saved and updated_chart else chart.viz_type

        response_assembly_start = time.perf_counter()
        if saved and updated_chart:
            try:
                chart_info = serialize_chart_object(updated_chart)
            except TypeError:
                chart_info = None
            chart_data = (
                chart_info.model_dump()
                if chart_info is not None
                else {
                    "id": updated_chart.id,
                    "slice_name": updated_chart.slice_name,
                    "viz_type": updated_chart.viz_type,
                    "url": (
                        f"{get_superset_base_url()}/explore/?slice_id={updated_chart.id}"
                    ),
                    "uuid": str(updated_chart.uuid) if updated_chart.uuid else None,
                }
            )
            chart_data["updated"] = True
            chart_data["form_data_key"] = form_data_key
            chart_data["is_unsaved_state"] = False
        else:
            chart_data = {
                "id": chart_id,
                "slice_name": chart_name,
                "viz_type": viz_type,
                "url": explore_url,
                "uuid": chart_uuid,
                "form_data": preview_or_error,
                "form_data_key": form_data_key,
                "is_unsaved_state": True,
            }

        result = {
            "chart": chart_data,
            "error": None,
            "warnings": warnings,
            # Include form_data so callers can verify what was saved.
            "form_data": (
                new_form_data
                if new_form_data is not None
                else chart_data.get("form_data") or {}
            ),
            "previews": previews,
            "capabilities": capabilities.model_dump() if capabilities else None,
            "semantics": semantics.model_dump() if semantics else None,
            "explore_url": explore_url,
            "api_endpoints": {
                "data": f"{get_superset_base_url()}/api/v1/chart/{chart_id}/data/",
                "export": f"{get_superset_base_url()}/api/v1/chart/{chart_id}/export/",
            },
            "form_data_key": form_data_key or chart_data.get("form_data_key"),
            "performance": None,
            "accessibility": accessibility.model_dump() if accessibility else None,
            "success": True,
            "schema_version": "2.0",
            "api_version": "v1",
        }
        stage_durations_ms["response_assembly"] = int(
            (time.perf_counter() - response_assembly_start) * 1000
        )
        performance = build_performance_metadata(
            total_start_time=total_start_time,
            cache_status="miss",
            optimization_suggestions=[],
            stage_durations_ms=stage_durations_ms,
        )
        result["performance"] = performance.model_dump()
        return GenerateChartResponse.model_validate(result)

    except OAuth2RedirectError as ex:
        await ctx.warning(
            "Chart update requires OAuth authentication: identifier=%s"
            % request.identifier
        )
        return GenerateChartResponse.model_validate(
            {
                "chart": None,
                "success": False,
                "error": {
                    "error_type": "OAUTH2_REDIRECT",
                    "message": build_oauth2_redirect_message(ex),
                    "details": "OAuth2 authentication required",
                },
            }
        )
    except OAuth2Error:
        await ctx.error("OAuth2 configuration error: chart_id=%s" % request.identifier)
        return GenerateChartResponse.model_validate(
            {
                "chart": None,
                "success": False,
                "error": {
                    "error_type": "OAUTH2_REDIRECT_ERROR",
                    "message": OAUTH2_CONFIG_ERROR_MESSAGE,
                    "details": "OAuth2 configuration or provider error",
                },
            }
        )
    except (
        CommandException,
        SQLAlchemyError,
        ValueError,
        KeyError,
        AttributeError,
    ) as e:
        from superset import db

        try:
            db.session.rollback()  # pylint: disable=consider-using-transaction
        except SQLAlchemyError:
            logger.warning(
                "Database rollback failed during error handling", exc_info=True
            )
        return GenerateChartResponse.model_validate(
            {
                "chart": None,
                "error": {
                    "error_type": type(e).__name__,
                    "message": f"Chart update failed: {e}",
                    "details": str(e),
                },
                "performance": build_performance_metadata(
                    total_start_time=total_start_time,
                    cache_status="error",
                    stage_durations_ms=stage_durations_ms,
                ).model_dump(),
                "success": False,
                "schema_version": "2.0",
                "api_version": "v1",
            }
        )
