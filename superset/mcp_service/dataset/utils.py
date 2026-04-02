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

"""Helpers shared by MCP dataset mutation tools."""

from __future__ import annotations

import logging
from typing import Any

from marshmallow import ValidationError
from sqlalchemy.orm import joinedload

from superset.commands.dataset.exceptions import (
    DatabaseNotFoundValidationError,
    DatasetCreateFailedError,
    DatasetDataAccessIsNotAllowed,
    DatasetExistsValidationError,
    DatasetInvalidError,
    TableNotFoundValidationError,
)
from superset.commands.exceptions import TagForbiddenError
from superset.commands.tag.create import CreateCustomTagCommand
from superset.commands.tag.delete import DeleteTaggedObjectCommand
from superset.connectors.sqla.models import SqlaTable
from superset.daos.dataset import DatasetDAO
from superset.errors import ErrorLevel, SupersetError, SupersetErrorType
from superset.exceptions import SupersetErrorException
from superset.extensions import security_manager
from superset.mcp_service.dataset.schemas import (
    DatasetCalculatedColumnMutation,
    DatasetError,
    DatasetInfo,
    DatasetMetricMutation,
    serialize_dataset_object,
)
from superset.tags.models import ObjectType, TagType
from superset.utils import json

logger = logging.getLogger(__name__)

_VIRTUAL_ONLY_DATASET_UPDATE_FIELDS = frozenset(
    {
        "table_name",
        "sql",
        "template_params",
        "normalize_columns",
        "always_filter_main_dttm",
    }
)


def get_dataset_by_identifier(identifier: int | str) -> SqlaTable:
    """Resolve a dataset by numeric ID or UUID within DAO base filters."""
    dataset = DatasetDAO.find_by_id_or_uuid(str(identifier))
    if dataset is None:
        raise SupersetErrorException(
            SupersetError(
                message=f"Dataset {identifier!r} not found",
                error_type=SupersetErrorType.GENERIC_COMMAND_ERROR,
                level=ErrorLevel.ERROR,
            )
        )
    return dataset


def serialize_dataset(dataset: SqlaTable) -> DatasetInfo:
    """Serialize a dataset model into the MCP response schema."""
    result = serialize_dataset_object(dataset)
    if result is None:
        raise SupersetErrorException(
            SupersetError(
                message="Failed to serialize dataset response",
                error_type=SupersetErrorType.GENERIC_COMMAND_ERROR,
                level=ErrorLevel.ERROR,
            )
        )
    return result


def build_create_dataset_payload(data: dict[str, Any]) -> dict[str, Any]:
    """Translate typed MCP dataset input into CreateDatasetCommand payload."""
    payload = data.copy()
    payload["database"] = payload.pop("database_id")
    payload["schema"] = payload.pop("schema_name", None)
    if (template_params := payload.get("template_params")) is not None:
        payload["template_params"] = json.dumps(template_params)
    return payload


def _flatten_validation_message(messages: dict[str, Any]) -> str:
    parts: list[str] = []
    for field, value in messages.items():
        if isinstance(value, list):
            text = "; ".join(str(item) for item in value)
        else:
            text = str(value)
        parts.append(f"{field}: {text}")
    return "; ".join(parts)


def _map_dataset_invalid_error(exception: DatasetInvalidError) -> DatasetError:
    validation_errors = getattr(exception, "_exceptions", [])
    error_message = _flatten_validation_message(exception.normalized_messages())
    if not error_message:
        error_message = str(exception)

    for validation_error in validation_errors:
        if isinstance(validation_error, DatabaseNotFoundValidationError):
            return DatasetError.create(error_message, "DatabaseNotFound")
        if isinstance(validation_error, DatasetExistsValidationError):
            return DatasetError.create(error_message, "DatasetExists")
        if isinstance(validation_error, DatasetDataAccessIsNotAllowed):
            return DatasetError.create(error_message, "PermissionDenied")
        if isinstance(validation_error, TableNotFoundValidationError):
            return DatasetError.create(error_message, "TableNotFound")
        if (
            isinstance(validation_error, ValidationError)
            and validation_error.field_name == "sql"
            and any(
                "Invalid SQL" in str(message)
                for message in getattr(validation_error, "messages", [])
            )
        ):
            return DatasetError.create(error_message, "InvalidSql")

    return DatasetError.create(error_message, "ValidationError")


def map_create_dataset_exception(exception: Exception) -> DatasetError:
    """Convert dataset-create failures into stable MCP error payloads."""
    if isinstance(exception, DatasetInvalidError):
        return _map_dataset_invalid_error(exception)
    if isinstance(exception, DatasetCreateFailedError):
        return DatasetError.create(str(exception), "CreateFailed")
    return DatasetError.create(str(exception), "InternalError")


def run_create_dataset_command(
    payload: dict[str, Any],
    *,
    command_factory: Any,
    action_label: str,
) -> SqlaTable | DatasetError:
    """Execute dataset creation and map failures into ``DatasetError``."""
    try:
        return command_factory(payload).run()
    except Exception as ex:  # noqa: BLE001
        logger.exception("%s failed", action_label)
        return map_create_dataset_exception(ex)


def build_dataset_metadata_update_payload(data: dict[str, Any]) -> dict[str, Any]:
    """Translate typed dataset metadata input into UpdateDatasetCommand payload."""
    payload = data.copy()
    payload.pop("identifier", None)
    payload.pop("tag_names", None)

    if "template_params" in payload:
        template_params = payload["template_params"]
        payload["template_params"] = (
            json.dumps(template_params) if template_params is not None else None
        )
    return payload


def build_metric_update_payload(
    dataset: SqlaTable,
    metrics: list[DatasetMetricMutation],
    remove_metrics: list[str],
    replace_metrics: bool,
) -> list[dict[str, Any]]:
    """Build UpdateDatasetCommand metric payload with safe typed upserts."""
    existing_metrics = {metric.metric_name: metric for metric in dataset.metrics}
    merged_metrics: dict[str, dict[str, Any]] = {}

    if not replace_metrics:
        for metric in dataset.metrics:
            merged_metrics[metric.metric_name] = {
                "id": metric.id,
                "metric_name": metric.metric_name,
                "expression": metric.expression,
                "description": metric.description,
                "extra": metric.extra,
                "metric_type": metric.metric_type,
                "d3format": metric.d3format,
                "verbose_name": metric.verbose_name,
                "warning_text": metric.warning_text,
                "currency": metric.currency,
            }

    for metric_name in remove_metrics:
        merged_metrics.pop(metric_name, None)

    for metric in metrics:
        payload = metric.model_dump(exclude_none=True)
        existing_metric = existing_metrics.get(metric.metric_name)
        if existing_metric is not None:
            payload["id"] = existing_metric.id
        merged_metrics[metric.metric_name] = payload

    return list(merged_metrics.values())


def build_calculated_column_update_payload(
    dataset: SqlaTable,
    columns: list[DatasetCalculatedColumnMutation],
    remove_columns: list[str],
    replace_calculated_columns: bool,
) -> list[dict[str, Any]]:
    """Build UpdateDatasetCommand column payload while preserving physical columns."""
    physical_columns: list[dict[str, Any]] = []
    existing_calculated_columns: dict[str, dict[str, Any]] = {}

    for column in dataset.columns:
        payload = {
            "id": column.id,
            "column_name": column.column_name,
            "type": column.type,
            "advanced_data_type": column.advanced_data_type,
            "verbose_name": column.verbose_name,
            "description": column.description,
            "expression": column.expression,
            "extra": column.extra,
            "filterable": column.filterable,
            "groupby": column.groupby,
            "is_active": column.is_active,
            "is_dttm": column.is_dttm,
            "python_date_format": column.python_date_format,
            "datetime_format": column.datetime_format,
        }
        if column.expression:
            existing_calculated_columns[column.column_name] = payload
        else:
            physical_columns.append(payload)

    merged_calculated_columns = (
        {} if replace_calculated_columns else existing_calculated_columns.copy()
    )

    for column_name in remove_columns:
        merged_calculated_columns.pop(column_name, None)

    for column in columns:
        payload = column.model_dump(exclude_none=True)
        existing_column = existing_calculated_columns.get(column.column_name)
        if existing_column is not None:
            payload["id"] = existing_column["id"]
        merged_calculated_columns[column.column_name] = payload

    return physical_columns + list(merged_calculated_columns.values())


def ensure_dataset_supports_requested_updates(
    dataset: SqlaTable,
    requested_fields: set[str],
) -> None:
    """Reject virtual-dataset-only mutations for physical datasets."""
    if dataset.is_virtual:
        return

    invalid_fields = sorted(requested_fields & _VIRTUAL_ONLY_DATASET_UPDATE_FIELDS)
    if not invalid_fields:
        return

    raise SupersetErrorException(
        SupersetError(
            message=(
                "The following updates are only supported for virtual datasets: "
                + ", ".join(invalid_fields)
            ),
            error_type=SupersetErrorType.GENERIC_COMMAND_ERROR,
            level=ErrorLevel.ERROR,
        )
    )


def sync_dataset_custom_tags(dataset: SqlaTable, tag_names: list[str]) -> None:
    """Replace the dataset's custom tag set with the provided names."""
    if not (
        security_manager.can_access("can_write", "Tag")
        or security_manager.can_access("can_tag", "Dataset")
    ):
        raise TagForbiddenError("You do not have permission to manage tags on datasets")

    current_custom_tags = {
        tag.name
        for tag in getattr(dataset, "tags", [])
        if getattr(tag, "type", None) == TagType.custom
    }
    requested_tags = set(tag_names)

    tags_to_remove = sorted(current_custom_tags - requested_tags)

    for tag_name in tags_to_remove:
        DeleteTaggedObjectCommand(ObjectType.dataset, dataset.id, tag_name).run()

    if tags_to_add := sorted(requested_tags - current_custom_tags):
        CreateCustomTagCommand(ObjectType.dataset, dataset.id, tags_to_add).run()


def refetch_dataset_for_response(dataset_id: int) -> SqlaTable:
    """Reload a dataset with relationship fields needed by the MCP serializer."""
    dataset = DatasetDAO.find_by_id(
        dataset_id,
        query_options=[
            joinedload(SqlaTable.owners),
            joinedload(SqlaTable.tags),
        ],
    )
    if dataset is None:
        raise SupersetErrorException(
            SupersetError(
                message=f"Dataset {dataset_id!r} not found",
                error_type=SupersetErrorType.GENERIC_COMMAND_ERROR,
                level=ErrorLevel.ERROR,
            )
        )
    return dataset
