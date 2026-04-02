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

"""Convenience MCP resource for instance metadata and discovery summaries."""

import logging

from sqlalchemy.exc import SQLAlchemyError

from superset.mcp_service.app import mcp
from superset.mcp_service.auth import mcp_auth_hook
from superset.mcp_service.system.discovery_utils import (
    list_accessible_databases,
    list_accessible_dataset_summaries,
    serialize_database_summary,
)
from superset.mcp_service.system.schemas import InstanceMetadata
from superset.mcp_service.system.tool.get_instance_info import build_instance_info

logger = logging.getLogger(__name__)


@mcp.resource("instance://metadata")
@mcp_auth_hook
def get_instance_metadata_resource() -> str:
    """Return a convenience instance summary plus accessible database/dataset IDs."""
    from superset.utils import json

    try:
        instance_info = build_instance_info(action="mcp.instance_metadata.metrics")
        instance_metadata = InstanceMetadata(
            **instance_info.model_dump(),
            available_databases=[
                serialize_database_summary(database)
                for database in list_accessible_databases()
            ],
            available_datasets=list_accessible_dataset_summaries(),
        )
        return json.dumps(
            instance_metadata.model_dump(mode="json", by_alias=True),
            indent=2,
        )
    except (SQLAlchemyError, AttributeError, KeyError, ValueError) as ex:
        logger.error("Error generating instance metadata: %s", ex)
        return json.dumps(
            {
                "error": "Unable to fetch complete metadata",
                "tips": [
                    "Use get_instance_info for the base instance summary",
                    "Use list_databases to discover accessible database IDs",
                    "Use list_datasets to explore accessible datasets",
                ],
            }
        )
