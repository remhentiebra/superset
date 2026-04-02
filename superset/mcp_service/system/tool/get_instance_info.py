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
Get instance high-level information FastMCP tool using configurable
InstanceInfoCore for flexible, extensible metrics calculation.
"""

import logging

from fastmcp import Context
from superset_core.mcp.decorators import tool, ToolAnnotations

from superset.extensions import event_logger
from superset.mcp_service.mcp_core import InstanceInfoCore
from superset.mcp_service.system.schemas import (
    GetSupersetInstanceInfoRequest,
    InstanceInfo,
    serialize_user_object,
)
from superset.mcp_service.system.system_utils import (
    calculate_dashboard_breakdown,
    calculate_database_breakdown,
    calculate_feature_availability,
    calculate_instance_summary,
    calculate_popular_content,
    calculate_recent_activity,
)

logger = logging.getLogger(__name__)


# Configure the instance info core
_instance_info_core = InstanceInfoCore(
    dao_classes={
        "dashboards": None,  # type: ignore[dict-item]  # Will be set at runtime
        "charts": None,  # type: ignore[dict-item]
        "datasets": None,  # type: ignore[dict-item]
        "databases": None,  # type: ignore[dict-item]
        "users": None,  # type: ignore[dict-item]
        "tags": None,  # type: ignore[dict-item]
    },
    output_schema=InstanceInfo,
    metric_calculators={
        "instance_summary": calculate_instance_summary,
        "recent_activity": calculate_recent_activity,
        "dashboard_breakdown": calculate_dashboard_breakdown,
        "database_breakdown": calculate_database_breakdown,
        "popular_content": calculate_popular_content,
        "feature_availability": calculate_feature_availability,
    },
    time_windows={
        "recent": 7,
        "monthly": 30,
        "quarterly": 90,
    },
    logger=logger,
)


_DEFAULT_INSTANCE_INFO_REQUEST = GetSupersetInstanceInfoRequest()


def _configure_instance_info_core() -> InstanceInfoCore:
    """Configure the shared InstanceInfoCore with runtime DAO classes."""
    from superset.daos.chart import ChartDAO
    from superset.daos.dashboard import DashboardDAO
    from superset.daos.database import DatabaseDAO
    from superset.daos.dataset import DatasetDAO
    from superset.daos.tag import TagDAO
    from superset.daos.user import UserDAO

    _instance_info_core.dao_classes = {
        "dashboards": DashboardDAO,
        "charts": ChartDAO,
        "datasets": DatasetDAO,
        "databases": DatabaseDAO,
        "users": UserDAO,
        "tags": TagDAO,
    }
    return _instance_info_core


def build_instance_info(action: str = "mcp.get_instance_info.metrics") -> InstanceInfo:
    """Build shared instance metadata for MCP tools and resources."""
    from flask import g

    instance_info_core = _configure_instance_info_core()

    with event_logger.log_context(action=action):
        result = instance_info_core.run_tool()

    if (user := getattr(g, "user", None)) is not None:
        result.current_user = serialize_user_object(user)

    return result


@tool(
    tags=["core"],
    annotations=ToolAnnotations(
        title="Get instance info",
        readOnlyHint=True,
        destructiveHint=False,
    ),
)
def get_instance_info(
    request: GetSupersetInstanceInfoRequest = _DEFAULT_INSTANCE_INFO_REQUEST,
    ctx: Context = None,
) -> InstanceInfo:
    """Get instance statistics.

    Returns counts, activity metrics, and database types.
    """
    try:
        return build_instance_info()

    except Exception as e:
        error_msg = f"Unexpected error in instance info: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise
