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

"""Tests for the instance://metadata MCP resource."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from superset.mcp_service.system.resources.instance_metadata import (
    get_instance_metadata_resource,
)
from superset.mcp_service.system.schemas import (
    AvailableDatasetSummary,
    DashboardBreakdown,
    DatabaseBreakdown,
    DatabaseSummary,
    FeatureAvailability,
    InstanceInfo,
    InstanceMetadata,
    InstanceSummary,
    PopularContent,
    RecentActivity,
)
from superset.utils import json


def _make_instance_info() -> InstanceInfo:
    return InstanceInfo(
        instance_summary=InstanceSummary(
            total_dashboards=1,
            total_charts=2,
            total_datasets=3,
            total_databases=4,
            total_users=5,
            total_roles=2,
            total_tags=0,
            avg_charts_per_dashboard=2.0,
        ),
        recent_activity=RecentActivity(
            dashboards_created_last_30_days=0,
            charts_created_last_30_days=0,
            datasets_created_last_30_days=0,
            dashboards_modified_last_7_days=0,
            charts_modified_last_7_days=0,
            datasets_modified_last_7_days=0,
        ),
        dashboard_breakdown=DashboardBreakdown(
            published=1,
            unpublished=0,
            certified=0,
            with_charts=1,
            without_charts=0,
        ),
        database_breakdown=DatabaseBreakdown(by_type={"postgresql": 1}),
        popular_content=PopularContent(),
        feature_availability=FeatureAvailability(accessible_menus=["SQL Lab"]),
        timestamp=datetime.now(timezone.utc),
    )


def test_instance_metadata_resource_returns_valid_schema() -> None:
    database = MagicMock()
    database.id = 7
    database.database_name = "warehouse"
    database.backend = "postgresql"

    with (
        patch(
            "superset.mcp_service.system.resources.instance_metadata.build_instance_info",
            return_value=_make_instance_info(),
        ),
        patch(
            "superset.mcp_service.system.resources.instance_metadata.list_accessible_databases",
            return_value=[database],
        ),
        patch(
            "superset.mcp_service.system.resources.instance_metadata.list_accessible_dataset_summaries",
            return_value=[
                AvailableDatasetSummary(
                    id=9,
                    table_name="sample_events_v2",
                    schema="example_schema",
                    database_id=7,
                )
            ],
        ),
    ):
        payload = json.loads(get_instance_metadata_resource())

    metadata = InstanceMetadata(**payload)
    assert metadata.feature_availability.accessible_menus == ["SQL Lab"]
    assert metadata.available_databases == [
        DatabaseSummary(id=7, database_name="warehouse", backend="postgresql")
    ]
    assert metadata.available_datasets[0].table_name == "sample_events_v2"
