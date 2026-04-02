# Superset MCP Implementation Plan

## Goal
Expand Superset MCP as a typed API in incremental batches, starting with shared chart schema improvements that unlock richer filtering and additional production-ready chart types without breaking existing MCP clients.

## Decisions
- The implementation will land in batches and this file is the continuity artifact between batches.
- Batch 1 focuses on the chart schema foundation: typed filters, a shared chart capability registry, and the first missing chart types for create/update flows.
- Backward compatibility is preserved by continuing to accept the existing simple `FilterConfig` shape and normalizing it internally into the richer filter model where possible.
- The roadmap remains typed-first. No raw `form_data` escape hatch is introduced in the initial phases.
- The roadmap was reopened on 2026-04-02 after live MCP validation exposed runtime and discoverability gaps that were not covered by the mocked command-boundary tests.

## Current MCP Surface
- Chart create/update/explore-link/preview-update all share `ChartConfig` and can evolve together.
- Chart authoring supports `xy`, `table`, `pie`, `pivot_table`, `mixed_timeseries`, `handlebars`, `funnel`, `big_number`, `gauge`, `heatmap`, `treemap`, `sunburst`, `sankey`, `word_cloud`, `world_map`, `box_plot`, and `bubble`.
- `get_chart_data` applies `extra_form_data` filters consistently across saved chart, fallback form-data, cached unsaved-state, and unsaved `form_data_key` execution paths.
- Dashboard operations support generation, listing, inspection, adding charts, metadata updates, typed row-based layout rebuilds, chart resizing, targeted chart move/reorder actions, chart removal, and typed native-filter upserts.
- Dataset operations support listing, inspection, virtual dataset creation with structured MCP error responses, and typed metric/calculated-column mutations.
- Dataset operations also support typed metadata updates for virtual-dataset rename/SQL replacement plus owner and custom-tag updates.
- SQL workflows support query execution, saved-query listing/inspection, saving a query, promoting a saved query into a virtual dataset, and opening SQL Lab with context.
- SQL workflows also support promoting a saved query directly into chart and explore-link workflows, with the same structured dataset-create error handling as `create_virtual_dataset`.

## Planned Phases
### Phase 1
- Add typed chart filter variants for value, range, null, and time filters.
- Add a small shared registry for supported typed chart creation/update capabilities.
- Add typed chart configs and form-data mapping for `funnel`, `big_number`, `gauge`, and `heatmap`.
- Update validation, examples, and help text to reflect the expanded chart surface.

### Phase 2
- Add second-wave chart configs: `treemap`, `sunburst`, `sankey`, `word_cloud`, `world_map`, `box_plot`, and `bubble`.
- Add richer dashboard-native filter authoring and typed layout operations.
- Expand filter semantics to cover metric/HAVING-style filters where supported.

### Phase 3
- Add dataset authoring and virtual dataset lifecycle operations.
- Add saved-query discovery plus SQL-to-dataset and dataset-to-chart typed workflows.
- Close remaining dashboard/chart lifecycle gaps and re-evaluate whether a limited advanced escape hatch is justified.

## Batch Status
### 2026-03-31 Batch 1
- Scope:
  - add this implementation tracker
  - add typed filter variants with compatibility normalization
  - add the shared chart capability registry
  - add `funnel`, `big_number`, `gauge`, and `heatmap` chart configs and mappers
  - update schema validation, chart examples, and help text
- Schemas/tools modified:
  - `superset/mcp_service/chart/schemas.py`
  - `superset/mcp_service/chart/chart_utils.py`
  - `superset/mcp_service/chart/validation/schema_validator.py`
  - `superset/mcp_service/chart/resources/chart_configs.py`
  - `superset/mcp_service/app.py`
- Compatibility notes:
  - existing `FilterConfig(column/op/value)` input remains accepted
  - no migrations are required
- Tests:
  - added unit coverage for new chart schemas, form-data mapping, and filter normalization
  - ran `python -m compileall` on the modified MCP chart modules and tests
  - ran `pre-commit run --files ...` on all modified files
  - could not run `pytest` because the active Python environment does not have `pytest` installed
- Remaining follow-up items after Batch 1:
  - dashboard update/remove/native-filter tools
  - dataset authoring tools
  - saved-query discovery and SQL promotion tools

### 2026-03-31 Batch 2
- Scope:
  - add typed dashboard mutation schemas
  - add `update_dashboard`
  - add `remove_chart_from_dashboard`
  - add `upsert_dashboard_native_filters`
  - add shared dashboard mutation helpers for layout and metadata cleanup
  - register the new tools in the MCP app and dashboard tool package
- Schemas/tools modified:
  - `superset/mcp_service/dashboard/schemas.py`
  - `superset/mcp_service/dashboard/utils.py`
  - `superset/mcp_service/dashboard/tool/update_dashboard.py`
  - `superset/mcp_service/dashboard/tool/remove_chart_from_dashboard.py`
  - `superset/mcp_service/dashboard/tool/upsert_dashboard_native_filters.py`
  - `superset/mcp_service/dashboard/tool/__init__.py`
  - `superset/mcp_service/app.py`
  - `superset/mcp_service/middleware.py`
- Compatibility notes:
  - existing dashboard read tools are unchanged
  - new dashboard mutation tools use typed request models and wrap existing Superset dashboard commands
- Tests:
  - added focused unit coverage for dashboard metadata updates, layout rebuilds, chart removal, and native-filter upserts
  - ran `python -m compileall` on the new dashboard modules and tests
  - ran `pre-commit run --files ...` and passed `mypy`, `ruff-format`, and `ruff`
  - could not complete the full `pre-commit` run because `pylint` is not installed in the active local Python environment
  - could not run `pytest` because the active Python environment does not have `pytest` installed
- Remaining follow-up items after Batch 2:
  - second-wave chart configs (`treemap`, `sunburst`, `sankey`, `word_cloud`, `world_map`, `box_plot`, `bubble`)
  - richer dashboard native-filter scope targeting and layout operations beyond the simple auto-grid rebuild path
  - dataset authoring tools
  - saved-query discovery and SQL promotion tools

### 2026-03-31 Batch 3
- Scope:
  - add second-wave typed chart configs for `treemap`, `sunburst`, `sankey`, `word_cloud`, `world_map`, `box_plot`, and `bubble`
  - map the new chart configs into Superset `form_data` through the shared chart utility layer
  - extend chart capability metadata, schema validation, chart examples, and MCP help text to reflect the new authoring surface
- Schemas/tools modified:
  - `superset/mcp_service/chart/registry.py`
  - `superset/mcp_service/chart/schemas.py`
  - `superset/mcp_service/chart/chart_utils.py`
  - `superset/mcp_service/chart/validation/schema_validator.py`
  - `superset/mcp_service/chart/resources/chart_configs.py`
  - `superset/mcp_service/app.py`
- Compatibility notes:
  - existing chart tool request shapes remain additive and backward compatible
  - the new typed chart configs intentionally cover stable, well-understood options from Superset example charts rather than every plugin-specific knob
- Tests:
  - added focused unit coverage for the second-wave chart schemas, name generation, and form-data mapping
  - ran `python -m compileall` on the modified MCP chart modules and tests
  - ran `pre-commit run --files ...` and passed `mypy`, `ruff-format`, and `ruff`
  - could not complete the full `pre-commit` run because `pylint` is not installed in the active local Python environment
  - could not run `pytest` because the active Python environment does not have `pytest` installed
- Remaining follow-up items after Batch 3:
  - richer dashboard native-filter scope targeting and layout operations beyond the simple auto-grid rebuild path
  - dataset authoring tools
  - saved-query discovery and SQL promotion tools
  - metric/HAVING-style filter semantics for compatible chart types

### 2026-03-31 Batch 4
- Scope:
  - extend `update_dashboard` with typed explicit row layout rebuilds
  - add typed chart dimension updates for dashboard layouts without exposing raw `position_json`
  - extend dashboard native filter typing with `root_path`, stricter scope validation, and typed default values
  - update MCP help text to reflect the richer dashboard mutation surface
- Schemas/tools modified:
  - `superset/mcp_service/dashboard/schemas.py`
  - `superset/mcp_service/dashboard/utils.py`
  - `superset/mcp_service/dashboard/tool/update_dashboard.py`
  - `superset/mcp_service/dashboard/tool/upsert_dashboard_native_filters.py`
  - `superset/mcp_service/app.py`
- Compatibility notes:
  - existing `chart_ids` auto-grid updates remain supported
  - new dashboard layout controls stay typed and additive rather than exposing raw layout JSON
  - native filter scope remains backward compatible while allowing narrower roots and typed defaults
- Tests:
  - added focused unit coverage for explicit row layouts, chart dimension updates, and scoped native filter defaults
  - ran `python -m compileall` on the modified dashboard modules and tests
  - ran `pre-commit run --files ...` and passed `mypy`, `ruff-format`, `ruff`, and `pylint`
  - could not run `pytest` because the active Python environment does not have `pytest` installed
- Remaining follow-up items after Batch 4:
  - dataset authoring tools
  - saved-query discovery and SQL promotion tools
  - metric/HAVING-style filter semantics for compatible chart types
  - deeper dashboard-native filter controls beyond the initial typed defaults and scope roots

### 2026-03-31 Batch 5
- Scope:
  - add `create_virtual_dataset` for typed SQL-to-dataset authoring
  - add `update_dataset_metrics` with typed upsert/remove semantics
  - add `update_dataset_calculated_columns` with typed calculated-column mutation semantics while preserving physical columns
  - add `list_saved_queries` and `get_saved_query` for saved-query discovery
  - add `create_virtual_dataset_from_saved_query` as the first SQL-to-dataset promotion bridge
  - update MCP instructions and middleware mappings to reflect the new dataset and saved-query tool surface
- Schemas/tools modified:
  - `superset/mcp_service/dataset/schemas.py`
  - `superset/mcp_service/dataset/utils.py`
  - `superset/mcp_service/dataset/tool/create_virtual_dataset.py`
  - `superset/mcp_service/dataset/tool/update_dataset_metrics.py`
  - `superset/mcp_service/dataset/tool/update_dataset_calculated_columns.py`
  - `superset/mcp_service/sql_lab/schemas.py`
  - `superset/mcp_service/sql_lab/tool/list_saved_queries.py`
  - `superset/mcp_service/sql_lab/tool/get_saved_query.py`
  - `superset/mcp_service/sql_lab/tool/create_virtual_dataset_from_saved_query.py`
  - `superset/mcp_service/app.py`
  - `superset/mcp_service/middleware.py`
- Compatibility notes:
  - existing dataset read tools remain unchanged
  - dataset metric and calculated-column mutations default to additive upsert behavior rather than destructive replacement
  - full replacement remains opt-in through `replace_metrics` and `replace_calculated_columns`
  - saved-query discovery continues to respect Superset's current-user base filter
- Tests:
  - added focused unit coverage for virtual dataset creation, typed metric/calculated-column mutation payload building, saved-query listing/lookup, and saved-query-to-dataset promotion
  - ran `python -m compileall` on the modified dataset/SQL MCP modules and tests
  - ran `pre-commit run --files ...` and passed `mypy`, `ruff-format`, `ruff`, and `pylint`
  - could not run `pytest` because the active Python environment does not have `pytest` installed
- Remaining follow-up items after Batch 5:
  - dataset-to-chart promotion helpers built on the new virtual-dataset tooling
  - metric/HAVING-style filter semantics for compatible chart types
  - deeper dashboard-native filter controls beyond the initial typed defaults and scope roots
  - broader dataset lifecycle coverage such as renaming virtual datasets, SQL replacement, and owner/tag metadata

### 2026-03-31 Batch 6
- Scope:
  - add `metric_filter` as a typed HAVING-style chart filter variant
  - map metric filters into Superset `adhoc_filters` with `HAVING` clauses while preserving typed operators
  - add `generate_chart_from_saved_query` as the first saved-query-to-chart bridge built on virtual dataset creation plus the existing typed chart generation flow
  - update MCP instructions and chart examples to document metric filters and the new bridge tool
- Schemas/tools modified:
  - `superset/mcp_service/chart/schemas.py`
  - `superset/mcp_service/chart/chart_utils.py`
  - `superset/mcp_service/chart/resources/chart_configs.py`
  - `superset/mcp_service/sql_lab/schemas.py`
  - `superset/mcp_service/sql_lab/tool/generate_chart_from_saved_query.py`
  - `superset/mcp_service/sql_lab/tool/__init__.py`
  - `superset/mcp_service/app.py`
- Compatibility notes:
  - existing row-level chart filters remain unchanged and backward compatible
  - metric filters are additive and opt-in via `filter_type="metric_filter"`
  - saved-query-to-chart promotion composes the existing virtual-dataset creation and chart generation flows instead of introducing a separate chart engine
- Tests:
  - added focused unit coverage for metric-filter schema parsing, HAVING-clause mapping, and saved-query-to-chart orchestration
  - ran `python -m compileall` on the modified chart/SQL MCP modules and tests
  - ran `pre-commit run --files ...` and passed `mypy`, `ruff-format`, `ruff`, and `pylint`
  - could not run `pytest` because the active Python environment does not have `pytest` installed
- Remaining follow-up items after Batch 6:
  - deeper dashboard-native filter controls beyond the initial typed defaults and scope roots
  - broader dataset lifecycle coverage such as renaming virtual datasets, SQL replacement, and owner/tag metadata
  - evaluate whether a generic explore-link bridge from saved queries is worth adding on top of the new chart bridge
  - assess whether dashboard-native filter targeting or broader dataset lifecycle work should be prioritized next

### 2026-04-01 Batch 7
- Scope:
  - add typed dataset metadata updates for virtual-dataset rename and SQL replacement
  - add typed owner updates and exact custom-tag replacement for datasets
  - keep the mutation surface scoped so physical datasets do not get retargeted via MCP
  - update MCP instructions and the implementation tracker to reflect the new dataset lifecycle coverage
- Schemas/tools modified:
  - `superset/mcp_service/dataset/schemas.py`
  - `superset/mcp_service/dataset/utils.py`
  - `superset/mcp_service/dataset/tool/update_dataset_metadata.py`
  - `superset/mcp_service/dataset/tool/__init__.py`
  - `superset/mcp_service/app.py`
- Compatibility notes:
  - existing dataset read and mutation tools remain unchanged
  - dataset metadata updates are additive and typed
  - virtual-dataset-only fields such as `sql` and dataset rename are rejected for physical datasets
  - custom tag updates replace only the custom tag set and leave implicit tags untouched
- Tests:
  - add focused unit coverage for dataset metadata request validation, virtual-dataset-only guardrails, payload building, and custom-tag synchronization
  - run `python -m compileall` on the modified dataset MCP modules and tests
  - run `pre-commit run --files ...` on the modified files
  - `pytest` may still be deferred if it is unavailable in the active Python environment
- Remaining follow-up items after Batch 7:
  - deeper dashboard-native filter controls beyond the initial typed defaults and scope roots
  - more granular dashboard layout move/resize/reorder actions
  - evaluate whether a generic explore-link bridge from saved queries is worth adding on top of the chart bridge

### 2026-04-01 Batch 8
- Scope:
  - add typed chart move/reorder actions for dashboard layouts without requiring full layout rebuilds
  - expand dashboard native filters with richer select-filter control values plus typed prefilter metadata
  - keep the new dashboard mutations additive and aligned with Superset's stored native-filter metadata shape
  - update MCP instructions and the implementation tracker to reflect the expanded dashboard completion surface
- Schemas/tools modified:
  - `superset/mcp_service/chart/chart_utils.py`
  - `superset/mcp_service/dashboard/schemas.py`
  - `superset/mcp_service/dashboard/utils.py`
  - `superset/mcp_service/dashboard/tool/update_dashboard.py`
  - `superset/mcp_service/dashboard/tool/upsert_dashboard_native_filters.py`
  - `superset/mcp_service/app.py`
- Compatibility notes:
  - existing dashboard layout rebuild and chart dimension flows remain unchanged
  - `chart_moves` is additive and intentionally separate from full `chart_ids`/`layout_rows` rebuilds
  - richer native-filter options map to existing Superset metadata fields such as `creatable`, `sortMetric`, `time_range`, `granularity_sqla`, and `adhoc_filters`
  - native-filter prefilters continue to reject metric/HAVING filters to avoid unsupported dashboard filter metadata
- Tests:
  - add focused unit coverage for chart move actions, richer native-filter serialization, and new schema guardrails
  - run `python -m compileall` on the modified dashboard/chart MCP modules and tests
  - run `pre-commit run --files ...` on the modified files
  - `pytest` may still be deferred if it is unavailable in the active Python environment
- Remaining follow-up items after Batch 8:
  - deeper dashboard-native filter controls beyond the newly added select-filter options and prefilters
  - more advanced dashboard layout operations if row/container-level mutations are still needed
  - evaluate whether a saved-query-to-explore-link helper is still justified after the chart bridge

### 2026-04-01 Batch 9
- Scope:
  - add `generate_explore_link_from_saved_query` as the saved-query-to-explore typed bridge
  - preserve the existing typed explore-link response shape while returning the intermediate dataset metadata
  - bring the served MCP surface back in sync with the tracker by importing `update_dataset_metadata` in the app bootstrap
  - update MCP instructions and tracker language to reflect the closed saved-query explore-link gap
- Schemas/tools modified:
  - `superset/mcp_service/sql_lab/schemas.py`
  - `superset/mcp_service/sql_lab/tool/generate_explore_link_from_saved_query.py`
  - `superset/mcp_service/sql_lab/tool/__init__.py`
  - `superset/mcp_service/app.py`
  - `superset/mcp_service/mcp_config.py`
- Compatibility notes:
  - existing saved-query-to-dataset and saved-query-to-chart flows remain unchanged
  - the new explore-link bridge composes the existing virtual-dataset creation and explore-link flows instead of introducing a separate visualization path
  - `update_dataset_metadata` was already implemented in Batch 7; this batch fixes the app import so the served tool surface matches the documented surface
- Tests:
  - add focused unit coverage for saved-query-to-explore-link orchestration and request validation
  - run `python -m compileall` on the modified SQL/app modules and tests
  - run `pre-commit run --files ...` on the modified files
  - `pytest` may still be deferred if it is unavailable in the active Python environment
- Remaining follow-up items after Batch 9:
  - row/container-level dashboard mutations only if a concrete workflow still needs them
  - otherwise close out the roadmap and explicitly defer the remaining dashboard-native filter/layout edge cases

### 2026-04-01 Batch 10
- Scope:
  - fix MCP auth-wrapper context detection so tools using postponed `ctx: Context` annotations register correctly
  - add repo-native compose support for a local MCP stack with PostgreSQL, Redis, web, and MCP services
  - add make targets for the minimal MCP-enabled stack and document the startup flow
  - isolate the local MCP stack in its own compose project so it does not reuse stale metadata DB volumes from other local runs
  - close the roadmap as operationally complete, deferring only dashboard edge cases that still lack a concrete user workflow
- Schemas/tools modified:
  - `superset/mcp_service/auth.py`
  - `docker-compose.yml`
  - `docker-compose-non-dev.yml`
  - `docker/.env`
  - `docker/README.md`
  - `Makefile`
  - `scripts/docker-compose-up.sh`
  - `tests/unit_tests/mcp_service/test_auth_user_resolution.py`
  - `tests/unit_tests/mcp_service/test_mcp_tool_registration.py`
- Compatibility notes:
  - existing MCP request/response shapes are unchanged
  - the new compose-backed MCP stack is additive and opt-in via the new `make up-mcp*` targets or the `mcp` compose profile
  - the MCP-focused make targets use a dedicated compose project name to avoid colliding with the default local dev stack
  - dashboard mutation tools with postponed annotations now register through the same auth wrapper path as the rest of the MCP surface
- Tests:
  - added focused unit coverage for string-based `ctx` annotations in `mcp_auth_hook`
  - tightened MCP registration coverage to assert the dashboard mutation tools are present in the served tool set
  - ran `pre-commit run --files ...` on the modified files and passed `mypy`, `ruff-format`, `ruff`, and `pylint`
  - attempted host-side `pytest`, but the active Python environment did not have `pytest` installed
  - ran `make up-mcp-detached` successfully after isolating the compose project and verified:
    - `curl http://127.0.0.1:8088/health` returned `OK`
    - `curl http://127.0.0.1:5009/mcp` returned `405 Method Not Allowed`, which is expected for a GET on the MCP endpoint
    - `docker exec superset-mcp-superset-mcp-1 superset mcp --help` succeeded
    - `remove_chart_from_dashboard`, `update_dashboard`, and `upsert_dashboard_native_filters` all registered in both the web and MCP container logs

### 2026-04-02 Batch 11
- Scope:
  - reopen the roadmap around runtime parity after validating the live MCP surface against the current tracker
  - compare the documented tool surface with actual local MCP behavior and logs
  - identify which reported gaps are outdated statements versus real remaining runtime issues
  - re-sequence the next implementation work around discovery, error-surface hardening, and end-to-end filter validation
- Findings recorded in the tracker:
  - `upsert_dashboard_native_filters` and `update_dashboard` are exposed in the live MCP surface, so the older statement that there is no clean native-filter API is no longer accurate
  - `create_virtual_dataset` exists and maps into `CreateDatasetCommand` correctly, but it does not yet translate known command failures into structured MCP errors
  - `instance://metadata` is broken locally because the resource path no longer satisfies the `InstanceInfo` schema and omits `feature_availability`
  - workflows that require `database_id` are not fully MCP-usable because database discovery is not dependable enough today
  - `get_chart_data` supports `extra_form_data.filters` in code, but organization-filter behavior is not covered by MCP integration tests and was not proven end-to-end in the local stack
  - dashboard mutation tools catch expected dashboard command errors, but at least one live failure path is still escaping to generic middleware `err_*` handling
- Validation performed:
  - inspected the live MCP tool surface through the local MCP stack and confirmed the presence of `create_virtual_dataset`, `update_dashboard`, `upsert_dashboard_native_filters`, and `get_chart_data`
  - inspected the local compose-backed MCP logs and confirmed the `instance://metadata` failure is caused by a missing `feature_availability` field in the resource response
  - compared the tool implementations with their underlying Superset command classes to verify that `create_virtual_dataset` payload mapping is correct and to identify missing error handling at the MCP tool layer
  - could not complete an end-to-end chart/dashboard filter workflow non-destructively because the local stack did not contain datasets, charts, or dashboards at the time of inspection
- Remaining follow-up items after Batch 11:
  - discovery and system metadata parity for database-driven workflows
  - structured error handling for dataset creation failures
  - live validation and hardening of dashboard mutation and native-filter workflows
  - end-to-end proof that chart data filtering works for organization-scoped workflows

### 2026-04-02 Batch 12
- Scope:
  - repair `instance://metadata` by reusing the shared `get_instance_info` metrics path instead of rebuilding a partial instance payload
  - add dedicated `list_databases` and `get_database_info` tools for MCP-first database discovery
  - align SQL workflow guidance and `open_sql_lab_with_context` database access checks with the same permission model used by `execute_sql`
  - update the tracker to treat discovery parity as complete and move the remaining work into runtime hardening plus end-to-end filter validation
- Schemas/tools modified:
  - `superset/mcp_service/system/schemas.py`
  - `superset/mcp_service/system/discovery_utils.py`
  - `superset/mcp_service/system/tool/get_instance_info.py`
  - `superset/mcp_service/system/tool/list_databases.py`
  - `superset/mcp_service/system/tool/get_database_info.py`
  - `superset/mcp_service/system/resources/instance_metadata.py`
  - `superset/mcp_service/system/tool/__init__.py`
  - `superset/mcp_service/system/prompts/quickstart.py`
  - `superset/mcp_service/sql_lab/tool/open_sql_lab_with_context.py`
  - `superset/mcp_service/app.py`
- Compatibility notes:
  - `instance://metadata` remains additive and still returns the instance summary fields from `InstanceInfo`, but now includes `feature_availability`, `current_user`, `available_databases`, and `available_datasets` reliably
  - the new database discovery tools are additive and intentionally minimal so they match the existing integer `database_id` inputs used by `execute_sql`, `save_sql_query`, and `create_virtual_dataset`
  - database discovery and SQL Lab link generation now use the same database access rule as `execute_sql`
- Tests:
  - added focused unit coverage for database discovery helpers, the repaired `instance://metadata` resource, database discovery tool registration/serialization, and SQL Lab database access checks
  - ran `python -m pytest tests/unit_tests/mcp_service/system/test_discovery_utils.py` and it passed
  - host-side `pytest` coverage for the MCP app/resource tests is still blocked in the local `.venv` because the installed `apache-superset-core` package predates `ToolAnnotations` and cannot import the full MCP app
  - ran `python -m compileall` on the modified MCP system/SQL files and tests
  - ran `pre-commit run --files ...` on the modified files and passed `mypy`, `ruff-format`, `ruff`, and `pylint`
  - ran the local MCP stack and manually validated inside the web container, as the admin user, that:
    - `instance://metadata` returns `feature_availability`, `available_databases`, and `available_datasets` without the previous schema failure
    - `list_databases` returns an accessible ClickHouse database
    - `get_database_info` returns the expected database detail payload
    - the discovered database ID can be used directly with `execute_sql` (`SELECT 1 AS ok`)
- Remaining follow-up items after Batch 12:
  - structured error handling for dataset creation failures
  - live validation and hardening of dashboard mutation and native-filter workflows
  - end-to-end proof that chart data filtering works for organization-scoped workflows

### 2026-04-02 Batch 13
- Scope:
  - add a shared dataset-create error mapper and use it from every MCP flow that builds a virtual dataset through `CreateDatasetCommand`
  - add a shared dashboard mutation error mapper and make dashboard mutation tools return structured tool-level errors instead of leaking generic middleware failures
  - validate the failure surface live so duplicate datasets and missing dashboards return structured MCP payloads
- Schemas/tools modified:
  - `superset/mcp_service/dataset/utils.py`
  - `superset/mcp_service/dataset/tool/create_virtual_dataset.py`
  - `superset/mcp_service/sql_lab/schemas.py`
  - `superset/mcp_service/sql_lab/tool/create_virtual_dataset_from_saved_query.py`
  - `superset/mcp_service/sql_lab/tool/generate_chart_from_saved_query.py`
  - `superset/mcp_service/sql_lab/tool/generate_explore_link_from_saved_query.py`
  - `superset/mcp_service/dashboard/schemas.py`
  - `superset/mcp_service/dashboard/utils.py`
  - `superset/mcp_service/dashboard/tool/update_dashboard.py`
  - `superset/mcp_service/dashboard/tool/remove_chart_from_dashboard.py`
  - `superset/mcp_service/dashboard/tool/upsert_dashboard_native_filters.py`
- Compatibility notes:
  - dataset-create entrypoints remain additive, but they now return structured MCP error payloads instead of relying on middleware fallthrough for known failure modes
  - saved-query promotion tools inherit the same dataset-create error behavior as `create_virtual_dataset`
  - dashboard mutation success responses are unchanged, but `error` is now a structured `DashboardError` object instead of a plain string
- Tests:
  - added focused unit coverage for dataset-create exception mapping and saved-query bridge error propagation
  - added focused unit coverage for structured dashboard mutation errors
  - ran `./.venv/bin/python -m pytest tests/unit_tests/mcp_service/dataset/tool/test_dataset_mutation_tools.py tests/unit_tests/mcp_service/sql_lab/tool/test_saved_query_tools.py` and it passed (`40 passed`)
  - host-side `pytest` collection for dashboard tests is still blocked in the local `.venv` because the installed `apache-superset-core` package predates `ToolAnnotations` and cannot import the full MCP tool package
  - ran `python -m compileall` on the modified MCP dataset/dashboard/SQL files and tests
  - live validation through the running MCP stack confirmed that:
    - creating the same virtual dataset twice returns a structured `DatasetExists` payload
    - `update_dashboard`, `upsert_dashboard_native_filters`, and `remove_chart_from_dashboard` return structured `NotFound` errors for a missing dashboard instead of generic middleware `err_*`

### 2026-04-02 Batch 14
- Scope:
  - fix `get_chart_data` so `extra_form_data.filters` are applied consistently across all execution paths, including fallback form-data and unsaved `form_data_key` flows
  - add MCP-level tests that assert filter clauses land in the query payload instead of relying only on lower-level Superset filter-merging behavior
  - close the organization-filter gap through live validation against a real dataset with an `organization_id` column
- Schemas/tools modified:
  - `superset/mcp_service/chart/tool/get_chart_data.py`
  - `tests/unit_tests/mcp_service/chart/tool/test_get_chart_data_filters.py`
- Compatibility notes:
  - `GetChartDataRequest` remains unchanged; this batch closes correctness gaps without introducing a second filter dialect
  - `extra_form_data.filters` continues to accept the existing simple filter shape, but now affects chart data consistently across saved and unsaved paths
  - the previously deferred typed `get_chart_data` filter wrapper remains unnecessary for correctness and stays out of scope
- Tests:
  - added focused MCP-level tests for saved query-context, fallback form-data, cached unsaved-state, and unsaved `form_data_key` filter application
  - host-side `pytest` collection for the new chart test file is still blocked in the local `.venv` because the installed `apache-superset-core` package predates `ToolAnnotations`
  - ran `python -m compileall` on `superset/mcp_service/chart/tool/get_chart_data.py` and the new chart test file
  - live validation through the running MCP stack, using a real dataset with an `organization_id` column, confirmed that:
    - `create_virtual_dataset` succeeded for a representative live SQL query
    - `generate_chart` produced a saved table chart grouped by `organization_id`
    - unfiltered `get_chart_data` returned the expected `default_organization` row
    - filtered `get_chart_data` with `extra_form_data.filters=[{\"col\": \"organization_id\", \"op\": \"IN\", \"val\": [\"missing_organization\"]}]` returned a structured `NoData` response instead of the unfiltered row
    - `generate_dashboard`, `update_dashboard`, and `upsert_dashboard_native_filters` all succeeded in the same live workflow without generic middleware errors
    - a native dashboard filter targeting `organization_id` was created successfully for the saved chart

## Open Gaps
- No blocking runtime-hardening or filter-parity gaps remain from the reopened roadmap.
- Optional dashboard row/container-level mutations remain a future enhancement if a concrete layout workflow needs them.
- Host-side `pytest` collection for MCP tests that import the full tool package is still blocked in the local `.venv` because the installed `apache-superset-core` package predates `ToolAnnotations`; live stack validation and targeted unit tests were used to close the current roadmap.

## Known Constraints
- Superset chart plugins use different `form_data` conventions, so new typed chart configs must stay intentionally scoped and map only well-understood options.
- Existing MCP clients depend on the simple filter shape, so schema changes must remain additive.
- Some advanced filtering and dashboard-native filter behavior depends on Superset internals that should be wrapped in typed APIs rather than passed through raw.
- The live MCP server is exposed through the `search_tools` / `call_tool` proxy, so resource quality and tool discoverability matter as much as the raw tool implementations.
- The current MCP unit coverage heavily exercises request mapping and mocked command orchestration, but several high-value workflows are not yet validated end-to-end against live command execution.
- Organization-filter validation requires real datasets, charts, and dashboards in the local stack; an empty local metadata DB can hide runtime issues in otherwise well-typed tool paths.

## Next Recommended Batch
No required implementation batch remains for the current MCP roadmap.

If future work is needed, the next optional batch should be limited to dashboard ergonomics:
- add row/container-level dashboard layout mutations only if a concrete workflow needs them
- revisit a typed `get_chart_data` filter wrapper only if usability feedback shows that `extra_form_data` is too opaque despite the runtime parity fixes

## Validation Checklist
- Add or update unit tests for any new schema or mapper.
- Verify the existing `xy` and `table` request shapes still validate.
- Verify unsupported chart types fail with explicit capability errors.
- Add integration-style validation for workflows that cross MCP tool boundaries rather than stopping at mocked command payloads.
- Verify `create_virtual_dataset` succeeds on valid input and returns structured errors for invalid SQL, duplicate datasets, missing database IDs, and permission denial.
- Verify `update_dashboard`, `upsert_dashboard_native_filters`, and `remove_chart_from_dashboard` do not leak generic middleware `err_*` for expected dashboard mutation failures.
- Verify `get_chart_data` applies organization filters through `extra_form_data` on a real saved chart.
- Record which tests were run, skipped, or deferred in the batch entry above.
