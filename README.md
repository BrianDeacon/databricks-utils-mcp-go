# Databricks Utils MCP Server (Go)

An MCP (Model Context Protocol) server for Databricks development and operations, written in Go. Compatible with any MCP client: Claude Code, Claude Desktop, Cursor, and others.

This is a Go implementation of [databricks-utils-mcp](https://github.com/BrianDeacon/databricks-utils-mcp) with identical tool signatures and behavior.

Covers ten areas:

- **Unity Catalog** -- browse catalogs, schemas, tables, volumes, and functions; describe table schemas; sample table data
- **SQL** -- execute SQL statements; manage SQL warehouses (list, start, stop)
- **Query History** -- list recent queries; get full query details
- **Jobs** -- list, inspect, trigger, cancel, and repair job runs
- **Clusters** -- list (with filtering and paging), inspect, start, and terminate clusters
- **Pipelines** -- list, inspect, start, and stop Delta Live Tables pipelines
- **Workspace** -- browse workspace directories; export notebooks
- **Files** -- list and read files from DBFS and Unity Catalog Volumes
- **Secrets** -- list secret scopes and key names (values are never returned)
- **Permissions** -- get Unity Catalog grants and workspace object ACLs

Authentication uses the Databricks SDK's standard credential chain, which auto-discovers credentials from `~/.databrickscfg` profiles, environment variables, or Azure CLI. Raw tokens are never passed as tool parameters. See [Authentication](#authentication) below.

## Requirements

- [Go](https://go.dev/) 1.22 or later
- A Databricks workspace with a personal access token, OAuth config, or Azure CLI session

## Installation

### From source

```bash
go install github.com/BrianDeacon/databricks-utils-mcp-go/cmd/databricks-utils-mcp@latest
```

### Building locally

```bash
git clone https://github.com/BrianDeacon/databricks-utils-mcp-go
cd databricks-utils-mcp-go
go build -o databricks-utils-mcp ./cmd/databricks-utils-mcp
```

## Configuration

**Claude Code** users:

```bash
claude mcp add --scope user databricks-utils -- databricks-utils-mcp
```

For other MCP clients, add the following to your server configuration:

```json
{
  "mcpServers": {
    "databricks-utils": {
      "command": "databricks-utils-mcp"
    }
  }
}
```

If the binary is not in your `$PATH`, use the full path:

```json
{
  "mcpServers": {
    "databricks-utils": {
      "command": "/path/to/databricks-utils-mcp"
    }
  }
}
```

Restart your MCP client after adding the server.

---

## Authentication

All tools use the [Databricks SDK's unified authentication](https://docs.databricks.com/dev-tools/auth/unified-auth.html). By default, the SDK checks (in order): environment variables, `~/.databrickscfg` default profile, Azure CLI, and other standard credential sources.

Every tool accepts three optional parameters to override the default:

| Parameter | Description |
|-----------|-------------|
| `profile` | Name of a `~/.databrickscfg` profile (provides both host and credentials) |
| `host` | Workspace URL override (credentials still resolved via the standard chain) |
| `token_env_var` | Name of an environment variable holding a PAT. The tool reads the value locally; the token itself is never sent as a parameter. |

### Example `~/.databrickscfg`

```ini
[DEFAULT]
host  = https://adb-1234567890.0.azuredatabricks.net/
token = dapi...

[staging]
host      = https://adb-0987654321.0.azuredatabricks.net/
auth_type = databricks-cli
```

With this config, tools use the `DEFAULT` profile automatically. Pass `profile="staging"` to target the staging workspace instead.

---

## Unity Catalog Tools

### `catalog_list_catalogs`

List all Unity Catalog catalogs accessible to the current user. Returns a sorted JSON array of catalog names.

### `catalog_list_schemas`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `catalog` | string | yes | Catalog name |

### `catalog_list_tables`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `catalog` | string | yes | Catalog name |
| `schema` | string | yes | Schema name |

### `catalog_describe_table`

Returns columns (name, type, comment), table type, storage location, properties, and timestamps.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `full_name` | string | yes | Three-part name: `catalog.schema.table` |

### `catalog_get_table_sample`

Executes `SELECT * LIMIT` against a table using a SQL warehouse. Returns a JSON array of row objects.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `full_name` | string | yes | Three-part name: `catalog.schema.table` |
| `limit` | integer | no | Number of rows (default 10, max 100) |
| `warehouse_id` | string | no | SQL warehouse ID. If omitted, uses the first running warehouse. |

### `catalog_list_volumes`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `catalog` | string | yes | Catalog name |
| `schema` | string | yes | Schema name |

### `catalog_list_volume_files`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `volume_path` | string | yes | Path under `/Volumes/catalog/schema/volume/` |

### `catalog_list_functions`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `catalog` | string | yes | Catalog name |
| `schema` | string | yes | Schema name |

---

## SQL Tools

### `sql_execute`

Execute a SQL statement against a Databricks SQL warehouse. Returns a JSON array of row objects for queries, or a status message for DDL/DML.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `statement` | string | yes | SQL statement to execute |
| `warehouse_id` | string | no | SQL warehouse ID. If omitted, uses the first running warehouse. |
| `max_rows` | integer | no | Maximum rows to return (default 100, cap 10,000) |
| `catalog` | string | no | Default catalog for unqualified names |
| `schema` | string | no | Default schema for unqualified names |

### `sql_list_warehouses`

List all SQL warehouses. Returns a JSON array with id, name, state, cluster_size, and auto_stop_mins.

### `sql_get_warehouse`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `warehouse_id` | string | yes | SQL warehouse ID |

### `sql_start_warehouse`

Start a stopped SQL warehouse. Does not wait for it to finish starting.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `warehouse_id` | string | yes | SQL warehouse ID |

### `sql_stop_warehouse`

Stop a running SQL warehouse.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `warehouse_id` | string | yes | SQL warehouse ID |

---

## Query History Tools

### `query_history_list`

List recent SQL queries. Returns query ID, statement (truncated), status, duration, rows produced, user, and warehouse.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `warehouse_id` | string | no | Filter to a specific warehouse |
| `user_name` | string | no | Filter by user |
| `status` | string | no | Filter by status: `FINISHED`, `FAILED`, `CANCELED`, `RUNNING` |
| `max_results` | integer | no | Max results (default 25, cap 100) |

### `query_history_get`

Get full details for a specific query including the complete statement text, metrics, and error message if failed.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `query_id` | string | yes | Query ID |

---

## Jobs Tools

### `jobs_list`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | no | Filter by job name (substring match) |
| `max_results` | integer | no | Max results (default 25) |

### `jobs_get`

Returns tasks, schedule, clusters, parameters, tags, and notifications.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `job_id` | integer | yes | Job ID |

### `jobs_list_runs`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `job_id` | integer | no | Filter to a specific job |
| `active_only` | boolean | no | Only show active (in-progress) runs (default false) |
| `max_results` | integer | no | Max results (default 25) |

### `jobs_get_run`

Returns per-task states, start/end times, cluster info, error messages, and attempt number.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `run_id` | integer | yes | Run ID |

### `jobs_get_run_output`

Returns notebook output, error trace, and logs depending on task type.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `run_id` | integer | yes | Run ID |

### `jobs_run`

Trigger a job run. Does not wait for completion. Returns the run_id for tracking.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `job_id` | integer | yes | Job ID |
| `parameters` | object | no | Parameter overrides as a key/value map |

### `jobs_cancel_run`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `run_id` | integer | yes | Run ID |

### `jobs_repair_run`

Re-run failed tasks in a multi-task job run.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `run_id` | integer | yes | Run ID of a failed multi-task job |
| `rerun_tasks` | array | no | Specific task keys to re-run. If omitted, re-runs all failed tasks. |

---

## Cluster Tools

### `clusters_list`

Results are paged (default 20 per page). The response includes `next_page_token` and `prev_page_token` when more pages are available.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `cluster_sources` | string | no | Comma-separated filter: `UI`, `API`, `JOB` |
| `cluster_states` | string | no | Comma-separated filter: `RUNNING`, `TERMINATED`, `PENDING`, etc. |
| `is_pinned` | boolean | no | If true, only return pinned clusters |
| `page_size` | integer | no | Clusters per page (default 20) |
| `page_token` | string | no | Token from a previous response for next/previous page |

### `clusters_get`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `cluster_id` | string | yes | Cluster ID |

### `clusters_get_events`

Returns recent events including termination reasons.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `cluster_id` | string | yes | Cluster ID |
| `max_results` | integer | no | Max events (default 25) |

### `clusters_start`

Start a terminated cluster. Does not wait for it to finish starting.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `cluster_id` | string | yes | Cluster ID |

### `clusters_terminate`

Terminate a running cluster. This stops the cluster but does not delete it.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `cluster_id` | string | yes | Cluster ID |

---

## Pipeline Tools

### `pipelines_list`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | no | Filter by pipeline name (substring match) |
| `max_results` | integer | no | Max results (default 25) |

### `pipelines_get`

Returns target catalog/schema, clusters, libraries, and notifications.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `pipeline_id` | string | yes | Pipeline ID |

### `pipelines_list_events`

Returns events including update progress, errors, and data quality metrics.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `pipeline_id` | string | yes | Pipeline ID |
| `max_results` | integer | no | Max events (default 25) |

### `pipelines_start`

Start a pipeline update. Returns the update_id for tracking.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `pipeline_id` | string | yes | Pipeline ID |
| `full_refresh` | boolean | no | Full refresh instead of incremental (default false) |

### `pipelines_stop`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `pipeline_id` | string | yes | Pipeline ID |

---

## Workspace Tools

### `workspace_list`

List objects in a workspace directory. Returns path, object_type (`NOTEBOOK`, `DIRECTORY`, `FILE`, `REPO`), and language (for notebooks).

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `path` | string | yes | Workspace path (e.g. `/Users/user@example.com`) |

### `workspace_export_notebook`

For `SOURCE` format, returns the notebook content as text. For other formats, returns base64-encoded content.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `path` | string | yes | Notebook path in workspace |
| `format` | string | no | Export format: `SOURCE` (default), `HTML`, `JUPYTER`, `DBC` |

---

## Files Tools

### `files_list`

List files in a DBFS or Volumes path.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `path` | string | yes | `dbfs:/...` for DBFS, `/Volumes/...` for Unity Catalog Volumes |

### `files_read`

Read a file from Volumes or DBFS. Returns text content for text files, or a size summary for binary files.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `path` | string | yes | Path to the file |
| `max_bytes` | integer | no | Max bytes to read (default 1 MB, cap 10 MB) |

---

## Secrets Tools

### `secrets_list_scopes`

List all secret scopes in the workspace. Returns a sorted JSON array of scope names.

### `secrets_list_keys`

List secret key names in a scope. Values are never returned.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `scope` | string | yes | Secret scope name |

---

## Permissions Tools

### `permissions_get_grants`

Get Unity Catalog grants on a securable object. Returns a JSON array of grant entries (principal, privileges).

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `securable_type` | string | yes | One of: `CATALOG`, `SCHEMA`, `TABLE`, `VOLUME`, `FUNCTION` |
| `full_name` | string | yes | Full name of the securable |

### `permissions_get_object_permissions`

Get access control list for a workspace object. Returns a JSON array of ACL entries with principal and permission levels.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `object_type` | string | yes | One of: `clusters`, `jobs`, `pipelines`, `sql/warehouses`, etc. |
| `object_id` | string | yes | Object ID |

---

## Development

```bash
go build ./cmd/databricks-utils-mcp
go test ./...
go vet ./...
```

## License

MIT
