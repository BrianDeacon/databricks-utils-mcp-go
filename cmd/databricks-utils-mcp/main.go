package main

import (
	"context"
	"fmt"
	"os"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/BrianDeacon/databricks-utils-mcp-go/internal/tools/catalog"
	"github.com/BrianDeacon/databricks-utils-mcp-go/internal/tools/clusters"
	"github.com/BrianDeacon/databricks-utils-mcp-go/internal/tools/files"
	"github.com/BrianDeacon/databricks-utils-mcp-go/internal/tools/jobs"
	"github.com/BrianDeacon/databricks-utils-mcp-go/internal/tools/permissions"
	"github.com/BrianDeacon/databricks-utils-mcp-go/internal/tools/pipelines"
	"github.com/BrianDeacon/databricks-utils-mcp-go/internal/tools/queryhistory"
	"github.com/BrianDeacon/databricks-utils-mcp-go/internal/tools/secrets"
	sqlt "github.com/BrianDeacon/databricks-utils-mcp-go/internal/tools/sql"
	"github.com/BrianDeacon/databricks-utils-mcp-go/internal/tools/workspace"
)

func main() {
	s := server.NewMCPServer(
		"databricks-utils-mcp",
		"0.1.0",
		server.WithToolCapabilities(true),
	)

	registerCatalogTools(s)
	registerSQLTools(s)
	registerQueryHistoryTools(s)
	registerJobsTools(s)
	registerClustersTools(s)
	registerPipelinesTools(s)
	registerWorkspaceTools(s)
	registerFilesTools(s)
	registerSecretsTools(s)
	registerPermissionsTools(s)

	stdio := server.NewStdioServer(s)
	if err := stdio.Listen(context.Background(), os.Stdin, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
		os.Exit(1)
	}
}

// Common auth parameter options used by every tool.
func authParams() []mcp.ToolOption {
	return []mcp.ToolOption{
		mcp.WithString("host", mcp.Description("Databricks workspace URL. Overrides the default from env/config.")),
		mcp.WithString("profile", mcp.Description("Name of a ~/.databrickscfg profile to use.")),
		mcp.WithString("token_env_var", mcp.Description("Name of an environment variable containing the access token.")),
	}
}

func toolOpts(desc string, opts ...mcp.ToolOption) []mcp.ToolOption {
	all := []mcp.ToolOption{mcp.WithDescription(desc)}
	all = append(all, opts...)
	all = append(all, authParams()...)
	return all
}

func getOptionalString(args map[string]any, key string) *string {
	if v, ok := args[key]; ok && v != nil {
		s := fmt.Sprintf("%v", v)
		return &s
	}
	return nil
}

func getRequiredString(args map[string]any, key string) string {
	if v, ok := args[key]; ok && v != nil {
		return fmt.Sprintf("%v", v)
	}
	return ""
}

func getInt(args map[string]any, key string, defaultVal int) int {
	if v, ok := args[key]; ok && v != nil {
		switch n := v.(type) {
		case float64:
			return int(n)
		case int:
			return n
		}
	}
	return defaultVal
}

func getBool(args map[string]any, key string, defaultVal bool) bool {
	if v, ok := args[key]; ok && v != nil {
		switch b := v.(type) {
		case bool:
			return b
		}
	}
	return defaultVal
}

func getOptionalInt(args map[string]any, key string) *int {
	if v, ok := args[key]; ok && v != nil {
		switch n := v.(type) {
		case float64:
			i := int(n)
			return &i
		case int:
			return &n
		}
	}
	return nil
}

func getOptionalBool(args map[string]any, key string) *bool {
	if v, ok := args[key]; ok && v != nil {
		switch b := v.(type) {
		case bool:
			return &b
		}
	}
	return nil
}

func getStringSlice(args map[string]any, key string) []string {
	if v, ok := args[key]; ok && v != nil {
		switch arr := v.(type) {
		case []any:
			result := make([]string, 0, len(arr))
			for _, item := range arr {
				result = append(result, fmt.Sprintf("%v", item))
			}
			return result
		case []string:
			return arr
		}
	}
	return nil
}

func getStringMap(args map[string]any, key string) map[string]string {
	if v, ok := args[key]; ok && v != nil {
		switch m := v.(type) {
		case map[string]any:
			result := make(map[string]string, len(m))
			for k, val := range m {
				result[k] = fmt.Sprintf("%v", val)
			}
			return result
		case map[string]string:
			return m
		}
	}
	return nil
}

func textResult(text string) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		Content: []mcp.Content{mcp.NewTextContent(text)},
	}
}

// ── Unity Catalog ────────────────────────────────────────────────────────────

func registerCatalogTools(s *server.MCPServer) {
	s.AddTool(
		mcp.NewTool("catalog_list_catalogs", toolOpts(
			"List all Unity Catalog catalogs accessible to the current user.\n\n"+
				"Returns a sorted JSON array of catalog names.\n\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(catalog.ListCatalogs(ctx, 
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("catalog_list_schemas", toolOpts(
			"List all schemas in a Unity Catalog catalog.\n\n"+
				"Returns a sorted JSON array of schema names.\n\n"+
				"catalog: Catalog name.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("catalog", mcp.Required(), mcp.Description("Catalog name.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(catalog.ListSchemas(ctx, 
				getRequiredString(args, "catalog"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("catalog_list_tables", toolOpts(
			"List all tables in a Unity Catalog schema.\n\n"+
				"Returns a sorted JSON array of table names.\n\n"+
				"catalog: Catalog name.\n"+
				"schema: Schema name.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("catalog", mcp.Required(), mcp.Description("Catalog name.")),
			mcp.WithString("schema", mcp.Required(), mcp.Description("Schema name.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(catalog.ListTables(ctx, 
				getRequiredString(args, "catalog"),
				getRequiredString(args, "schema"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("catalog_describe_table", toolOpts(
			"Describe a Unity Catalog table including columns, types, and properties.\n\n"+
				"Returns JSON with columns (name, type, comment), table type, storage location,\n"+
				"properties, and timestamps.\n\n"+
				"full_name: Three-part name: catalog.schema.table.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("full_name", mcp.Required(), mcp.Description("Three-part name: catalog.schema.table.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(catalog.DescribeTable(ctx, 
				getRequiredString(args, "full_name"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("catalog_get_table_sample", toolOpts(
			"Get sample rows from a Unity Catalog table.\n\n"+
				"Executes SELECT * LIMIT against the table using a SQL warehouse.\n"+
				"Returns a JSON array of row objects.\n\n"+
				"full_name: Three-part name: catalog.schema.table.\n"+
				"limit: Number of rows (default 10, max 100).\n"+
				"warehouse_id: SQL warehouse to use. If omitted, uses the first running warehouse.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("full_name", mcp.Required(), mcp.Description("Three-part name: catalog.schema.table.")),
			mcp.WithInteger("limit", mcp.Description("Number of rows (default 10, max 100).")),
			mcp.WithString("warehouse_id", mcp.Description("SQL warehouse to use. If omitted, uses the first running warehouse.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(catalog.GetTableSample(ctx, 
				getRequiredString(args, "full_name"),
				getInt(args, "limit", 10),
				getOptionalString(args, "warehouse_id"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("catalog_list_volumes", toolOpts(
			"List all volumes in a Unity Catalog schema.\n\n"+
				"Returns a JSON array of volume info (name, type, storage location).\n\n"+
				"catalog: Catalog name.\n"+
				"schema: Schema name.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("catalog", mcp.Required(), mcp.Description("Catalog name.")),
			mcp.WithString("schema", mcp.Required(), mcp.Description("Schema name.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(catalog.ListVolumes(ctx, 
				getRequiredString(args, "catalog"),
				getRequiredString(args, "schema"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("catalog_list_volume_files", toolOpts(
			"List files and directories inside a Unity Catalog volume.\n\n"+
				"Returns a JSON array of entries with name, path, size, and modification time.\n\n"+
				"volume_path: Path under /Volumes/catalog/schema/volume/.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("volume_path", mcp.Required(), mcp.Description("Path under /Volumes/catalog/schema/volume/.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(catalog.ListVolumeFiles(ctx, 
				getRequiredString(args, "volume_path"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("catalog_list_functions", toolOpts(
			"List all functions in a Unity Catalog schema.\n\n"+
				"Returns a JSON array with function name, input params, return type, and language.\n\n"+
				"catalog: Catalog name.\n"+
				"schema: Schema name.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("catalog", mcp.Required(), mcp.Description("Catalog name.")),
			mcp.WithString("schema", mcp.Required(), mcp.Description("Schema name.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(catalog.ListFunctions(ctx, 
				getRequiredString(args, "catalog"),
				getRequiredString(args, "schema"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)
}

// ── SQL Execution ────────────────────────────────────────────────────────────

func registerSQLTools(s *server.MCPServer) {
	s.AddTool(
		mcp.NewTool("sql_execute", toolOpts(
			"Execute a SQL statement against a Databricks SQL warehouse.\n\n"+
				"Returns a JSON array of row objects for queries, or a status message for DDL/DML.\n\n"+
				"statement: SQL statement to execute.\n"+
				"warehouse_id: SQL warehouse ID. If omitted, uses the first running warehouse.\n"+
				"max_rows: Maximum rows to return (default 100, max 10000).\n"+
				"catalog: Default catalog for unqualified names.\n"+
				"schema: Default schema for unqualified names.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("statement", mcp.Required(), mcp.Description("SQL statement to execute.")),
			mcp.WithString("warehouse_id", mcp.Description("SQL warehouse ID. If omitted, uses the first running warehouse.")),
			mcp.WithInteger("max_rows", mcp.Description("Maximum rows to return (default 100, max 10000).")),
			mcp.WithString("catalog", mcp.Description("Default catalog for unqualified names.")),
			mcp.WithString("schema", mcp.Description("Default schema for unqualified names.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(sqlt.ExecuteSQL(ctx, 
				getRequiredString(args, "statement"),
				getOptionalString(args, "warehouse_id"),
				getInt(args, "max_rows", 100),
				getOptionalString(args, "catalog"),
				getOptionalString(args, "schema"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("sql_list_warehouses", toolOpts(
			"List all SQL warehouses in the workspace.\n\n"+
				"Returns a JSON array with id, name, state, cluster_size, and auto_stop_mins.\n\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(sqlt.ListWarehouses(ctx, 
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("sql_get_warehouse", toolOpts(
			"Get full configuration details for a SQL warehouse.\n\n"+
				"warehouse_id: SQL warehouse ID.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("warehouse_id", mcp.Required(), mcp.Description("SQL warehouse ID.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(sqlt.GetWarehouse(ctx, 
				getRequiredString(args, "warehouse_id"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("sql_start_warehouse", toolOpts(
			"Start a stopped SQL warehouse. Does not wait for it to finish starting.\n\n"+
				"warehouse_id: SQL warehouse ID.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("warehouse_id", mcp.Required(), mcp.Description("SQL warehouse ID.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(sqlt.StartWarehouse(ctx, 
				getRequiredString(args, "warehouse_id"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("sql_stop_warehouse", toolOpts(
			"Stop a running SQL warehouse.\n\n"+
				"warehouse_id: SQL warehouse ID.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("warehouse_id", mcp.Required(), mcp.Description("SQL warehouse ID.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(sqlt.StopWarehouse(ctx, 
				getRequiredString(args, "warehouse_id"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)
}

// ── Query History ────────────────────────────────────────────────────────────

func registerQueryHistoryTools(s *server.MCPServer) {
	s.AddTool(
		mcp.NewTool("query_history_list", toolOpts(
			"List recent SQL queries from query history.\n\n"+
				"Returns a JSON array of queries with id, statement (truncated), status,\n"+
				"duration, rows produced, user, and warehouse.\n\n"+
				"warehouse_id: Filter to a specific warehouse.\n"+
				"user_name: Filter by user.\n"+
				"status: Filter by status (FINISHED, FAILED, CANCELED, RUNNING).\n"+
				"max_results: Max results (default 25, max 100).\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("warehouse_id", mcp.Description("Filter to a specific warehouse.")),
			mcp.WithString("user_name", mcp.Description("Filter by user.")),
			mcp.WithString("status", mcp.Description("Filter by status (FINISHED, FAILED, CANCELED, RUNNING).")),
			mcp.WithInteger("max_results", mcp.Description("Max results (default 25, max 100).")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(queryhistory.ListRecentQueries(ctx, 
				getOptionalString(args, "warehouse_id"),
				getOptionalString(args, "user_name"),
				getOptionalString(args, "status"),
				getInt(args, "max_results", 25),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("query_history_get", toolOpts(
			"Get full details for a specific query from history.\n\n"+
				"Returns the complete statement text, metrics, and error message if failed.\n\n"+
				"query_id: Query ID.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("query_id", mcp.Required(), mcp.Description("Query ID.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(queryhistory.GetQuery(ctx, 
				getRequiredString(args, "query_id"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)
}

// ── Jobs & Workflows ─────────────────────────────────────────────────────────

func registerJobsTools(s *server.MCPServer) {
	s.AddTool(
		mcp.NewTool("jobs_list", toolOpts(
			"List jobs in the workspace. Results are paged (default 25 per page).\n\n"+
				"name: Filter by job name (substring match).\n"+
				"page_size: Number of jobs per page (default 25).\n"+
				"page_token: Token from a previous response to fetch the next/previous page.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("name", mcp.Description("Filter by job name (substring match).")),
			mcp.WithInteger("page_size", mcp.Description("Number of jobs per page (default 25).")),
			mcp.WithString("page_token", mcp.Description("Token from a previous response to fetch the next/previous page.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(jobs.ListJobs(ctx, 
				getOptionalString(args, "name"),
				getInt(args, "page_size", 25),
				getOptionalString(args, "page_token"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("jobs_get", toolOpts(
			"Get full configuration for a job.\n\n"+
				"Returns tasks, schedule, clusters, parameters, tags, and notifications.\n\n"+
				"job_id: Job ID.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithInteger("job_id", mcp.Required(), mcp.Description("Job ID.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(jobs.GetJob(ctx, 
				int64(getInt(args, "job_id", 0)),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("jobs_list_runs", toolOpts(
			"List job runs. Results are paged (default 25 per page).\n\n"+
				"Returns a JSON object with runs array, and pagination metadata.\n\n"+
				"job_id: Filter to a specific job.\n"+
				"active_only: Only show active (in-progress) runs.\n"+
				"page_size: Number of runs per page (default 25).\n"+
				"page_token: Token from a previous response to fetch the next/previous page.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithInteger("job_id", mcp.Description("Filter to a specific job.")),
			mcp.WithBoolean("active_only", mcp.Description("Only show active (in-progress) runs.")),
			mcp.WithInteger("page_size", mcp.Description("Number of runs per page (default 25).")),
			mcp.WithString("page_token", mcp.Description("Token from a previous response to fetch the next/previous page.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			var jobID *int64
			if v := getOptionalInt(args, "job_id"); v != nil {
				id := int64(*v)
				jobID = &id
			}
			return textResult(jobs.ListJobRuns(ctx, 
				jobID,
				getBool(args, "active_only", false),
				getInt(args, "page_size", 25),
				getOptionalString(args, "page_token"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("jobs_get_run", toolOpts(
			"Get full details for a job run.\n\n"+
				"Returns per-task states, start/end times, cluster info, error messages,\n"+
				"and attempt number.\n\n"+
				"run_id: Run ID.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithInteger("run_id", mcp.Required(), mcp.Description("Run ID.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(jobs.GetJobRun(ctx, 
				int64(getInt(args, "run_id", 0)),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("jobs_get_run_output", toolOpts(
			"Get output for a job run.\n\n"+
				"Returns notebook output, error trace, and logs depending on task type.\n\n"+
				"run_id: Run ID.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithInteger("run_id", mcp.Required(), mcp.Description("Run ID.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(jobs.GetRunOutput(ctx, 
				int64(getInt(args, "run_id", 0)),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("jobs_run", toolOpts(
			"Trigger a job run. Does not wait for completion.\n\n"+
				"Returns the run_id for tracking.\n\n"+
				"job_id: Job ID.\n"+
				"parameters: Optional parameter overrides as key/value map.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithInteger("job_id", mcp.Required(), mcp.Description("Job ID.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(jobs.RunJob(ctx, 
				int64(getInt(args, "job_id", 0)),
				getStringMap(args, "parameters"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("jobs_cancel_run", toolOpts(
			"Cancel an in-progress job run.\n\n"+
				"run_id: Run ID.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithInteger("run_id", mcp.Required(), mcp.Description("Run ID.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(jobs.CancelRun(ctx, 
				int64(getInt(args, "run_id", 0)),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("jobs_repair_run", toolOpts(
			"Repair a failed multi-task job run by re-running failed tasks.\n\n"+
				"run_id: Run ID of a failed multi-task job.\n"+
				"rerun_tasks: Specific task keys to re-run. If omitted, re-runs all failed tasks.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithInteger("run_id", mcp.Required(), mcp.Description("Run ID of a failed multi-task job.")),
			mcp.WithArray("rerun_tasks", mcp.Description("Specific task keys to re-run. If omitted, re-runs all failed tasks.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(jobs.RepairRun(ctx, 
				int64(getInt(args, "run_id", 0)),
				getStringSlice(args, "rerun_tasks"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)
}

// ── Clusters ─────────────────────────────────────────────────────────────────

func registerClustersTools(s *server.MCPServer) {
	s.AddTool(
		mcp.NewTool("clusters_list", toolOpts(
			"List clusters in the workspace. Results are paged (default 20 per page).\n\n"+
				"cluster_sources: Comma-separated filter. Values: UI, API, JOB.\n"+
				"cluster_states: Comma-separated filter. Values: RUNNING, TERMINATED, PENDING, etc.\n"+
				"is_pinned: If true, only return pinned clusters.\n"+
				"page_size: Number of clusters per page (default 20).\n"+
				"page_token: Token from a previous response to fetch the next/previous page.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("cluster_sources", mcp.Description("Comma-separated filter. Values: UI, API, JOB.")),
			mcp.WithString("cluster_states", mcp.Description("Comma-separated filter. Values: RUNNING, TERMINATED, PENDING, etc.")),
			mcp.WithBoolean("is_pinned", mcp.Description("If true, only return pinned clusters.")),
			mcp.WithInteger("page_size", mcp.Description("Number of clusters per page (default 20).")),
			mcp.WithString("page_token", mcp.Description("Token from a previous response to fetch the next/previous page.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(clusters.ListClusters(ctx, 
				getOptionalString(args, "cluster_sources"),
				getOptionalString(args, "cluster_states"),
				getOptionalBool(args, "is_pinned"),
				getInt(args, "page_size", 20),
				getOptionalString(args, "page_token"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("clusters_get", toolOpts(
			"Get full configuration and state for a cluster.\n\n"+
				"cluster_id: Cluster ID.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("cluster_id", mcp.Required(), mcp.Description("Cluster ID.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(clusters.GetCluster(ctx, 
				getRequiredString(args, "cluster_id"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("clusters_get_events", toolOpts(
			"Get recent events for a cluster.\n\n"+
				"Returns a JSON array of events with type, timestamp, and details\n"+
				"(especially termination reasons).\n\n"+
				"cluster_id: Cluster ID.\n"+
				"max_results: Max events (default 25).\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("cluster_id", mcp.Required(), mcp.Description("Cluster ID.")),
			mcp.WithInteger("max_results", mcp.Description("Max events (default 25).")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(clusters.GetClusterEvents(ctx, 
				getRequiredString(args, "cluster_id"),
				getInt(args, "max_results", 25),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("clusters_start", toolOpts(
			"Start a terminated cluster. Does not wait for it to finish starting.\n\n"+
				"cluster_id: Cluster ID.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("cluster_id", mcp.Required(), mcp.Description("Cluster ID.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(clusters.StartCluster(ctx, 
				getRequiredString(args, "cluster_id"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("clusters_terminate", toolOpts(
			"Terminate a running cluster. This stops the cluster but does not delete it.\n\n"+
				"cluster_id: Cluster ID.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("cluster_id", mcp.Required(), mcp.Description("Cluster ID.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(clusters.TerminateCluster(ctx, 
				getRequiredString(args, "cluster_id"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)
}

// ── Pipelines (Delta Live Tables) ────────────────────────────────────────────

func registerPipelinesTools(s *server.MCPServer) {
	s.AddTool(
		mcp.NewTool("pipelines_list", toolOpts(
			"List Delta Live Tables pipelines.\n\n"+
				"Returns a JSON array with pipeline_id, name, state, and creator.\n\n"+
				"name: Filter by pipeline name (substring match).\n"+
				"max_results: Max results (default 25).\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("name", mcp.Description("Filter by pipeline name (substring match).")),
			mcp.WithInteger("max_results", mcp.Description("Max results (default 25).")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(pipelines.ListPipelines(ctx, 
				getOptionalString(args, "name"),
				getInt(args, "max_results", 25),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("pipelines_get", toolOpts(
			"Get full configuration for a pipeline.\n\n"+
				"Returns target catalog/schema, clusters, libraries, and notifications.\n\n"+
				"pipeline_id: Pipeline ID.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("pipeline_id", mcp.Required(), mcp.Description("Pipeline ID.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(pipelines.GetPipeline(ctx, 
				getRequiredString(args, "pipeline_id"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("pipelines_list_events", toolOpts(
			"List recent events for a pipeline.\n\n"+
				"Returns a JSON array of events including update progress, errors,\n"+
				"and data quality metrics.\n\n"+
				"pipeline_id: Pipeline ID.\n"+
				"max_results: Max events (default 25).\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("pipeline_id", mcp.Required(), mcp.Description("Pipeline ID.")),
			mcp.WithInteger("max_results", mcp.Description("Max events (default 25).")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(pipelines.ListPipelineEvents(ctx, 
				getRequiredString(args, "pipeline_id"),
				getInt(args, "max_results", 25),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("pipelines_start", toolOpts(
			"Start a pipeline update. Returns the update_id for tracking.\n\n"+
				"pipeline_id: Pipeline ID.\n"+
				"full_refresh: If true, full refresh instead of incremental (default false).\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("pipeline_id", mcp.Required(), mcp.Description("Pipeline ID.")),
			mcp.WithBoolean("full_refresh", mcp.Description("If true, full refresh instead of incremental (default false).")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(pipelines.StartPipeline(ctx, 
				getRequiredString(args, "pipeline_id"),
				getBool(args, "full_refresh", false),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("pipelines_stop", toolOpts(
			"Stop a running pipeline.\n\n"+
				"pipeline_id: Pipeline ID.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("pipeline_id", mcp.Required(), mcp.Description("Pipeline ID.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(pipelines.StopPipeline(ctx, 
				getRequiredString(args, "pipeline_id"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)
}

// ── Workspace / Notebooks ────────────────────────────────────────────────────

func registerWorkspaceTools(s *server.MCPServer) {
	s.AddTool(
		mcp.NewTool("workspace_list", toolOpts(
			"List objects in a workspace directory.\n\n"+
				"Returns a JSON array of objects with path, object_type (NOTEBOOK, DIRECTORY,\n"+
				"FILE, REPO), and language (for notebooks).\n\n"+
				"path: Workspace path (e.g. /Users/user@example.com).\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("path", mcp.Required(), mcp.Description("Workspace path (e.g. /Users/user@example.com).")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(workspace.ListWorkspace(ctx, 
				getRequiredString(args, "path"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("workspace_export_notebook", toolOpts(
			"Export a notebook from the workspace.\n\n"+
				"For SOURCE format, returns the notebook content as text.\n"+
				"For other formats (HTML, JUPYTER, DBC), returns base64-encoded content.\n\n"+
				"path: Notebook path in workspace.\n"+
				"format: Export format: SOURCE (default), HTML, JUPYTER, DBC.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("path", mcp.Required(), mcp.Description("Notebook path in workspace.")),
			mcp.WithString("format", mcp.Description("Export format: SOURCE (default), HTML, JUPYTER, DBC.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			format := "SOURCE"
			if v := getOptionalString(args, "format"); v != nil {
				format = *v
			}
			return textResult(workspace.ExportNotebook(ctx, 
				getRequiredString(args, "path"),
				format,
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)
}

// ── Files (DBFS + Volumes) ───────────────────────────────────────────────────

func registerFilesTools(s *server.MCPServer) {
	s.AddTool(
		mcp.NewTool("files_list", toolOpts(
			"List files in a DBFS or Volumes path.\n\n"+
				"Use dbfs:/... prefix for DBFS paths, or /Volumes/... for Unity Catalog volumes.\n"+
				"Returns a JSON array of entries with path, name, size, and directory flag.\n\n"+
				"path: DBFS path (dbfs:/...) or Volumes path (/Volumes/...).\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("path", mcp.Required(), mcp.Description("DBFS path (dbfs:/...) or Volumes path (/Volumes/...).")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(files.ListFiles(ctx, 
				getRequiredString(args, "path"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("files_read", toolOpts(
			"Read a file from Volumes or DBFS.\n\n"+
				"Returns text content for text files, or a size summary for binary files.\n"+
				"Guards against reading huge files.\n\n"+
				"path: Path to the file.\n"+
				"max_bytes: Max bytes to read (default 1MB, max 10MB).\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("path", mcp.Required(), mcp.Description("Path to the file.")),
			mcp.WithInteger("max_bytes", mcp.Description("Max bytes to read (default 1MB, max 10MB).")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(files.ReadFile(ctx, 
				getRequiredString(args, "path"),
				getInt(args, "max_bytes", 1000000),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)
}

// ── Secrets ──────────────────────────────────────────────────────────────────

func registerSecretsTools(s *server.MCPServer) {
	s.AddTool(
		mcp.NewTool("secrets_list_scopes", toolOpts(
			"List all secret scopes in the workspace.\n\n"+
				"Returns a sorted JSON array of scope names.\n\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(secrets.ListSecretScopes(ctx, 
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("secrets_list_keys", toolOpts(
			"List secret key names in a scope. Values are never returned.\n\n"+
				"scope: Secret scope name.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("scope", mcp.Required(), mcp.Description("Secret scope name.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(secrets.ListSecrets(ctx, 
				getRequiredString(args, "scope"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)
}

// ── Permissions & Grants ─────────────────────────────────────────────────────

func registerPermissionsTools(s *server.MCPServer) {
	s.AddTool(
		mcp.NewTool("permissions_get_grants", toolOpts(
			"Get Unity Catalog grants on a securable object.\n\n"+
				"Returns a JSON array of grant entries (principal, privileges).\n\n"+
				"securable_type: One of: CATALOG, SCHEMA, TABLE, VOLUME, FUNCTION.\n"+
				"full_name: Full name of the securable.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("securable_type", mcp.Required(), mcp.Description("One of: CATALOG, SCHEMA, TABLE, VOLUME, FUNCTION.")),
			mcp.WithString("full_name", mcp.Required(), mcp.Description("Full name of the securable.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(permissions.GetGrants(ctx, 
				getRequiredString(args, "securable_type"),
				getRequiredString(args, "full_name"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("permissions_get_object_permissions", toolOpts(
			"Get access control list for a workspace object.\n\n"+
				"Returns a JSON array of ACL entries with principal and permission levels.\n\n"+
				"object_type: One of: clusters, jobs, pipelines, sql/warehouses, etc.\n"+
				"object_id: Object ID.\n"+
				"host: Databricks workspace URL. Overrides the default from env/config.\n"+
				"profile: Name of a ~/.databrickscfg profile to use.\n"+
				"token_env_var: Name of an environment variable containing the access token.",
			mcp.WithString("object_type", mcp.Required(), mcp.Description("One of: clusters, jobs, pipelines, sql/warehouses, etc.")),
			mcp.WithString("object_id", mcp.Required(), mcp.Description("Object ID.")),
		)...),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			return textResult(permissions.GetObjectPermissions(ctx, 
				getRequiredString(args, "object_type"),
				getRequiredString(args, "object_id"),
				getOptionalString(args, "host"),
				getOptionalString(args, "profile"),
				getOptionalString(args, "token_env_var"),
			)), nil
		},
	)
}
