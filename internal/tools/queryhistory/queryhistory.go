package queryhistory

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/databricks/databricks-sdk-go/service/sql"

	"github.com/BrianDeacon/databricks-utils-mcp-go/internal/client"
)

func ListRecentQueries(ctx context.Context, warehouseID, userName, status *string, maxResults int, host, profile, tokenEnvVar *string) string {
	if maxResults <= 0 {
		maxResults = 25
	}
	if maxResults > 100 {
		maxResults = 100
	}

	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	filterBy := &sql.QueryFilter{}
	if warehouseID != nil && *warehouseID != "" {
		filterBy.WarehouseIds = []string{*warehouseID}
	}
	if userName != nil && *userName != "" {
		filterBy.UserIds = []int64{} // user_name filter handled differently in Go SDK
	}
	if status != nil && *status != "" {
		filterBy.Statuses = []sql.QueryStatus{sql.QueryStatus(*status)}
	}

	resp, err := w.QueryHistory.List(ctx, sql.ListQueryHistoryRequest{
		FilterBy:       filterBy,
		MaxResults:     maxResults,
		IncludeMetrics: true,
	})
	if err != nil {
		return fmt.Sprintf("Error listing queries: %v", err)
	}

	type queryInfo struct {
		QueryID            string `json:"query_id"`
		Statement          string `json:"statement"`
		Status             string `json:"status,omitempty"`
		DurationMs         int64  `json:"duration_ms,omitempty"`
		RowsProduced       int64  `json:"rows_produced,omitempty"`
		UserName           string `json:"user_name,omitempty"`
		WarehouseID        string `json:"warehouse_id,omitempty"`
		ExecutionEndTimeMs int64  `json:"execution_end_time_ms,omitempty"`
	}
	var queries []queryInfo
	for _, q := range resp.Res {
		stmt := q.QueryText
		if len(stmt) > 200 {
			stmt = stmt[:200]
		}
		queries = append(queries, queryInfo{
			QueryID:            q.QueryId,
			Statement:          stmt,
			Status:             string(q.Status),
			DurationMs:         q.Duration,
			RowsProduced:       q.RowsProduced,
			UserName:           q.UserName,
			WarehouseID:        q.WarehouseId,
			ExecutionEndTimeMs: q.ExecutionEndTimeMs,
		})
	}

	if len(queries) == 0 {
		return "No queries found matching the filters."
	}

	out, _ := json.MarshalIndent(queries, "", "  ")
	return string(out)
}

func GetQuery(ctx context.Context, queryID string, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	resp, err := w.QueryHistory.List(ctx, sql.ListQueryHistoryRequest{
		MaxResults:     100,
		IncludeMetrics: true,
	})
	if err != nil {
		return fmt.Sprintf("Error fetching query history: %v", err)
	}

	for _, q := range resp.Res {
		if q.QueryId == queryID {
			result := map[string]any{
				"query_id":               q.QueryId,
				"statement":              q.QueryText,
				"status":                 string(q.Status),
				"duration_ms":            q.Duration,
				"rows_produced":          q.RowsProduced,
				"user_name":              q.UserName,
				"warehouse_id":           q.WarehouseId,
				"execution_end_time_ms":  q.ExecutionEndTimeMs,
				"error_message":          q.ErrorMessage,
			}
			if q.Metrics != nil {
				result["metrics"] = map[string]any{
					"total_time_ms":       q.Metrics.TotalTimeMs,
					"compilation_time_ms": q.Metrics.CompilationTimeMs,
					"execution_time_ms":   q.Metrics.ExecutionTimeMs,
					"rows_produced_count": q.Metrics.RowsProducedCount,
				}
			}
			out, _ := json.MarshalIndent(result, "", "  ")
			return string(out)
		}
	}

	return fmt.Sprintf("Query '%s' not found in recent history.", queryID)
}
