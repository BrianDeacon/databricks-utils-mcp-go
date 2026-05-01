package catalog

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/databricks/databricks-sdk-go/service/sql"

	"github.com/BrianDeacon/databricks-utils-mcp-go/internal/client"
)

func GetTableSample(ctx context.Context, fullName string, limit int, warehouseID *string, host, profile, tokenEnvVar *string) string {
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}

	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	wid := ""
	if warehouseID != nil && *warehouseID != "" {
		wid = *warehouseID
	} else {
		key := "_default"
		if profile != nil && *profile != "" {
			key = *profile
		} else if host != nil && *host != "" {
			key = *host
		}
		resolved, err := client.GetDefaultWarehouse(ctx, w, key)
		if err != nil {
			return fmt.Sprintf("Error: %v", err)
		}
		wid = resolved
	}

	stmt := fmt.Sprintf("SELECT * FROM %s LIMIT %d", fullName, limit)
	resp, err := w.StatementExecution.ExecuteStatement(ctx, sql.ExecuteStatementRequest{
		WarehouseId: wid,
		Statement:   stmt,
		WaitTimeout: "30s",
	})
	if err != nil {
		return fmt.Sprintf("Error executing sample query: %v", err)
	}

	if resp.Status != nil && resp.Status.State == sql.StatementStateFailed {
		msg := "Query failed"
		if resp.Status.Error != nil {
			msg = resp.Status.Error.Message
		}
		return fmt.Sprintf("Error: %s", msg)
	}

	var columns []string
	if resp.Manifest != nil {
		for _, col := range resp.Manifest.Schema.Columns {
			columns = append(columns, col.Name)
		}
	}

	var rows []map[string]any
	if resp.Result != nil && resp.Result.DataArray != nil {
		for _, row := range resp.Result.DataArray {
			rowMap := make(map[string]any)
			for i, val := range row {
				if i < len(columns) {
					rowMap[columns[i]] = val
				}
			}
			rows = append(rows, rowMap)
		}
	}

	if len(rows) == 0 {
		return fmt.Sprintf("No data found in '%s'.", fullName)
	}

	out, _ := json.MarshalIndent(rows, "", "  ")
	return string(out)
}
