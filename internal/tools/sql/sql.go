package sqlt

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/databricks/databricks-sdk-go/service/sql"

	"github.com/BrianDeacon/databricks-utils-mcp-go/internal/client"
)

func ExecuteSQL(ctx context.Context, statement string, warehouseID *string, maxRows int, catalogName, schemaName *string, host, profile, tokenEnvVar *string) string {
	if maxRows <= 0 {
		maxRows = 100
	}
	if maxRows > 10000 {
		maxRows = 10000
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

	req := sql.ExecuteStatementRequest{
		WarehouseId: wid,
		Statement:   statement,
		WaitTimeout: "30s",
		RowLimit:    int64(maxRows),
	}
	if catalogName != nil && *catalogName != "" {
		req.Catalog = *catalogName
	}
	if schemaName != nil && *schemaName != "" {
		req.Schema = *schemaName
	}

	resp, err := w.StatementExecution.ExecuteStatement(ctx, req)
	if err != nil {
		return fmt.Sprintf("Error executing statement: %v", err)
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

	result := map[string]any{
		"columns":   columns,
		"row_count": len(rows),
		"rows":      rows,
	}

	if resp.Status != nil {
		result["status"] = string(resp.Status.State)
	}

	out, _ := json.MarshalIndent(result, "", "  ")
	return string(out)
}

func ListWarehouses(ctx context.Context, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	all, err := w.Warehouses.ListAll(ctx, sql.ListWarehousesRequest{})
	if err != nil {
		return fmt.Sprintf("Error listing warehouses: %v", err)
	}

	type whInfo struct {
		ID           string `json:"id"`
		Name         string `json:"name"`
		State        string `json:"state"`
		ClusterSize  string `json:"cluster_size"`
		AutoStopMins int   `json:"auto_stop_mins"`
	}
	var warehouses []whInfo
	for _, wh := range all {
		warehouses = append(warehouses, whInfo{
			ID:           wh.Id,
			Name:         wh.Name,
			State:        string(wh.State),
			ClusterSize:  wh.ClusterSize,
			AutoStopMins: wh.AutoStopMins,
		})
	}

	if len(warehouses) == 0 {
		return "No SQL warehouses found."
	}

	sort.Slice(warehouses, func(i, j int) bool { return warehouses[i].Name < warehouses[j].Name })
	out, _ := json.MarshalIndent(warehouses, "", "  ")
	return string(out)
}

func GetWarehouse(ctx context.Context, warehouseID string, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	wh, err := w.Warehouses.GetById(ctx, warehouseID)
	if err != nil {
		return fmt.Sprintf("Error getting warehouse: %v", err)
	}

	result := map[string]any{
		"id":             wh.Id,
		"name":           wh.Name,
		"state":          string(wh.State),
		"cluster_size":   wh.ClusterSize,
		"auto_stop_mins": wh.AutoStopMins,
		"num_clusters":   wh.NumClusters,
		"creator_name":   wh.CreatorName,
	}

	out, _ := json.MarshalIndent(result, "", "  ")
	return string(out)
}

func StartWarehouse(ctx context.Context, warehouseID string, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	_, err = w.Warehouses.Start(ctx, sql.StartRequest{Id: warehouseID})
	if err != nil {
		return fmt.Sprintf("Error starting warehouse: %v", err)
	}

	return fmt.Sprintf("Start requested for warehouse '%s'. It may take a few minutes to become available.", warehouseID)
}

func StopWarehouse(ctx context.Context, warehouseID string, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	_, err = w.Warehouses.Stop(ctx, sql.StopRequest{Id: warehouseID})
	if err != nil {
		return fmt.Sprintf("Error stopping warehouse: %v", err)
	}

	return fmt.Sprintf("Stop requested for warehouse '%s'.", warehouseID)
}
