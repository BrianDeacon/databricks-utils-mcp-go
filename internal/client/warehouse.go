package client

import (
	"context"
	"fmt"
	"sync"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/sql"
)

var (
	warehouseMu       sync.Mutex
	defaultWarehouses = make(map[string]string)
)

func GetDefaultWarehouse(ctx context.Context, w *databricks.WorkspaceClient, key string) (string, error) {
	warehouseMu.Lock()
	defer warehouseMu.Unlock()

	if wid, ok := defaultWarehouses[key]; ok {
		return wid, nil
	}

	all, err := w.Warehouses.ListAll(ctx, sql.ListWarehousesRequest{})
	if err != nil {
		return "", fmt.Errorf("failed to list warehouses: %w", err)
	}

	for _, wh := range all {
		if wh.State == sql.StateRunning {
			defaultWarehouses[key] = wh.Id
			return wh.Id, nil
		}
	}

	return "", fmt.Errorf("no running SQL warehouse found; specify warehouse_id explicitly")
}
