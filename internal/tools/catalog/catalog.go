package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/databricks/databricks-sdk-go/service/files"

	"github.com/BrianDeacon/databricks-utils-mcp-go/internal/client"
)

func ListCatalogs(ctx context.Context, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	all, err := w.Catalogs.ListAll(ctx, catalog.ListCatalogsRequest{})
	if err != nil {
		return fmt.Sprintf("Error listing catalogs: %v", err)
	}

	var names []string
	for _, c := range all {
		names = append(names, c.Name)
	}

	if len(names) == 0 {
		return "No catalogs found."
	}

	sort.Strings(names)
	out, _ := json.MarshalIndent(names, "", "  ")
	return string(out)
}

func ListSchemas(ctx context.Context, catalogName string, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	all, err := w.Schemas.ListAll(ctx, catalog.ListSchemasRequest{CatalogName: catalogName})
	if err != nil {
		return fmt.Sprintf("Error listing schemas: %v", err)
	}

	var names []string
	for _, s := range all {
		names = append(names, s.Name)
	}

	if len(names) == 0 {
		return fmt.Sprintf("No schemas found in catalog '%s'.", catalogName)
	}

	sort.Strings(names)
	out, _ := json.MarshalIndent(names, "", "  ")
	return string(out)
}

func ListTables(ctx context.Context, catalogName, schemaName string, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	all, err := w.Tables.ListAll(ctx, catalog.ListTablesRequest{
		CatalogName: catalogName,
		SchemaName:  schemaName,
	})
	if err != nil {
		return fmt.Sprintf("Error listing tables: %v", err)
	}

	var names []string
	for _, t := range all {
		names = append(names, t.Name)
	}

	if len(names) == 0 {
		return fmt.Sprintf("No tables found in '%s.%s'.", catalogName, schemaName)
	}

	sort.Strings(names)
	out, _ := json.MarshalIndent(names, "", "  ")
	return string(out)
}

func DescribeTable(ctx context.Context, fullName string, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	t, err := w.Tables.GetByFullName(ctx, fullName)
	if err != nil {
		return fmt.Sprintf("Error describing table: %v", err)
	}

	type colInfo struct {
		Name    string `json:"name"`
		Type    string `json:"type"`
		Comment string `json:"comment,omitempty"`
	}
	var columns []colInfo
	for _, c := range t.Columns {
		columns = append(columns, colInfo{
			Name:    c.Name,
			Type:    string(c.TypeName),
			Comment: c.Comment,
		})
	}

	result := map[string]any{
		"full_name":        t.FullName,
		"table_type":       string(t.TableType),
		"columns":          columns,
		"storage_location": t.StorageLocation,
		"properties":       t.Properties,
		"created_at":       t.CreatedAt,
		"updated_at":       t.UpdatedAt,
	}

	out, _ := json.MarshalIndent(result, "", "  ")
	return string(out)
}

func ListVolumes(ctx context.Context, catalogName, schemaName string, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	all, err := w.Volumes.ListAll(ctx, catalog.ListVolumesRequest{
		CatalogName: catalogName,
		SchemaName:  schemaName,
	})
	if err != nil {
		return fmt.Sprintf("Error listing volumes: %v", err)
	}

	type volInfo struct {
		Name            string `json:"name"`
		VolumeType      string `json:"volume_type"`
		StorageLocation string `json:"storage_location,omitempty"`
	}
	var volumes []volInfo
	for _, v := range all {
		volumes = append(volumes, volInfo{
			Name:            v.Name,
			VolumeType:      string(v.VolumeType),
			StorageLocation: v.StorageLocation,
		})
	}

	if len(volumes) == 0 {
		return fmt.Sprintf("No volumes found in '%s.%s'.", catalogName, schemaName)
	}

	sort.Slice(volumes, func(i, j int) bool { return volumes[i].Name < volumes[j].Name })
	out, _ := json.MarshalIndent(volumes, "", "  ")
	return string(out)
}

func ListVolumeFiles(ctx context.Context, volumePath string, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	iter := w.Files.ListDirectoryContents(ctx, files.ListDirectoryContentsRequest{
		DirectoryPath: volumePath,
	})

	type entry struct {
		Name         string `json:"name"`
		Path         string `json:"path"`
		FileSize     *int64 `json:"file_size,omitempty"`
		LastModified *int64 `json:"last_modified,omitempty"`
	}
	var entries []entry
	for iter.HasNext(ctx) {
		item, err := iter.Next(ctx)
		if err != nil {
			return fmt.Sprintf("Error listing volume files: %v", err)
		}
		e := entry{
			Name: item.Name,
			Path: item.Path,
		}
		if item.FileSize != 0 {
			sz := item.FileSize
			e.FileSize = &sz
		}
		if item.LastModified != 0 {
			lm := item.LastModified
			e.LastModified = &lm
		}
		entries = append(entries, e)
	}

	if len(entries) == 0 {
		return fmt.Sprintf("No files found at '%s'.", volumePath)
	}

	out, _ := json.MarshalIndent(entries, "", "  ")
	return string(out)
}

func ListFunctions(ctx context.Context, catalogName, schemaName string, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	all, err := w.Functions.ListAll(ctx, catalog.ListFunctionsRequest{
		CatalogName: catalogName,
		SchemaName:  schemaName,
	})
	if err != nil {
		return fmt.Sprintf("Error listing functions: %v", err)
	}

	type paramInfo struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}
	type funcInfo struct {
		Name        string      `json:"name"`
		InputParams []paramInfo `json:"input_params"`
		ReturnType  string      `json:"return_type"`
		Language    string      `json:"language,omitempty"`
	}
	var funcs []funcInfo
	for _, f := range all {
		var params []paramInfo
		if f.InputParams != nil {
			for _, p := range f.InputParams.Parameters {
				params = append(params, paramInfo{Name: p.Name, Type: string(p.TypeName)})
			}
		}
		funcs = append(funcs, funcInfo{
			Name:        f.Name,
			InputParams: params,
			ReturnType:  string(f.DataType),
			Language:    f.ExternalLanguage,
		})
	}

	if len(funcs) == 0 {
		return fmt.Sprintf("No functions found in '%s.%s'.", catalogName, schemaName)
	}

	sort.Slice(funcs, func(i, j int) bool { return funcs[i].Name < funcs[j].Name })
	out, _ := json.MarshalIndent(funcs, "", "  ")
	return string(out)
}
