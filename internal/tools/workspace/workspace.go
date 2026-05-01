package workspace

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/databricks/databricks-sdk-go/service/workspace"

	"github.com/BrianDeacon/databricks-utils-mcp-go/internal/client"
)

func ListWorkspace(ctx context.Context, path string, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	all, err := w.Workspace.ListAll(ctx, workspace.ListWorkspaceRequest{Path: path})
	if err != nil {
		return fmt.Sprintf("Error listing workspace: %v", err)
	}

	type entry struct {
		Path       string `json:"path"`
		ObjectType string `json:"object_type,omitempty"`
		Language   string `json:"language,omitempty"`
	}
	var entries []entry
	for _, obj := range all {
		entries = append(entries, entry{
			Path:       obj.Path,
			ObjectType: string(obj.ObjectType),
			Language:   string(obj.Language),
		})
	}

	if len(entries) == 0 {
		return fmt.Sprintf("No objects found at '%s'.", path)
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	out, _ := json.MarshalIndent(entries, "", "  ")
	return string(out)
}

func ExportNotebook(ctx context.Context, path, format string, host, profile, tokenEnvVar *string) string {
	if format == "" {
		format = "SOURCE"
	}

	validFormats := map[string]workspace.ExportFormat{
		"SOURCE":  workspace.ExportFormatSource,
		"HTML":    workspace.ExportFormatHtml,
		"JUPYTER": workspace.ExportFormatJupyter,
		"DBC":     workspace.ExportFormatDbc,
	}
	fmt_, ok := validFormats[format]
	if !ok {
		return fmt.Sprintf("Invalid format '%s'. Use one of: SOURCE, HTML, JUPYTER, DBC.", format)
	}

	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	resp, err := w.Workspace.Export(ctx, workspace.ExportRequest{Path: path, Format: fmt_})
	if err != nil {
		return fmt.Sprintf("Error exporting notebook: %v", err)
	}

	if resp.Content == "" {
		return fmt.Sprintf("Notebook at '%s' returned no content.", path)
	}

	decoded, err := base64.StdEncoding.DecodeString(resp.Content)
	if err != nil {
		return fmt.Sprintf("Error decoding content: %v", err)
	}

	if format == "SOURCE" {
		return string(decoded)
	}

	result := map[string]any{
		"path":           path,
		"format":         format,
		"size_bytes":     len(decoded),
		"content_base64": resp.Content,
	}
	out, _ := json.MarshalIndent(result, "", "  ")
	return string(out)
}
