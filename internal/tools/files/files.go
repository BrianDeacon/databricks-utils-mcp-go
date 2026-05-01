package files

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/databricks/databricks-sdk-go/service/files"

	"github.com/BrianDeacon/databricks-utils-mcp-go/internal/client"
)

const (
	maxBytesDefault = 1_000_000
	maxBytesCap     = 10_000_000
)

func ListFiles(ctx context.Context, path string, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	if strings.HasPrefix(path, "dbfs:") {
		all, err := w.Dbfs.ListAll(ctx, files.ListDbfsRequest{Path: path})
		if err != nil {
			return fmt.Sprintf("Error listing files: %v", err)
		}

		type entry struct {
			Path     string `json:"path"`
			Name     string `json:"name"`
			IsDir    bool   `json:"is_dir"`
			FileSize int64  `json:"file_size"`
		}
		var entries []entry
		for _, f := range all {
			name := f.Path
			if idx := strings.LastIndex(f.Path, "/"); idx >= 0 {
				name = f.Path[idx+1:]
			}
			entries = append(entries, entry{
				Path:     f.Path,
				Name:     name,
				IsDir:    f.IsDir,
				FileSize: f.FileSize,
			})
		}
		if len(entries) == 0 {
			return fmt.Sprintf("No files found at '%s'.", path)
		}
		out, _ := json.MarshalIndent(entries, "", "  ")
		return string(out)
	}

	iter := w.Files.ListDirectoryContents(ctx, files.ListDirectoryContentsRequest{
		DirectoryPath: path,
	})

	type entry struct {
		Path         string `json:"path"`
		Name         string `json:"name"`
		IsDirectory  bool   `json:"is_directory"`
		FileSize     int64  `json:"file_size,omitempty"`
		LastModified int64  `json:"last_modified,omitempty"`
	}
	var entries []entry
	for iter.HasNext(ctx) {
		item, err := iter.Next(ctx)
		if err != nil {
			return fmt.Sprintf("Error listing files: %v", err)
		}
		entries = append(entries, entry{
			Path:         item.Path,
			Name:         item.Name,
			IsDirectory:  item.IsDirectory,
			FileSize:     item.FileSize,
			LastModified: item.LastModified,
		})
	}

	if len(entries) == 0 {
		return fmt.Sprintf("No files found at '%s'.", path)
	}

	out, _ := json.MarshalIndent(entries, "", "  ")
	return string(out)
}

func ReadFile(ctx context.Context, path string, maxBytes int, host, profile, tokenEnvVar *string) string {
	if maxBytes <= 0 {
		maxBytes = maxBytesDefault
	}
	if maxBytes > maxBytesCap {
		maxBytes = maxBytesCap
	}

	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	var data []byte

	if strings.HasPrefix(path, "dbfs:") || strings.HasPrefix(path, "/dbfs") {
		dbfsPath := strings.TrimPrefix(path, "/dbfs")
		resp, err := w.Dbfs.Read(ctx, files.ReadDbfsRequest{Path: dbfsPath, Length: int64(maxBytes + 1)})
		if err != nil {
			return fmt.Sprintf("Error reading file: %v", err)
		}
		data = []byte(resp.Data)
	} else {
		resp, err := w.Files.Download(ctx, files.DownloadRequest{FilePath: path})
		if err != nil {
			return fmt.Sprintf("Error reading file: %v", err)
		}
		defer resp.Contents.Close()
		data, err = io.ReadAll(io.LimitReader(resp.Contents, int64(maxBytes+1)))
		if err != nil {
			return fmt.Sprintf("Error reading file: %v", err)
		}
	}

	truncated := len(data) > maxBytes
	if truncated {
		data = data[:maxBytes]
	}

	text := string(data)
	for _, b := range data {
		if b == 0 {
			result := map[string]any{
				"path":       path,
				"size_bytes": len(data),
				"truncated":  truncated,
				"message":    "Binary file. Content not displayed.",
			}
			out, _ := json.MarshalIndent(result, "", "  ")
			return string(out)
		}
	}

	if truncated {
		return fmt.Sprintf("%s\n\n--- TRUNCATED at %d bytes ---", text, maxBytes)
	}
	return text
}
