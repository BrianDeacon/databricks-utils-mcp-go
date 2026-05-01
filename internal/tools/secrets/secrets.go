package secrets

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/databricks/databricks-sdk-go/service/workspace"

	"github.com/BrianDeacon/databricks-utils-mcp-go/internal/client"
)

func ListSecretScopes(ctx context.Context, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	all, err := w.Secrets.ListScopesAll(ctx)
	if err != nil {
		return fmt.Sprintf("Error listing secret scopes: %v", err)
	}

	var names []string
	for _, s := range all {
		if s.Name != "" {
			names = append(names, s.Name)
		}
	}

	if len(names) == 0 {
		return "No secret scopes found."
	}

	sort.Strings(names)
	out, _ := json.MarshalIndent(names, "", "  ")
	return string(out)
}

func ListSecrets(ctx context.Context, scope string, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	all, err := w.Secrets.ListSecretsAll(ctx, workspace.ListSecretsRequest{Scope: scope})
	if err != nil {
		return fmt.Sprintf("Error listing secrets: %v", err)
	}

	var keys []string
	for _, s := range all {
		if s.Key != "" {
			keys = append(keys, s.Key)
		}
	}

	if len(keys) == 0 {
		return fmt.Sprintf("No secrets found in scope '%s'.", scope)
	}

	sort.Strings(keys)
	out, _ := json.MarshalIndent(keys, "", "  ")
	return string(out)
}
