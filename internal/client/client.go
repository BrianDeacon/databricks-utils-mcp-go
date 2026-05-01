package client

import (
	"os"
	"sync"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/config"
)

var (
	mu      sync.Mutex
	clients = make(map[string]*databricks.WorkspaceClient)
)

func GetClient(host, profile, tokenEnvVar *string) (*databricks.WorkspaceClient, error) {
	key := "_default"
	cfg := &config.Config{}

	if profile != nil && *profile != "" {
		key = *profile
		cfg.Profile = *profile
	} else if host != nil && *host != "" {
		key = *host
		cfg.Host = *host
	}

	if tokenEnvVar != nil && *tokenEnvVar != "" {
		if token := os.Getenv(*tokenEnvVar); token != "" {
			cfg.Token = token
		}
	}

	mu.Lock()
	defer mu.Unlock()

	if c, ok := clients[key]; ok {
		return c, nil
	}

	w, err := databricks.NewWorkspaceClient((*databricks.Config)(cfg))
	if err != nil {
		return nil, err
	}

	clients[key] = w
	return w, nil
}
