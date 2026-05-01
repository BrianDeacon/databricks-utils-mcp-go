package clusters

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/databricks/databricks-sdk-go/service/compute"

	"github.com/BrianDeacon/databricks-utils-mcp-go/internal/client"
)

func ListClusters(ctx context.Context, clusterSources, clusterStates *string, isPinned *bool, pageSize int, pageToken *string, host, profile, tokenEnvVar *string) string {
	if pageSize <= 0 {
		pageSize = 20
	}

	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	req := compute.ListClustersRequest{}
	if pageToken != nil && *pageToken != "" {
		req.PageToken = *pageToken
	}
	if clusterSources != nil && *clusterSources != "" {
		req.FilterBy = &compute.ListClustersFilterBy{
			ClusterSources: []compute.ClusterSource{compute.ClusterSource(*clusterSources)},
		}
	}

	all, err := w.Clusters.ListAll(ctx, req)
	if err != nil {
		return fmt.Sprintf("Error listing clusters: %v", err)
	}

	type clusterInfo struct {
		ClusterID   string `json:"cluster_id"`
		ClusterName string `json:"cluster_name"`
		State       string `json:"state"`
		Creator     string `json:"creator,omitempty"`
	}
	var result []clusterInfo
	for _, c := range all {
		result = append(result, clusterInfo{
			ClusterID:   c.ClusterId,
			ClusterName: c.ClusterName,
			State:       string(c.State),
			Creator:     c.CreatorUserName,
		})
		if len(result) >= pageSize {
			break
		}
	}

	if len(result) == 0 {
		return "No clusters found."
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ClusterName < result[j].ClusterName })
	out, _ := json.MarshalIndent(result, "", "  ")
	return string(out)
}

func GetCluster(ctx context.Context, clusterID string, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	c, err := w.Clusters.Get(ctx, compute.GetClusterRequest{ClusterId: clusterID})
	if err != nil {
		return fmt.Sprintf("Error getting cluster: %v", err)
	}

	result := map[string]any{
		"cluster_id":         c.ClusterId,
		"cluster_name":       c.ClusterName,
		"state":              string(c.State),
		"state_message":      c.StateMessage,
		"spark_version":      c.SparkVersion,
		"node_type_id":       c.NodeTypeId,
		"driver_node_type":   c.DriverNodeTypeId,
		"num_workers":        c.NumWorkers,
		"creator_user_name":  c.CreatorUserName,
		"start_time":         c.StartTime,
		"terminated_time":    c.TerminatedTime,
		"last_restarted_time": c.LastRestartedTime,
	}
	if c.Autoscale != nil {
		result["autoscale"] = map[string]any{
			"min_workers": c.Autoscale.MinWorkers,
			"max_workers": c.Autoscale.MaxWorkers,
		}
	}
	if c.TerminationReason != nil {
		result["termination_reason"] = map[string]any{
			"code":    string(c.TerminationReason.Code),
			"type":    string(c.TerminationReason.Type),
			"message": c.TerminationReason.Parameters,
		}
	}

	out, _ := json.MarshalIndent(result, "", "  ")
	return string(out)
}

func GetClusterEvents(ctx context.Context, clusterID string, maxResults int, host, profile, tokenEnvVar *string) string {
	if maxResults <= 0 {
		maxResults = 25
	}

	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	all, err := w.Clusters.EventsAll(ctx, compute.GetEvents{
		ClusterId: clusterID,
	})
	if err != nil {
		return fmt.Sprintf("Error getting cluster events: %v", err)
	}

	type eventInfo struct {
		Type      string `json:"type"`
		Timestamp int64  `json:"timestamp"`
		Details   any    `json:"details,omitempty"`
	}
	var events []eventInfo
	for _, e := range all {
		info := eventInfo{
			Type:      string(e.Type),
			Timestamp: e.Timestamp,
		}
		if e.Details != nil {
			details := map[string]any{}
			if e.Details.Reason != nil {
				details["reason"] = map[string]any{
					"code":    string(e.Details.Reason.Code),
					"type":    string(e.Details.Reason.Type),
					"message": e.Details.Reason.Parameters,
				}
			}
			if e.Details.CurrentNumWorkers != 0 {
				details["current_num_workers"] = e.Details.CurrentNumWorkers
			}
			if e.Details.TargetNumWorkers != 0 {
				details["target_num_workers"] = e.Details.TargetNumWorkers
			}
			if len(details) > 0 {
				info.Details = details
			}
		}
		events = append(events, info)
		if len(events) >= maxResults {
			break
		}
	}

	if len(events) == 0 {
		return fmt.Sprintf("No events found for cluster '%s'.", clusterID)
	}

	out, _ := json.MarshalIndent(events, "", "  ")
	return string(out)
}

func StartCluster(ctx context.Context, clusterID string, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	_, err = w.Clusters.Start(ctx, compute.StartCluster{ClusterId: clusterID})
	if err != nil {
		return fmt.Sprintf("Error starting cluster: %v", err)
	}

	return fmt.Sprintf("Start requested for cluster '%s'. It may take a few minutes to become available.", clusterID)
}

func TerminateCluster(ctx context.Context, clusterID string, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	_, err = w.Clusters.Delete(ctx, compute.DeleteCluster{ClusterId: clusterID})
	if err != nil {
		return fmt.Sprintf("Error terminating cluster: %v", err)
	}

	return fmt.Sprintf("Terminate requested for cluster '%s'.", clusterID)
}
