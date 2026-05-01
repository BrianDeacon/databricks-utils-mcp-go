package pipelines

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/databricks/databricks-sdk-go/service/pipelines"

	"github.com/BrianDeacon/databricks-utils-mcp-go/internal/client"
)

func ListPipelines(ctx context.Context, name *string, maxResults int, host, profile, tokenEnvVar *string) string {
	if maxResults <= 0 {
		maxResults = 25
	}

	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	var filterStr string
	if name != nil && *name != "" {
		filterStr = fmt.Sprintf("name LIKE '%%%s%%'", *name)
	}

	all, err := w.Pipelines.ListPipelinesAll(ctx, pipelines.ListPipelinesRequest{
		Filter:     filterStr,
		MaxResults: maxResults,
	})
	if err != nil {
		return fmt.Sprintf("Error listing pipelines: %v", err)
	}

	type pipelineInfo struct {
		PipelineID  string `json:"pipeline_id"`
		Name        string `json:"name"`
		State       string `json:"state,omitempty"`
		CreatorName string `json:"creator_user_name,omitempty"`
	}
	var result []pipelineInfo
	for _, p := range all {
		result = append(result, pipelineInfo{
			PipelineID:  p.PipelineId,
			Name:        p.Name,
			State:       string(p.State),
			CreatorName: p.CreatorUserName,
		})
		if len(result) >= maxResults {
			break
		}
	}

	if len(result) == 0 {
		return "No pipelines found."
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	out, _ := json.MarshalIndent(result, "", "  ")
	return string(out)
}

func GetPipeline(ctx context.Context, pipelineID string, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	p, err := w.Pipelines.Get(ctx, pipelines.GetPipelineRequest{PipelineId: pipelineID})
	if err != nil {
		return fmt.Sprintf("Error getting pipeline: %v", err)
	}

	result := map[string]any{
		"pipeline_id":        p.PipelineId,
		"name":               p.Name,
		"state":              string(p.State),
		"creator_user_name":  p.CreatorUserName,
	}

	if p.Spec != nil {
		result["catalog"] = p.Spec.Catalog
		result["target"] = p.Spec.Target
		result["continuous"] = p.Spec.Continuous

		var libs []map[string]any
		for _, l := range p.Spec.Libraries {
			lib := map[string]any{}
			if l.Notebook != nil {
				lib["notebook"] = map[string]string{"path": l.Notebook.Path}
			}
			libs = append(libs, lib)
		}
		result["libraries"] = libs

		var cls []map[string]any
		for _, c := range p.Spec.Clusters {
			cls = append(cls, map[string]any{
				"label":        c.Label,
				"num_workers":  c.NumWorkers,
				"node_type_id": c.NodeTypeId,
			})
		}
		result["clusters"] = cls
	}

	out, _ := json.MarshalIndent(result, "", "  ")
	return string(out)
}

func ListPipelineEvents(ctx context.Context, pipelineID string, maxResults int, host, profile, tokenEnvVar *string) string {
	if maxResults <= 0 {
		maxResults = 25
	}

	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	all, err := w.Pipelines.ListPipelineEventsAll(ctx, pipelines.ListPipelineEventsRequest{
		PipelineId: pipelineID,
		MaxResults: maxResults,
	})
	if err != nil {
		return fmt.Sprintf("Error listing pipeline events: %v", err)
	}

	type errorInfo struct {
		Fatal      bool     `json:"fatal,omitempty"`
		Exceptions []string `json:"exceptions,omitempty"`
	}
	type eventInfo struct {
		ID        string     `json:"id"`
		EventType string     `json:"event_type"`
		Level     string     `json:"level,omitempty"`
		Message   string     `json:"message,omitempty"`
		Timestamp string     `json:"timestamp,omitempty"`
		Error     *errorInfo `json:"error,omitempty"`
	}
	var events []eventInfo
	for _, e := range all {
		info := eventInfo{
			ID:        e.Id,
			EventType: e.EventType,
			Level:     string(e.Level),
			Message:   e.Message,
			Timestamp: e.Timestamp,
		}
		if e.Error != nil {
			ei := &errorInfo{Fatal: e.Error.Fatal}
			for _, ex := range e.Error.Exceptions {
				ei.Exceptions = append(ei.Exceptions, ex.Message)
			}
			info.Error = ei
		}
		events = append(events, info)
		if len(events) >= maxResults {
			break
		}
	}

	if len(events) == 0 {
		return fmt.Sprintf("No events found for pipeline '%s'.", pipelineID)
	}

	out, _ := json.MarshalIndent(events, "", "  ")
	return string(out)
}

func StartPipeline(ctx context.Context, pipelineID string, fullRefresh bool, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	resp, err := w.Pipelines.StartUpdate(ctx, pipelines.StartUpdate{
		PipelineId:  pipelineID,
		FullRefresh: fullRefresh,
	})
	if err != nil {
		return fmt.Sprintf("Error starting pipeline: %v", err)
	}

	result := map[string]any{
		"update_id": resp.UpdateId,
		"message":   fmt.Sprintf("Pipeline '%s' update started. Use pipelines_list_events to monitor progress.", pipelineID),
	}
	out, _ := json.MarshalIndent(result, "", "  ")
	return string(out)
}

func StopPipeline(ctx context.Context, pipelineID string, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	_, err = w.Pipelines.Stop(ctx, pipelines.StopRequest{PipelineId: pipelineID})
	if err != nil {
		return fmt.Sprintf("Error stopping pipeline: %v", err)
	}

	return fmt.Sprintf("Stop requested for pipeline '%s'.", pipelineID)
}
