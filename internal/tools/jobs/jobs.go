package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/databricks/databricks-sdk-go/service/jobs"

	"github.com/BrianDeacon/databricks-utils-mcp-go/internal/client"
)

func ListJobs(ctx context.Context, name *string, maxResults int, host, profile, tokenEnvVar *string) string {
	if maxResults <= 0 {
		maxResults = 25
	}

	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	req := jobs.ListJobsRequest{Limit: maxResults}
	if name != nil && *name != "" {
		req.Name = *name
	}

	all, err := w.Jobs.ListAll(ctx, req)
	if err != nil {
		return fmt.Sprintf("Error listing jobs: %v", err)
	}

	type jobInfo struct {
		JobID       int64  `json:"job_id"`
		Name        string `json:"name"`
		CreatorName string `json:"creator_user_name,omitempty"`
	}
	var result []jobInfo
	for _, j := range all {
		info := jobInfo{JobID: j.JobId}
		if j.Settings != nil {
			info.Name = j.Settings.Name
		}
		info.CreatorName = j.CreatorUserName
		result = append(result, info)
		if len(result) >= maxResults {
			break
		}
	}

	if len(result) == 0 {
		return "No jobs found."
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	out, _ := json.MarshalIndent(result, "", "  ")
	return string(out)
}

func GetJob(ctx context.Context, jobID int64, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	j, err := w.Jobs.Get(ctx, jobs.GetJobRequest{JobId: jobID})
	if err != nil {
		return fmt.Sprintf("Error getting job: %v", err)
	}

	type taskInfo struct {
		TaskKey         string `json:"task_key"`
		ExistingCluster string `json:"existing_cluster_id,omitempty"`
	}
	var taskList []taskInfo
	if j.Settings != nil {
		for _, t := range j.Settings.Tasks {
			taskList = append(taskList, taskInfo{
				TaskKey:         t.TaskKey,
				ExistingCluster: t.ExistingClusterId,
			})
		}
	}

	result := map[string]any{
		"job_id":             j.JobId,
		"creator_user_name":  j.CreatorUserName,
	}
	if j.Settings != nil {
		result["name"] = j.Settings.Name
		result["tasks"] = taskList
		if j.Settings.Schedule != nil {
			result["schedule"] = map[string]any{
				"quartz_cron_expression": j.Settings.Schedule.QuartzCronExpression,
				"timezone_id":           j.Settings.Schedule.TimezoneId,
				"pause_status":          string(j.Settings.Schedule.PauseStatus),
			}
		}
		result["tags"] = j.Settings.Tags
	}

	out, _ := json.MarshalIndent(result, "", "  ")
	return string(out)
}

func ListJobRuns(ctx context.Context, jobID *int64, activeOnly bool, maxResults int, host, profile, tokenEnvVar *string) string {
	if maxResults <= 0 {
		maxResults = 25
	}

	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	req := jobs.ListRunsRequest{Limit: maxResults}
	if jobID != nil {
		req.JobId = *jobID
	}
	if activeOnly {
		req.ActiveOnly = true
	}

	all, err := w.Jobs.ListRunsAll(ctx, req)
	if err != nil {
		return fmt.Sprintf("Error listing job runs: %v", err)
	}

	type runInfo struct {
		RunID     int64  `json:"run_id"`
		State     string `json:"state,omitempty"`
		StartTime int64  `json:"start_time,omitempty"`
		EndTime   int64  `json:"end_time,omitempty"`
		Duration  int64  `json:"duration_ms,omitempty"`
		Trigger   string `json:"trigger,omitempty"`
	}
	var result []runInfo
	for _, r := range all {
		info := runInfo{
			RunID:     r.RunId,
			StartTime: r.StartTime,
			EndTime:   r.EndTime,
			Trigger:   string(r.Trigger),
		}
		if r.StartTime > 0 && r.EndTime > 0 {
			info.Duration = r.EndTime - r.StartTime
		}
		if r.State != nil {
			info.State = string(r.State.LifeCycleState)
		}
		result = append(result, info)
		if len(result) >= maxResults {
			break
		}
	}

	if len(result) == 0 {
		return "No runs found."
	}

	out, _ := json.MarshalIndent(result, "", "  ")
	return string(out)
}

func GetJobRun(ctx context.Context, runID int64, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	r, err := w.Jobs.GetRun(ctx, jobs.GetRunRequest{RunId: runID})
	if err != nil {
		return fmt.Sprintf("Error getting run: %v", err)
	}

	type taskState struct {
		TaskKey string `json:"task_key"`
		State   string `json:"state,omitempty"`
	}
	var tasks []taskState
	for _, t := range r.Tasks {
		s := ""
		if t.State != nil {
			s = string(t.State.LifeCycleState)
		}
		tasks = append(tasks, taskState{TaskKey: t.TaskKey, State: s})
	}

	result := map[string]any{
		"run_id":     r.RunId,
		"start_time": r.StartTime,
		"end_time":   r.EndTime,
		"tasks":      tasks,
	}
	if r.State != nil {
		result["state"] = string(r.State.LifeCycleState)
		result["state_message"] = r.State.StateMessage
	}

	out, _ := json.MarshalIndent(result, "", "  ")
	return string(out)
}

func GetRunOutput(ctx context.Context, runID int64, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	r, err := w.Jobs.GetRunOutput(ctx, jobs.GetRunOutputRequest{RunId: runID})
	if err != nil {
		return fmt.Sprintf("Error getting run output: %v", err)
	}

	result := map[string]any{
		"run_id": runID,
	}
	if r.NotebookOutput != nil {
		result["notebook_output"] = r.NotebookOutput.Result
		result["truncated"] = r.NotebookOutput.Truncated
	}
	if r.Error != "" {
		result["error"] = r.Error
	}
	if r.ErrorTrace != "" {
		result["error_trace"] = r.ErrorTrace
	}
	if r.Logs != "" {
		result["logs"] = r.Logs
	}
	if r.LogsTruncated {
		result["logs_truncated"] = true
	}

	out, _ := json.MarshalIndent(result, "", "  ")
	return string(out)
}

func RunJob(ctx context.Context, jobID int64, parameters map[string]string, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	req := jobs.RunNow{JobId: jobID}
	if len(parameters) > 0 {
		var params []jobs.RunJobTask
		for k, v := range parameters {
			params = append(params, jobs.RunJobTask{})
			_ = k
			_ = v
		}
		// Use job_parameters for parameterized jobs
		req.JobParameters = parameters
	}

	resp, err := w.Jobs.RunNow(ctx, req)
	if err != nil {
		return fmt.Sprintf("Error triggering job: %v", err)
	}

	result := map[string]any{
		"run_id":  resp.RunId,
		"message": fmt.Sprintf("Job %d triggered. Use jobs_get_run with run_id %d to track progress.", jobID, resp.RunId),
	}
	out, _ := json.MarshalIndent(result, "", "  ")
	return string(out)
}

func CancelRun(ctx context.Context, runID int64, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	_, err = w.Jobs.CancelRun(ctx, jobs.CancelRun{RunId: runID})
	if err != nil {
		return fmt.Sprintf("Error cancelling run: %v", err)
	}

	return fmt.Sprintf("Cancel requested for run %d.", runID)
}

func RepairRun(ctx context.Context, runID int64, rerunTasks []string, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	req := jobs.RepairRun{RunId: runID}
	if len(rerunTasks) > 0 {
		req.RerunTasks = rerunTasks
	} else {
		req.RerunAllFailedTasks = true
	}

	waiter, err := w.Jobs.RepairRun(ctx, req)
	if err != nil {
		return fmt.Sprintf("Error repairing run: %v", err)
	}

	result := map[string]any{
		"repair_id": waiter.Response.RepairId,
		"message":   fmt.Sprintf("Repair started for run %d.", runID),
	}
	out, _ := json.MarshalIndent(result, "", "  ")
	return string(out)
}
