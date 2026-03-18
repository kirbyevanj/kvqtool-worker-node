package activities

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/kirbyevanj/kvqtool-kvq-models/types"
	"github.com/kirbyevanj/kvqtool-worker-node/internal/s3client"
)

// Activities holds shared dependencies injected into all activity methods.
type Activities struct {
	S3              *s3client.Client
	TmpDir          string
	ApiURL          string
	TransnetModel   string // path to TransNet V2 ONNX model
	Logger          *slog.Logger
}

func (a *Activities) registerResource(ctx context.Context, projectID, name, s3Key, contentType string) {
	if a.ApiURL == "" {
		return
	}
	body, _ := json.Marshal(map[string]string{
		"filename":     name,
		"content_type": contentType,
		"s3_key":       s3Key,
	})
	url := fmt.Sprintf("%s/v1/projects/%s/resources/register", a.ApiURL, projectID)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		a.Logger.Warn("register resource request failed", "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		a.Logger.Warn("register resource failed", "err", err)
		return
	}
	resp.Body.Close()
	a.Logger.Info("resource registered", "name", name, "s3_key", s3Key)
}

// FetchWorkflowDAG retrieves a saved workflow's DAG JSON from the API server.
func (a *Activities) FetchWorkflowDAG(ctx context.Context, input types.ActivityInput) (*types.ActivityOutput, error) {
	workflowID := input.Params["workflow_id"]
	projectID := input.Params["project_id"]
	if workflowID == "" || projectID == "" {
		return fail(input.NodeID, "workflow_id and project_id required"), nil
	}

	url := fmt.Sprintf("%s/v1/projects/%s/workflows/%s", a.ApiURL, projectID, workflowID)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fail(input.NodeID, fmt.Sprintf("build request: %s", err)), nil
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fail(input.NodeID, fmt.Sprintf("fetch workflow: %s", err)), nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fail(input.NodeID, fmt.Sprintf("fetch workflow: status %d", resp.StatusCode)), nil
	}

	var wfDef struct {
		DAGJson json.RawMessage `json:"dag_json"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&wfDef); err != nil {
		return fail(input.NodeID, fmt.Sprintf("decode workflow: %s", err)), nil
	}

	return &types.ActivityOutput{
		NodeID:  input.NodeID,
		Success: true,
		Data:    wfDef.DAGJson,
	}, nil
}

// --- Shared helpers ---

func runGstPipeline(ctx context.Context, pipelineStr string) error {
	args := append([]string{"-e"}, strings.Fields(pipelineStr)...)
	cmd := exec.CommandContext(ctx, "gst-launch-1.0", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("gst-launch: %s: %s", err, string(out))
	}
	return nil
}

func resolveUpstreamS3Key(input types.ActivityInput) string {
	for _, raw := range input.UpstreamResults {
		var out types.ActivityOutput
		if json.Unmarshal(raw, &out) == nil && out.S3Key != "" {
			return out.S3Key
		}
	}
	return ""
}

func resolveUpstreamLocalPath(input types.ActivityInput) string {
	for _, raw := range input.UpstreamResults {
		var out types.ActivityOutput
		if json.Unmarshal(raw, &out) == nil && out.Data != nil {
			var d map[string]string
			if json.Unmarshal(out.Data, &d) == nil {
				if p, ok := d["local_path"]; ok {
					return p
				}
			}
		}
	}
	return ""
}

func resolveUpstreamParam(input types.ActivityInput, key string) string {
	for _, raw := range input.UpstreamResults {
		var out types.ActivityOutput
		if json.Unmarshal(raw, &out) == nil && out.Data != nil {
			var d map[string]string
			if json.Unmarshal(out.Data, &d) == nil {
				if v, ok := d[key]; ok {
					return v
				}
			}
		}
	}
	return ""
}

// resolveUpstreamSceneCutFile extracts a SceneCutFile from upstream results.
func resolveUpstreamSceneCutFile(input types.ActivityInput) (*types.SceneCutFile, bool) {
	for _, raw := range input.UpstreamResults {
		var out types.ActivityOutput
		if json.Unmarshal(raw, &out) != nil || out.Data == nil {
			continue
		}
		var scf types.SceneCutFile
		if json.Unmarshal(out.Data, &scf) == nil && len(scf.Segments) > 0 {
			return &scf, true
		}
	}
	return nil, false
}

// ffmpegTrim cuts inputPath between start/end timestamps into outputPath using stream-copy.
// start and/or end may be empty strings (treated as "from beginning" / "to end").
func ffmpegTrim(ctx context.Context, inputPath, outputPath, startTime, endTime string) error {
	args := []string{"-y", "-i", inputPath}
	if startTime != "" {
		args = append(args, "-ss", startTime)
	}
	if endTime != "" {
		args = append(args, "-to", endTime)
	}
	args = append(args, "-c", "copy", outputPath)
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ffmpeg trim: %s: %s", err, string(out))
	}
	return nil
}

// trimmedInput returns a trimmed copy of inputPath if start_time/end_time params are set,
// writing it into workDir/trimmed.mp4. Returns the (possibly original) path and cleanup func.
func trimmedInput(ctx context.Context, inputPath, workDir string, params map[string]string) (string, func(), error) {
	start := params["start_time"]
	end := params["end_time"]
	if start == "" && end == "" {
		return inputPath, func() {}, nil
	}
	trimPath := filepath.Join(workDir, "trimmed.mp4")
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return "", nil, err
	}
	if err := ffmpegTrim(ctx, inputPath, trimPath, start, end); err != nil {
		return "", nil, err
	}
	return trimPath, func() { os.Remove(trimPath) }, nil
}

func ok(nodeID, s3Key string, data map[string]string) *types.ActivityOutput {
	var rawData json.RawMessage
	if data != nil {
		rawData, _ = json.Marshal(data)
	}
	return &types.ActivityOutput{NodeID: nodeID, Success: true, S3Key: s3Key, Data: rawData}
}

func okJSON(nodeID, s3Key string, data any) *types.ActivityOutput {
	rawData, _ := json.Marshal(data)
	return &types.ActivityOutput{NodeID: nodeID, Success: true, S3Key: s3Key, Data: rawData}
}

func fail(nodeID, msg string) *types.ActivityOutput {
	return &types.ActivityOutput{NodeID: nodeID, Success: false, Error: msg}
}
