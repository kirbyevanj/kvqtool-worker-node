package activities

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-gst/go-gst/gst"
	"github.com/kirbyevanj/kvqtool-kvq-models/types"
	"github.com/kirbyevanj/kvqtool-worker-node/internal/s3client"
	"go.temporal.io/sdk/activity"
)

type Activities struct {
	S3     *s3client.Client
	TmpDir string
	Logger *slog.Logger
}

func (a *Activities) S3Download(ctx context.Context, input types.ActivityInput) (*types.ActivityOutput, error) {
	s3Key := input.Params["s3_key"]
	if s3Key == "" {
		s3Key = resolveUpstreamS3Key(input)
	}
	if s3Key == "" {
		return fail(input.NodeID, "no s3_key in params or upstream"), nil
	}

	localPath := filepath.Join(a.TmpDir, input.NodeID, "input.mp4")
	if err := a.S3.Download(ctx, s3Key, localPath); err != nil {
		return fail(input.NodeID, err.Error()), nil
	}

	activity.RecordHeartbeat(ctx, "downloaded")
	return ok(input.NodeID, s3Key, map[string]string{"local_path": localPath}), nil
}

func (a *Activities) S3Upload(ctx context.Context, input types.ActivityInput) (*types.ActivityOutput, error) {
	localPath := input.Params["local_path"]
	if localPath == "" {
		localPath = resolveUpstreamLocalPath(input)
	}
	outputName := input.Params["output_name"]
	if outputName == "" {
		outputName = "output.mp4"
	}
	projectID := input.ProjectID
	s3Key := fmt.Sprintf("projects/%s/media/%s-%s", projectID, input.NodeID, outputName)

	contentType := "video/mp4"
	if strings.HasSuffix(outputName, ".json") {
		contentType = "application/json"
	}

	if err := a.S3.Upload(ctx, localPath, s3Key, contentType); err != nil {
		return fail(input.NodeID, err.Error()), nil
	}

	activity.RecordHeartbeat(ctx, "uploaded")
	return ok(input.NodeID, s3Key, nil), nil
}

func (a *Activities) GStreamerEncode(ctx context.Context, input types.ActivityInput) (*types.ActivityOutput, error) {
	localPath := input.Params["local_path"]
	if localPath == "" {
		localPath = resolveUpstreamLocalPath(input)
	}
	if localPath == "" {
		return fail(input.NodeID, "no local_path"), nil
	}

	outputDir := filepath.Join(a.TmpDir, input.NodeID)
	os.MkdirAll(outputDir, 0o755)
	outputPath := filepath.Join(outputDir, "encoded.mp4")

	pipelineStr := buildEncodePipeline(localPath, outputPath, input.Params)
	a.Logger.Info("GStreamer encode", "pipeline", pipelineStr)

	if err := runGstPipeline(ctx, pipelineStr); err != nil {
		return fail(input.NodeID, err.Error()), nil
	}

	activity.RecordHeartbeat(ctx, "encoded")
	return ok(input.NodeID, "", map[string]string{"local_path": outputPath}), nil
}

func (a *Activities) GStreamerMetrics(ctx context.Context, input types.ActivityInput) (*types.ActivityOutput, error) {
	refPath := input.Params["reference_path"]
	distPath := input.Params["distorted_path"]
	if refPath == "" {
		refPath = resolveUpstreamLocalPath(input)
	}

	pipelineStr := fmt.Sprintf(
		"filesrc location=%s ! decodebin ! videoconvert ! video/x-raw,format=I420 ! iqa.sink_0 "+
			"filesrc location=%s ! decodebin ! videoconvert ! video/x-raw,format=I420 ! iqa.sink_1 "+
			"iqa name=iqa do-dssim=true ! fakesink",
		refPath, distPath,
	)

	if err := runGstPipeline(ctx, pipelineStr); err != nil {
		return fail(input.NodeID, err.Error()), nil
	}

	activity.RecordHeartbeat(ctx, "metrics complete")
	return ok(input.NodeID, "", nil), nil
}

func (a *Activities) SplitVideo(ctx context.Context, input types.ActivityInput) (*types.ActivityOutput, error) {
	localPath := input.Params["local_path"]
	if localPath == "" {
		localPath = resolveUpstreamLocalPath(input)
	}
	segDuration := input.Params["segment_duration"]
	if segDuration == "" {
		segDuration = "4"
	}

	outputDir := filepath.Join(a.TmpDir, input.NodeID)
	os.MkdirAll(outputDir, 0o755)
	outputPattern := filepath.Join(outputDir, "segment_%03d.mp4")

	cmd := exec.CommandContext(ctx, "ffmpeg", "-i", localPath, "-c", "copy",
		"-f", "segment", "-segment_time", segDuration,
		"-reset_timestamps", "1", outputPattern)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fail(input.NodeID, fmt.Sprintf("ffmpeg split: %s: %s", err, string(out))), nil
	}

	entries, _ := os.ReadDir(outputDir)
	var segments []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "segment_") {
			segments = append(segments, filepath.Join(outputDir, e.Name()))
		}
	}

	activity.RecordHeartbeat(ctx, fmt.Sprintf("split into %d segments", len(segments)))
	return ok(input.NodeID, "", map[string]string{
		"segment_dir":   outputDir,
		"segment_count": fmt.Sprintf("%d", len(segments)),
	}), nil
}

func (a *Activities) ConcatVideo(ctx context.Context, input types.ActivityInput) (*types.ActivityOutput, error) {
	segDir := input.Params["segment_dir"]
	if segDir == "" {
		segDir = resolveUpstreamParam(input, "segment_dir")
	}

	outputDir := filepath.Join(a.TmpDir, input.NodeID)
	os.MkdirAll(outputDir, 0o755)
	outputPath := filepath.Join(outputDir, "concatenated.mp4")
	listPath := filepath.Join(outputDir, "concat.txt")

	entries, _ := os.ReadDir(segDir)
	f, _ := os.Create(listPath)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "segment_") {
			fmt.Fprintf(f, "file '%s'\n", filepath.Join(segDir, e.Name()))
		}
	}
	f.Close()

	cmd := exec.CommandContext(ctx, "ffmpeg", "-f", "concat", "-safe", "0", "-i", listPath, "-c", "copy", outputPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fail(input.NodeID, fmt.Sprintf("ffmpeg concat: %s: %s", err, string(out))), nil
	}

	activity.RecordHeartbeat(ctx, "concatenated")
	return ok(input.NodeID, "", map[string]string{"local_path": outputPath}), nil
}

func (a *Activities) GenerateReport(ctx context.Context, input types.ActivityInput) (*types.ActivityOutput, error) {
	outputDir := filepath.Join(a.TmpDir, input.NodeID)
	os.MkdirAll(outputDir, 0o755)
	reportPath := filepath.Join(outputDir, "report.json")

	report := fmt.Sprintf(`{"header":{"version":"0.1","type":"workflow_report","software":"kvqtool-web","node_id":"%s"}}`, input.NodeID)
	os.WriteFile(reportPath, []byte(report), 0o644)

	activity.RecordHeartbeat(ctx, "report generated")
	return ok(input.NodeID, "", map[string]string{"local_path": reportPath}), nil
}

func (a *Activities) FragmentedMP4Repackage(ctx context.Context, input types.ActivityInput) (*types.ActivityOutput, error) {
	localPath := input.Params["local_path"]
	if localPath == "" {
		localPath = resolveUpstreamLocalPath(input)
	}

	outputDir := filepath.Join(a.TmpDir, input.NodeID)
	os.MkdirAll(outputDir, 0o755)
	outputPath := filepath.Join(outputDir, "fragmented.mp4")

	cmd := exec.CommandContext(ctx, "ffmpeg", "-i", localPath,
		"-c", "copy", "-movflags", "frag_keyframe+empty_moov+default_base_moof",
		outputPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fail(input.NodeID, fmt.Sprintf("ffmpeg fmp4: %s: %s", err, string(out))), nil
	}

	activity.RecordHeartbeat(ctx, "repackaged")
	return ok(input.NodeID, "", map[string]string{"local_path": outputPath}), nil
}

func buildEncodePipeline(inputPath, outputPath string, params map[string]string) string {
	pass := "quant"
	encProps := ""
	if _, ok := params["bitrate_kbps"]; ok {
		pass = "cbr"
		encProps += " bitrate=" + params["bitrate_kbps"]
	}
	if crf, ok := params["crf"]; ok {
		encProps += " quantizer=" + crf
	}
	preset := params["preset"]
	if preset == "" {
		preset = "medium"
	}
	encProps += " speed-preset=" + preset
	if gop, ok := params["gop_length"]; ok {
		encProps += " key-int-max=" + gop
	}
	return fmt.Sprintf("filesrc location=%s ! decodebin ! videoconvert ! x264enc pass=%s%s ! video/x-h264,profile=high ! mp4mux ! filesink location=%s",
		inputPath, pass, encProps, outputPath)
}

func runGstPipeline(ctx context.Context, pipelineStr string) error {
	pipeline, err := gst.NewPipelineFromString(pipelineStr)
	if err != nil {
		return fmt.Errorf("create pipeline: %w", err)
	}
	defer pipeline.Unref()

	bus := pipeline.GetBus()
	defer bus.Unref()

	if err := pipeline.SetState(gst.StatePlaying); err != nil {
		return fmt.Errorf("set playing: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			pipeline.SetState(gst.StateNull)
			return ctx.Err()
		default:
		}
		msg := bus.TimedPop(gst.ClockTime(500 * time.Millisecond))
		if msg == nil {
			continue
		}
		switch msg.Type() {
		case gst.MessageEOS:
			msg.Unref()
			pipeline.SetState(gst.StateNull)
			return nil
		case gst.MessageError:
			gerr := msg.ParseError()
			msg.Unref()
			pipeline.SetState(gst.StateNull)
			return fmt.Errorf("gst: %s", gerr.Error())
		default:
			msg.Unref()
		}
	}
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
		var data map[string]string
		if json.Unmarshal(raw, &data) == nil {
			if p, ok := data["local_path"]; ok {
				return p
			}
		}
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

func ok(nodeID, s3Key string, data map[string]string) *types.ActivityOutput {
	var rawData json.RawMessage
	if data != nil {
		rawData, _ = json.Marshal(data)
	}
	return &types.ActivityOutput{NodeID: nodeID, Success: true, S3Key: s3Key, Data: rawData}
}

func fail(nodeID, msg string) *types.ActivityOutput {
	return &types.ActivityOutput{NodeID: nodeID, Success: false, Error: msg}
}
