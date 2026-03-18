package activities

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/kirbyevanj/kvqtool-kvq-models/types"
	"go.temporal.io/sdk/activity"
)

// SceneCut detects scene changes in a local file using GStreamer videoanalyse.
func (a *Activities) SceneCut(ctx context.Context, input types.ActivityInput) (*types.ActivityOutput, error) {
	localPath := input.Params["local_path"]
	if localPath == "" {
		localPath = resolveUpstreamLocalPath(input)
	}
	if localPath == "" {
		return fail(input.NodeID, "no local_path"), nil
	}

	scf, err := a.runSceneCutDetection(ctx, localPath, input.Params)
	if err != nil {
		return fail(input.NodeID, err.Error()), nil
	}

	activity.RecordHeartbeat(ctx, "scene detection complete")
	return okJSON(input.NodeID, "", scf), nil
}

// RemoteSceneCut streams from S3 → TransnetV2 frame extraction (no MP4 temp file).
// Falls back to materializing the file for ffprobe scene detection when no ONNX model is configured.
func (a *Activities) RemoteSceneCut(ctx context.Context, input types.ActivityInput) (*types.ActivityOutput, error) {
	s3Key := input.Params["s3_key"]
	if s3Key == "" {
		s3Key = resolveUpstreamS3Key(input)
	}
	if s3Key == "" {
		return fail(input.NodeID, "no s3_key"), nil
	}

	threshold, fw, fh := transnetParams(input.Params)

	var scf *types.SceneCutFile

	if a.TransnetModel != "" {
		// Streaming path: S3 GetObject → ffmpeg stdin for frame extraction.
		r, err := a.S3.GetReader(ctx, s3Key)
		if err != nil {
			return fail(input.NodeID, fmt.Sprintf("s3 open: %s", err)), nil
		}
		scf, err = a.runTransnetV2InferenceStream(ctx, r, s3Key, threshold, fw, fh)
		r.Close()
		if err != nil {
			a.Logger.Warn("RemoteSceneCut: TransnetV2 stream failed, falling back to ffprobe download", "err", err)
			scf = nil
		}
	}

	if scf == nil {
		// ffprobe fallback: needs a seekable file.
		workDir := filepath.Join(a.TmpDir, input.NodeID)
		os.MkdirAll(workDir, 0o755)
		defer os.RemoveAll(workDir)

		localPath := filepath.Join(workDir, "input.mp4")
		if err := a.S3.Download(ctx, s3Key, localPath); err != nil {
			return fail(input.NodeID, fmt.Sprintf("download: %s", err)), nil
		}
		activity.RecordHeartbeat(ctx, "downloaded (fallback)")

		var err error
		scf, err = a.runSceneCutDetection(ctx, localPath, map[string]string{
			"threshold": fmt.Sprintf("%.2f", threshold),
		})
		if err != nil {
			return fail(input.NodeID, err.Error()), nil
		}
	}

	scfBytes, _ := json.Marshal(scf)
	uploadKey := fmt.Sprintf("projects/%s/media/%s-scenecuts.json", input.ProjectID, input.NodeID)
	if err := a.S3.UploadStream(ctx, strings.NewReader(string(scfBytes)), uploadKey, "application/json"); err != nil {
		return fail(input.NodeID, fmt.Sprintf("upload: %s", err)), nil
	}

	a.registerResource(ctx, input.ProjectID, "scenecuts.json", uploadKey, "application/json")
	activity.RecordHeartbeat(ctx, "scene detection complete")
	return okJSON(input.NodeID, uploadKey, scf), nil
}

// runSceneCutDetection uses ffprobe scene detection filter to produce a SceneCutFile.
func (a *Activities) runSceneCutDetection(ctx context.Context, inputPath string, params map[string]string) (*types.SceneCutFile, error) {
	threshold := params["threshold"]
	if threshold == "" {
		threshold = "0.3"
	}

	// Use ffprobe with scene detection for reliable structured output.
	cmd := exec.CommandContext(ctx, "ffprobe",
		"-v", "quiet",
		"-show_entries", "frame=pts_time",
		"-of", "csv=p=0",
		"-f", "lavfi",
		fmt.Sprintf("movie=%s,select=gt(scene\\,%s)", inputPath, threshold),
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("ffprobe scene detect: %s: %s", err, string(out))
	}

	duration, err := probeDuration(ctx, inputPath)
	if err != nil {
		return nil, fmt.Errorf("probe duration: %w", err)
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	var cutPoints []float64
	cutPoints = append(cutPoints, 0)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		t, err := strconv.ParseFloat(line, 64)
		if err == nil && t > 0 {
			cutPoints = append(cutPoints, t)
		}
	}
	cutPoints = append(cutPoints, duration)

	scf := &types.SceneCutFile{
		Version: "0.1",
		Source:  inputPath,
	}
	for i := 0; i < len(cutPoints)-1; i++ {
		scf.Segments = append(scf.Segments, types.Segment{
			Index:     i,
			StartTime: formatTimestamp(cutPoints[i]),
			EndTime:   formatTimestamp(cutPoints[i+1]),
		})
	}
	return scf, nil
}

// probeDuration returns the duration in seconds of a local media file.
func probeDuration(ctx context.Context, path string) (float64, error) {
	cmd := exec.CommandContext(ctx, "ffprobe",
		"-v", "quiet",
		"-show_entries", "format=duration",
		"-of", "csv=p=0",
		path,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("ffprobe: %s: %s", err, string(out))
	}
	return strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
}

// probeDurationFromReader probes duration from a streaming io.Reader via ffprobe stdin.
func probeDurationFromReader(ctx context.Context, r io.Reader) (float64, error) {
	cmd := exec.CommandContext(ctx, "ffprobe",
		"-v", "quiet",
		"-show_entries", "format=duration",
		"-of", "csv=p=0",
		"-i", "pipe:0",
	)
	cmd.Stdin = r
	out, err := cmd.CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("ffprobe stdin: %s: %s", err, string(out))
	}
	return strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
}

func formatTimestamp(seconds float64) string {
	h := int(seconds) / 3600
	m := (int(seconds) % 3600) / 60
	s := seconds - float64(h*3600+m*60)
	return fmt.Sprintf("%02d:%02d:%06.3f", h, m, s)
}
