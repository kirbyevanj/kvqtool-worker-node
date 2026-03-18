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

const (
	transnetDefaultWidth  = 48
	transnetDefaultHeight = 27
)

// TransnetV2SceneCut runs TransNet V2 ML-based scene detection.
// Accepts frame_width/frame_height params to rescale frames before inference (default 48×27).
// Falls back to ffprobe scene detection if the ONNX model is not available.
func (a *Activities) TransnetV2SceneCut(ctx context.Context, input types.ActivityInput) (*types.ActivityOutput, error) {
	localPath := input.Params["local_path"]
	if localPath == "" {
		localPath = resolveUpstreamLocalPath(input)
	}
	if localPath == "" {
		return fail(input.NodeID, "no local_path"), nil
	}

	threshold, fw, fh := transnetParams(input.Params)

	if a.TransnetModel != "" {
		scf, err := a.runTransnetV2InferenceFile(ctx, localPath, localPath, threshold, fw, fh)
		if err != nil {
			a.Logger.Warn("TransnetV2 inference failed, falling back to ffprobe", "err", err)
		} else {
			activity.RecordHeartbeat(ctx, "transnetv2 detection complete")
			return okJSON(input.NodeID, "", scf), nil
		}
	}

	scf, err := a.runSceneCutDetection(ctx, localPath, map[string]string{
		"threshold": fmt.Sprintf("%.2f", threshold),
	})
	if err != nil {
		return fail(input.NodeID, err.Error()), nil
	}

	activity.RecordHeartbeat(ctx, "scene detection complete (fallback)")
	return okJSON(input.NodeID, "", scf), nil
}

// runTransnetV2InferenceFile extracts frames from a local file and runs ONNX inference.
func (a *Activities) runTransnetV2InferenceFile(ctx context.Context, inputPath, source string, threshold float64, frameW, frameH int) (*types.SceneCutFile, error) {
	workDir := filepath.Join(a.TmpDir, "transnetv2-"+filepath.Base(inputPath))
	os.MkdirAll(workDir, 0o755)
	defer os.RemoveAll(workDir)

	framesPath := filepath.Join(workDir, "frames.raw")
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-i", inputPath,
		"-vf", fmt.Sprintf("scale=%d:%d,fps=25", frameW, frameH),
		"-pix_fmt", "rgb24",
		"-f", "rawvideo",
		"-y", framesPath,
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("frame extraction: %s: %s", err, string(out))
	}
	activity.RecordHeartbeat(ctx, "frames extracted")

	duration, err := probeDuration(ctx, inputPath)
	if err != nil {
		return nil, fmt.Errorf("probe duration: %w", err)
	}

	return a.transnetFromFramesFile(ctx, framesPath, source, threshold, frameW, frameH, duration)
}

// runTransnetV2InferenceStream extracts frames from an io.Reader (streaming S3 download).
// source is used as the SceneCutFile.Source label.
func (a *Activities) runTransnetV2InferenceStream(ctx context.Context, r io.Reader, source string, threshold float64, frameW, frameH int) (*types.SceneCutFile, error) {
	workDir := filepath.Join(a.TmpDir, "transnetv2-stream-"+filepath.Base(source))
	os.MkdirAll(workDir, 0o755)
	defer os.RemoveAll(workDir)

	framesPath := filepath.Join(workDir, "frames.raw")

	// ffmpeg reads from stdin, writes raw frames to framesPath.
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-i", "pipe:0",
		"-vf", fmt.Sprintf("scale=%d:%d,fps=25", frameW, frameH),
		"-pix_fmt", "rgb24",
		"-f", "rawvideo",
		"-y", framesPath,
	)
	cmd.Stdin = r

	// Pipe duration probe concurrently: we need duration but don't have a file.
	// We capture it from ffmpeg stderr (it prints the stream duration on stderr).
	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf

	if out, err := cmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("frame extraction (stream): %s: %s", err, string(out))
	}
	activity.RecordHeartbeat(ctx, "frames extracted (stream)")

	// Parse duration from ffmpeg stderr output.
	duration := parseDurationFromFFmpegStderr(stderrBuf.String())
	if duration <= 0 {
		// Estimate from frame count after reading the file.
		stat, err := os.Stat(framesPath)
		if err == nil {
			frameSize := int64(frameW * frameH * 3)
			totalFrames := stat.Size() / frameSize
			duration = float64(totalFrames) / 25.0
		}
	}

	return a.transnetFromFramesFile(ctx, framesPath, source, threshold, frameW, frameH, duration)
}

// transnetFromFramesFile runs ONNX inference on a raw frames file and returns a SceneCutFile.
func (a *Activities) transnetFromFramesFile(ctx context.Context, framesPath, source string, threshold float64, frameW, frameH int, duration float64) (*types.SceneCutFile, error) {
	stat, err := os.Stat(framesPath)
	if err != nil {
		return nil, fmt.Errorf("frames stat: %w", err)
	}
	frameSize := int64(frameW * frameH * 3)
	totalFrames := int(stat.Size() / frameSize)
	if totalFrames == 0 {
		return nil, fmt.Errorf("no frames in %s", framesPath)
	}

	fps := float64(totalFrames) / duration
	if fps <= 0 {
		fps = 25.0
	}

	predictions, err := runTransnetONNX(ctx, a.TransnetModel, framesPath, totalFrames, frameW, frameH)
	if err != nil {
		return nil, err
	}

	var cutFrames []int
	for i, prob := range predictions {
		if prob >= threshold {
			cutFrames = append(cutFrames, i)
		}
	}

	scf := &types.SceneCutFile{
		Version: "0.1",
		Source:  source,
	}

	prevFrame := 0
	for idx, cutFrame := range cutFrames {
		scf.Segments = append(scf.Segments, types.Segment{
			Index:     idx,
			StartTime: formatTimestamp(float64(prevFrame) / fps),
			EndTime:   formatTimestamp(float64(cutFrame) / fps),
		})
		prevFrame = cutFrame
	}
	if prevFrame < totalFrames {
		scf.Segments = append(scf.Segments, types.Segment{
			Index:     len(scf.Segments),
			StartTime: formatTimestamp(float64(prevFrame) / fps),
			EndTime:   formatTimestamp(duration),
		})
	}

	return scf, nil
}

// runTransnetONNX runs the TransNet V2 ONNX model on raw frame data via Python subprocess.
// frameW and frameH must match the dimensions used during frame extraction.
func runTransnetONNX(ctx context.Context, modelPath, framesPath string, totalFrames, frameW, frameH int) ([]float64, error) {
	script := fmt.Sprintf(`
import numpy as np, onnxruntime as ort, sys, json
data = np.fromfile('%s', dtype=np.uint8).reshape(-1, %d, %d, 3).astype(np.float32) / 255.0
total = data.shape[0]
sess = ort.InferenceSession('%s')
batch = 100
probs = []
for i in range(0, total, batch):
    chunk = data[i:i+batch]
    if chunk.shape[0] < batch:
        pad = np.zeros((batch - chunk.shape[0], %d, %d, 3), dtype=np.float32)
        chunk = np.concatenate([chunk, pad])
    out = sess.run(None, {'input': chunk[np.newaxis]})
    p = out[0][0][:min(batch, total-i), 0]
    probs.extend(p.tolist())
json.dump(probs, sys.stdout)
`, framesPath, frameH, frameW, modelPath, frameH, frameW)

	cmd := exec.CommandContext(ctx, "python3", "-c", script)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("onnx inference: %s: %s", err, strings.TrimSpace(string(out)))
	}

	var probs []float64
	if err := json.Unmarshal(out, &probs); err != nil {
		return nil, fmt.Errorf("parse predictions: %w", err)
	}
	return probs, nil
}

// transnetParams extracts threshold, frame_width, frame_height from params.
func transnetParams(params map[string]string) (threshold float64, fw, fh int) {
	threshold = 0.5
	fw = transnetDefaultWidth
	fh = transnetDefaultHeight

	if t := params["threshold"]; t != "" {
		if v, err := strconv.ParseFloat(t, 64); err == nil {
			threshold = v
		}
	}
	if w := params["frame_width"]; w != "" {
		if v, err := strconv.Atoi(w); err == nil && v > 0 {
			fw = v
		}
	}
	if h := params["frame_height"]; h != "" {
		if v, err := strconv.Atoi(h); err == nil && v > 0 {
			fh = v
		}
	}
	return
}

// parseDurationFromFFmpegStderr extracts "Duration: HH:MM:SS.mm" from ffmpeg stderr.
func parseDurationFromFFmpegStderr(stderr string) float64 {
	// Look for "Duration: HH:MM:SS.ff"
	idx := strings.Index(stderr, "Duration: ")
	if idx < 0 {
		return 0
	}
	rest := stderr[idx+len("Duration: "):]
	// Read until comma or space
	end := strings.IndexAny(rest, ", \n")
	if end < 0 {
		end = len(rest)
	}
	ts := rest[:end]
	parts := strings.Split(ts, ":")
	if len(parts) != 3 {
		return 0
	}
	h, _ := strconv.ParseFloat(parts[0], 64)
	m, _ := strconv.ParseFloat(parts[1], 64)
	s, _ := strconv.ParseFloat(parts[2], 64)
	return h*3600 + m*60 + s
}
