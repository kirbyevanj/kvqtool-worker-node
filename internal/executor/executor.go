package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/go-gst/go-gst/gst"
	"github.com/kirbyevanj/kvqtool-kvq-models/messages"
	"github.com/kirbyevanj/kvqtool-worker-node/internal/compiler"
	"github.com/kirbyevanj/kvqtool-worker-node/internal/s3client"
)

type ProgressFunc func(ctx context.Context, progress messages.JobProgress)

type Executor struct {
	s3      *s3client.Client
	tempDir string
	logger  *slog.Logger
}

func New(s3 *s3client.Client, tempDir string, logger *slog.Logger) *Executor {
	return &Executor{s3: s3, tempDir: tempDir, logger: logger}
}

func (e *Executor) RunEncode(ctx context.Context, msg messages.JobMessage, nodes []compiler.PipelineNode, onProgress ProgressFunc) (string, error) {
	jobDir := filepath.Join(e.tempDir, msg.JobID.String())
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		return "", fmt.Errorf("create job dir: %w", err)
	}
	defer os.RemoveAll(jobDir)

	inputPath, inputKey, err := e.resolveInput(ctx, nodes, jobDir)
	if err != nil {
		return "", err
	}
	_ = inputKey

	outputPath := filepath.Join(jobDir, "output.mp4")
	props := resolveEncodeProps(nodes)

	pipelineStr := buildEncodePipeline(inputPath, outputPath, props)
	e.logger.Info("launching encode pipeline", "pipeline", pipelineStr, "job_id", msg.JobID)

	outputKey := fmt.Sprintf("projects/%s/media/%s-output.mp4", msg.ProjectID, msg.JobID)
	if err := e.runPipeline(ctx, msg, pipelineStr, onProgress); err != nil {
		return "", err
	}

	if err := e.s3.Upload(ctx, outputPath, outputKey, "video/mp4"); err != nil {
		return "", fmt.Errorf("upload output: %w", err)
	}

	return outputKey, nil
}

func (e *Executor) resolveInput(ctx context.Context, nodes []compiler.PipelineNode, jobDir string) (string, string, error) {
	for _, n := range nodes {
		if n.NodeType == "FileSource" {
			s3Key := n.Properties["s3_key"]
			if s3Key == "" {
				s3Key = n.Properties["resource_id"]
			}
			localPath := filepath.Join(jobDir, "input.mp4")
			if err := e.s3.Download(ctx, s3Key, localPath); err != nil {
				return "", "", fmt.Errorf("download input: %w", err)
			}
			return localPath, s3Key, nil
		}
	}
	return "", "", fmt.Errorf("no FileSource node found")
}

func resolveEncodeProps(nodes []compiler.PipelineNode) map[string]string {
	for _, n := range nodes {
		if n.NodeType == "x264Encode" {
			return n.Properties
		}
	}
	return map[string]string{"quantizer": "23", "speed-preset": "medium"}
}

func buildEncodePipeline(inputPath, outputPath string, props map[string]string) string {
	pass := "quant"
	encProps := ""

	if _, ok := props["bitrate_kbps"]; ok {
		pass = "cbr"
		encProps += " bitrate=" + props["bitrate_kbps"]
	}
	if crf, ok := props["crf"]; ok {
		encProps += " quantizer=" + crf
	}
	if preset, ok := props["preset"]; ok {
		encProps += " speed-preset=" + preset
	} else {
		encProps += " speed-preset=medium"
	}
	if gop, ok := props["gop_length"]; ok {
		encProps += " key-int-max=" + gop
	}

	return fmt.Sprintf(
		"filesrc location=%s ! decodebin ! videoconvert ! x264enc pass=%s%s ! video/x-h264,profile=high ! mp4mux ! filesink location=%s",
		inputPath, pass, encProps, outputPath,
	)
}

func (e *Executor) runPipeline(ctx context.Context, msg messages.JobMessage, pipelineStr string, onProgress ProgressFunc) error {
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

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			pipeline.SetState(gst.StateNull)
			return ctx.Err()
		case <-ticker.C:
			e.emitProgress(pipeline, msg, onProgress, ctx)
		default:
		}

		gstMsg := bus.TimedPop(gst.ClockTime(100 * time.Millisecond))
		if gstMsg == nil {
			continue
		}

		switch gstMsg.Type() {
		case gst.MessageEOS:
			gstMsg.Unref()
			pipeline.SetState(gst.StateNull)
			return nil
		case gst.MessageError:
			gerr := gstMsg.ParseError()
			gstMsg.Unref()
			pipeline.SetState(gst.StateNull)
			return fmt.Errorf("gstreamer error: %s", gerr.Error())
		case gst.MessageElement:
			e.handleElementMessage(gstMsg, msg)
			gstMsg.Unref()
		default:
			gstMsg.Unref()
		}
	}
}

func (e *Executor) emitProgress(pipeline *gst.Pipeline, msg messages.JobMessage, onProgress ProgressFunc, ctx context.Context) {
	ok, pos := pipeline.QueryPosition(gst.FormatTime)
	if !ok {
		return
	}
	ok2, dur := pipeline.QueryDuration(gst.FormatTime)
	if !ok2 || dur <= 0 {
		return
	}

	pct := int32(float64(pos) / float64(dur) * 100)
	onProgress(ctx, messages.JobProgress{
		JobID:       msg.JobID,
		Status:      "running",
		ProgressPct: pct,
	})
}

func (e *Executor) handleElementMessage(gstMsg *gst.Message, msg messages.JobMessage) {
	structure := gstMsg.GetStructure()
	if structure == nil {
		return
	}
	e.logger.Debug("element message", "name", structure.Name(), "job_id", msg.JobID)
}

// CollectMetrics runs a metric analysis pipeline and returns per-frame results.
func (e *Executor) CollectMetrics(ctx context.Context, msg messages.JobMessage, refPath, distPath string, metrics []string, onProgress ProgressFunc) (map[string]map[string]map[string]float64, error) {
	results := make(map[string]map[string]map[string]float64)
	for _, m := range metrics {
		results[m] = make(map[string]map[string]float64)
		results[m]["0"] = make(map[string]float64)
	}

	pipelineStr := buildMetricPipeline(refPath, distPath)
	e.logger.Info("launching metrics pipeline", "pipeline", pipelineStr, "job_id", msg.JobID)

	pipeline, err := gst.NewPipelineFromString(pipelineStr)
	if err != nil {
		return nil, fmt.Errorf("create metrics pipeline: %w", err)
	}
	defer pipeline.Unref()

	bus := pipeline.GetBus()
	defer bus.Unref()

	if err := pipeline.SetState(gst.StatePlaying); err != nil {
		return nil, fmt.Errorf("set playing: %w", err)
	}

	frameNum := 0
	for {
		select {
		case <-ctx.Done():
			pipeline.SetState(gst.StateNull)
			return nil, ctx.Err()
		default:
		}

		gstMsg := bus.TimedPop(gst.ClockTime(100 * time.Millisecond))
		if gstMsg == nil {
			continue
		}

		switch gstMsg.Type() {
		case gst.MessageEOS:
			gstMsg.Unref()
			pipeline.SetState(gst.StateNull)
			return results, nil
		case gst.MessageError:
			gerr := gstMsg.ParseError()
			gstMsg.Unref()
			pipeline.SetState(gst.StateNull)
			return nil, fmt.Errorf("metrics pipeline error: %s", gerr.Error())
		case gst.MessageElement:
			structure := gstMsg.GetStructure()
			if structure != nil {
				frameKey := strconv.Itoa(frameNum)
				for _, m := range metrics {
					val, err := structure.GetValue(m)
					if err == nil {
						if f, ok := val.(float64); ok {
							results[m]["0"][frameKey] = f
						}
					}
				}
				frameNum++
			}
			gstMsg.Unref()
		default:
			gstMsg.Unref()
		}
	}
}

func buildMetricPipeline(refPath, distPath string) string {
	return fmt.Sprintf(
		"filesrc location=%s ! decodebin ! videoconvert ! video/x-raw,format=I420 ! iqa.sink_0 "+
			"filesrc location=%s ! decodebin ! videoconvert ! video/x-raw,format=I420 ! iqa.sink_1 "+
			"iqa name=iqa do-dssim=true ! fakesink",
		refPath, distPath,
	)
}

// BuildReportJSON assembles the metric results into the standard report format.
func BuildReportJSON(refKey, distKey string, metrics []string, results map[string]map[string]map[string]float64) ([]byte, error) {
	report := map[string]any{
		"header": map[string]any{
			"version":   "0.1",
			"type":      "full_reference",
			"software":  "kvqtool-web",
			"metrics":   metrics,
			"reference": refKey,
			"dist":      map[string]string{"0": distKey},
		},
	}
	for _, m := range metrics {
		report[m] = results[m]
	}
	return json.MarshalIndent(report, "", "  ")
}
