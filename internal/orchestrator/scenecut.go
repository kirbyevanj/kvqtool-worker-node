package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/kirbyevanj/kvqtool-kvq-models/messages"
	"github.com/kirbyevanj/kvqtool-worker-node/internal/s3client"
)

type SceneCutConfig struct {
	Threshold    float64
	MinSegFrames int64
	EncodeParams EncodeParams
	FPS          float64
	TotalFrames  int64
}

type SceneCutResult struct {
	OutputS3Key string
	ReportS3Key string
	Segments    []Segment
	Predictions map[int64]float64
}

func RunSceneCutAndEncode(
	ctx context.Context,
	msg messages.JobMessage,
	inputPath string,
	predictions map[int64]float64,
	cfg SceneCutConfig,
	s3 *s3client.Client,
	jobDir string,
	logger *slog.Logger,
) (*SceneCutResult, error) {
	segments := predictionsToSegments(predictions, cfg)
	logger.Info("scene cuts detected", "segments", len(segments), "threshold", cfg.Threshold)

	results, err := EncodeSegments(ctx, inputPath, segments, cfg.EncodeParams, jobDir, logger)
	if err != nil {
		return nil, fmt.Errorf("encode segments: %w", err)
	}

	var segPaths []string
	for _, r := range results {
		segPaths = append(segPaths, r.FilePath)
	}

	outputPath := filepath.Join(jobDir, "final_output.mp4")
	if err := ConcatSegments(ctx, segPaths, outputPath, logger); err != nil {
		return nil, fmt.Errorf("concat: %w", err)
	}

	outputKey := fmt.Sprintf("projects/%s/media/%s-scenecut-output.mp4", msg.ProjectID, msg.JobID)
	if err := s3.Upload(ctx, outputPath, outputKey, "video/mp4"); err != nil {
		return nil, fmt.Errorf("upload output: %w", err)
	}

	report := buildSceneReport(msg, predictions, segments, cfg)
	reportPath := filepath.Join(jobDir, "scene_report.json")
	if err := os.WriteFile(reportPath, report, 0o644); err != nil {
		return nil, fmt.Errorf("write report: %w", err)
	}

	reportKey := fmt.Sprintf("projects/%s/reports/%s-scenecut.json", msg.ProjectID, msg.JobID)
	if err := s3.Upload(ctx, reportPath, reportKey, "application/json"); err != nil {
		return nil, fmt.Errorf("upload report: %w", err)
	}

	return &SceneCutResult{
		OutputS3Key: outputKey,
		ReportS3Key: reportKey,
		Segments:    segments,
		Predictions: predictions,
	}, nil
}

func predictionsToSegments(predictions map[int64]float64, cfg SceneCutConfig) []Segment {
	if cfg.TotalFrames == 0 {
		return nil
	}

	var cutFrames []int64
	for frame, prob := range predictions {
		if prob >= cfg.Threshold {
			cutFrames = append(cutFrames, frame)
		}
	}

	sortFrames(cutFrames)
	cutFrames = enforceMinSegLength(cutFrames, cfg.MinSegFrames, cfg.TotalFrames)

	var segments []Segment
	fps := cfg.FPS
	if fps <= 0 {
		fps = 30
	}

	start := int64(0)
	for i, cut := range cutFrames {
		segments = append(segments, Segment{
			Index:      i,
			StartFrame: start,
			EndFrame:   cut - 1,
			StartTime:  frameToDuration(start, fps),
			EndTime:    frameToDuration(cut-1, fps),
		})
		start = cut
	}

	segments = append(segments, Segment{
		Index:      len(segments),
		StartFrame: start,
		EndFrame:   cfg.TotalFrames - 1,
		StartTime:  frameToDuration(start, fps),
		EndTime:    frameToDuration(cfg.TotalFrames-1, fps),
	})

	return segments
}

func enforceMinSegLength(cuts []int64, minFrames, totalFrames int64) []int64 {
	if minFrames <= 0 {
		return cuts
	}
	var filtered []int64
	prev := int64(0)
	for _, c := range cuts {
		if c-prev >= minFrames {
			filtered = append(filtered, c)
			prev = c
		}
	}
	return filtered
}

func sortFrames(frames []int64) {
	for i := 1; i < len(frames); i++ {
		for j := i; j > 0 && frames[j] < frames[j-1]; j-- {
			frames[j], frames[j-1] = frames[j-1], frames[j]
		}
	}
}

func frameToDuration(frame int64, fps float64) time.Duration {
	return time.Duration(float64(frame) / fps * float64(time.Second))
}

func buildSceneReport(msg messages.JobMessage, predictions map[int64]float64, segments []Segment, cfg SceneCutConfig) []byte {
	predMap := make(map[string]float64, len(predictions))
	for f, p := range predictions {
		predMap[fmt.Sprintf("%d", f)] = p
	}

	type cutEntry struct {
		StartFrame int64 `json:"start_frame"`
		EndFrame   int64 `json:"end_frame"`
	}
	cuts := make([]cutEntry, len(segments))
	for i, s := range segments {
		cuts[i] = cutEntry{StartFrame: s.StartFrame, EndFrame: s.EndFrame}
	}

	report := map[string]any{
		"header": map[string]any{
			"version":    "0.1",
			"type":       "scene_detection",
			"software":   "kvqtool-web",
			"model":      "transnetv2",
			"threshold":  cfg.Threshold,
			"fps":        cfg.FPS,
			"job_id":     msg.JobID.String(),
			"project_id": msg.ProjectID.String(),
		},
		"predictions": predMap,
		"cuts":        cuts,
	}

	data, _ := json.MarshalIndent(report, "", "  ")
	return data
}
