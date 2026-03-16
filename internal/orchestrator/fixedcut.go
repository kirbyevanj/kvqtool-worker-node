package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/kirbyevanj/kvqtool-kvq-models/messages"
	"github.com/kirbyevanj/kvqtool-worker-node/internal/s3client"
)

type FixedCutConfig struct {
	SegmentDuration time.Duration
	EncodeParams    EncodeParams
	FPS             float64
	TotalFrames     int64
}

func RunFixedCutAndEncode(
	ctx context.Context,
	msg messages.JobMessage,
	inputPath string,
	cfg FixedCutConfig,
	s3 *s3client.Client,
	jobDir string,
	logger *slog.Logger,
) (string, error) {
	segments := generateFixedSegments(cfg)
	logger.Info("fixed segments generated", "count", len(segments), "duration", cfg.SegmentDuration)

	results, err := EncodeSegments(ctx, inputPath, segments, cfg.EncodeParams, jobDir, logger)
	if err != nil {
		return "", fmt.Errorf("encode segments: %w", err)
	}

	var segPaths []string
	for _, r := range results {
		segPaths = append(segPaths, r.FilePath)
	}

	outputPath := filepath.Join(jobDir, "final_output.mp4")
	if err := ConcatSegments(ctx, segPaths, outputPath, logger); err != nil {
		return "", fmt.Errorf("concat: %w", err)
	}

	outputKey := fmt.Sprintf("projects/%s/media/%s-fixedcut-output.mp4", msg.ProjectID, msg.JobID)
	if err := s3.Upload(ctx, outputPath, outputKey, "video/mp4"); err != nil {
		return "", fmt.Errorf("upload: %w", err)
	}

	return outputKey, nil
}

func generateFixedSegments(cfg FixedCutConfig) []Segment {
	fps := cfg.FPS
	if fps <= 0 {
		fps = 30
	}

	framesPerSegment := int64(cfg.SegmentDuration.Seconds() * fps)
	if framesPerSegment <= 0 {
		framesPerSegment = int64(4 * fps)
	}

	var segments []Segment
	for start := int64(0); start < cfg.TotalFrames; start += framesPerSegment {
		end := start + framesPerSegment - 1
		if end >= cfg.TotalFrames {
			end = cfg.TotalFrames - 1
		}
		segments = append(segments, Segment{
			Index:      len(segments),
			StartFrame: start,
			EndFrame:   end,
			StartTime:  frameToDuration(start, fps),
			EndTime:    frameToDuration(end, fps),
		})
	}
	return segments
}
