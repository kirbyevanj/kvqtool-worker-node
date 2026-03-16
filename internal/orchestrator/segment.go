package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/go-gst/go-gst/gst"
	"github.com/kirbyevanj/kvqtool-kvq-models/messages"
)

type Segment struct {
	Index      int
	StartFrame int64
	EndFrame   int64
	StartTime  time.Duration
	EndTime    time.Duration
}

type EncodeParams struct {
	Pass      string
	CRF       string
	Bitrate   string
	Preset    string
	GOPLength string
}

type SegmentResult struct {
	Index    int
	FilePath string
	Err      error
}

func EncodeSegments(ctx context.Context, inputPath string, segments []Segment, params EncodeParams, jobDir string, logger *slog.Logger) ([]SegmentResult, error) {
	maxParallel := runtime.NumCPU() / 2
	if maxParallel < 1 {
		maxParallel = 1
	}

	results := make([]SegmentResult, len(segments))
	sem := make(chan struct{}, maxParallel)
	var wg sync.WaitGroup

	for i, seg := range segments {
		wg.Add(1)
		go func(idx int, s Segment) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			outPath := filepath.Join(jobDir, fmt.Sprintf("segment_%04d.mp4", idx))
			err := encodeSegment(ctx, inputPath, outPath, s, params, logger)
			results[idx] = SegmentResult{Index: idx, FilePath: outPath, Err: err}
		}(i, seg)
	}

	wg.Wait()

	for _, r := range results {
		if r.Err != nil {
			return results, fmt.Errorf("segment %d failed: %w", r.Index, r.Err)
		}
	}
	return results, nil
}

func encodeSegment(ctx context.Context, inputPath, outputPath string, seg Segment, params EncodeParams, logger *slog.Logger) error {
	pipelineStr := buildSegmentPipeline(inputPath, outputPath, seg, params)
	logger.Info("encoding segment", "index", seg.Index, "start", seg.StartTime, "end", seg.EndTime, "pipeline", pipelineStr)

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
			return fmt.Errorf("gst error: %s", gerr.Error())
		default:
			msg.Unref()
		}
	}
}

func buildSegmentPipeline(inputPath, outputPath string, seg Segment, params EncodeParams) string {
	pass := "quant"
	encProps := ""

	if params.Bitrate != "" {
		pass = "cbr"
		encProps += " bitrate=" + params.Bitrate
	}
	if params.CRF != "" {
		encProps += " quantizer=" + params.CRF
	}
	preset := params.Preset
	if preset == "" {
		preset = "medium"
	}
	encProps += " speed-preset=" + preset
	if params.GOPLength != "" {
		encProps += " key-int-max=" + params.GOPLength
	}

	return fmt.Sprintf(
		"filesrc location=%s ! decodebin ! videoconvert ! x264enc pass=%s%s ! video/x-h264,profile=high ! mp4mux ! filesink location=%s",
		inputPath, pass, encProps, outputPath,
	)
}

func ConcatSegments(ctx context.Context, segmentPaths []string, outputPath string, logger *slog.Logger) error {
	listPath := outputPath + ".list.txt"
	f, err := os.Create(listPath)
	if err != nil {
		return fmt.Errorf("create concat list: %w", err)
	}
	for _, p := range segmentPaths {
		fmt.Fprintf(f, "file '%s'\n", p)
	}
	f.Close()
	defer os.Remove(listPath)

	pipelineStr := buildConcatPipeline(segmentPaths)
	logger.Info("concatenating segments", "count", len(segmentPaths), "output", outputPath)

	pipeline, err := gst.NewPipelineFromString(pipelineStr)
	if err != nil {
		logger.Warn("gstreamer concat failed, falling back to copy", "err", err)
		return concatByCopy(segmentPaths, outputPath)
	}
	defer pipeline.Unref()

	bus := pipeline.GetBus()
	defer bus.Unref()

	if err := pipeline.SetState(gst.StatePlaying); err != nil {
		pipeline.SetState(gst.StateNull)
		return concatByCopy(segmentPaths, outputPath)
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
			logger.Warn("concat pipeline error, falling back", "err", gerr.Error())
			return concatByCopy(segmentPaths, outputPath)
		default:
			msg.Unref()
		}
	}
}

func buildConcatPipeline(paths []string) string {
	pipeline := "concat name=c ! mp4mux ! filesink location=output.mp4"
	for _, p := range paths {
		pipeline = fmt.Sprintf("filesrc location=%s ! decodebin ! c. ", p) + pipeline
	}
	return pipeline
}

func concatByCopy(segmentPaths []string, outputPath string) error {
	out, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer out.Close()

	for _, p := range segmentPaths {
		data, err := os.ReadFile(p)
		if err != nil {
			return fmt.Errorf("read segment %s: %w", p, err)
		}
		if _, err := out.Write(data); err != nil {
			return fmt.Errorf("write segment: %w", err)
		}
	}
	return nil
}

// ProgressCallback reports aggregate progress across all segments.
func ReportSegmentProgress(consumer interface {
	PublishProgress(context.Context, messages.JobProgress)
}, ctx context.Context, jobMsg messages.JobMessage, completed, total int) {
	pct := int32(float64(completed) / float64(total) * 100)
	consumer.PublishProgress(ctx, messages.JobProgress{
		JobID:       jobMsg.JobID,
		Status:      "running",
		ProgressPct: pct,
		Message:     fmt.Sprintf("segments: %d/%d", completed, total),
	})
}
