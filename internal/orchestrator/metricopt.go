package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"

	"github.com/kirbyevanj/kvqtool-kvq-models/messages"
	"github.com/kirbyevanj/kvqtool-worker-node/internal/executor"
	"github.com/kirbyevanj/kvqtool-worker-node/internal/s3client"
)

type MetricOptConfig struct {
	TargetMetric string
	TargetValue  float64
	Tolerance    float64
	SearchParam  string
	SearchMin    int
	SearchMax    int
	FixedParams  EncodeParams
	FPS          float64
	TotalFrames  int64
	Metrics      []string
}

type OptIteration struct {
	ParamValue int     `json:"param_value"`
	MetricAvg  float64 `json:"metric_avg"`
}

type MetricOptResult struct {
	OptimalValue int            `json:"optimal_value"`
	Iterations   []OptIteration `json:"iterations"`
	OutputS3Key  string         `json:"output_s3_key"`
	ReportS3Key  string         `json:"report_s3_key"`
}

func RunMetricOptimizer(
	ctx context.Context,
	msg messages.JobMessage,
	inputPath string,
	referencePath string,
	cfg MetricOptConfig,
	exec *executor.Executor,
	s3 *s3client.Client,
	jobDir string,
	logger *slog.Logger,
) (*MetricOptResult, error) {
	lo, hi := cfg.SearchMin, cfg.SearchMax
	var iterations []OptIteration
	bestValue := (lo + hi) / 2

	for lo <= hi {
		mid := (lo + hi) / 2
		logger.Info("metric optimizer iteration", "param", cfg.SearchParam, "value", mid, "range", fmt.Sprintf("[%d,%d]", lo, hi))

		avg, err := encodeAndMeasure(ctx, msg, inputPath, referencePath, mid, cfg, exec, s3, jobDir, logger)
		if err != nil {
			return nil, fmt.Errorf("iteration crf=%d: %w", mid, err)
		}

		iterations = append(iterations, OptIteration{ParamValue: mid, MetricAvg: avg})
		bestValue = mid

		if math.Abs(avg-cfg.TargetValue) <= cfg.Tolerance {
			break
		}

		if avg >= cfg.TargetValue {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}

	params := cfg.FixedParams
	params.CRF = strconv.Itoa(bestValue)
	outputPath := filepath.Join(jobDir, "optimal_output.mp4")
	finalPipeline := buildSegmentPipeline(inputPath, outputPath, Segment{
		StartFrame: 0,
		EndFrame:   cfg.TotalFrames - 1,
	}, params)

	_ = finalPipeline

	outputKey := fmt.Sprintf("projects/%s/media/%s-optimized.mp4", msg.ProjectID, msg.JobID)
	if err := s3.Upload(ctx, outputPath, outputKey, "video/mp4"); err != nil {
		return nil, fmt.Errorf("upload optimal: %w", err)
	}

	report := buildOptReport(msg, cfg, iterations, bestValue)
	reportPath := filepath.Join(jobDir, "optimizer_report.json")
	os.WriteFile(reportPath, report, 0o644)

	reportKey := fmt.Sprintf("projects/%s/reports/%s-optimizer.json", msg.ProjectID, msg.JobID)
	if err := s3.Upload(ctx, reportPath, reportKey, "application/json"); err != nil {
		return nil, fmt.Errorf("upload report: %w", err)
	}

	return &MetricOptResult{
		OptimalValue: bestValue,
		Iterations:   iterations,
		OutputS3Key:  outputKey,
		ReportS3Key:  reportKey,
	}, nil
}

func encodeAndMeasure(
	ctx context.Context,
	msg messages.JobMessage,
	inputPath, referencePath string,
	paramValue int,
	cfg MetricOptConfig,
	exec *executor.Executor,
	s3 *s3client.Client,
	jobDir string,
	logger *slog.Logger,
) (float64, error) {
	params := cfg.FixedParams
	params.CRF = strconv.Itoa(paramValue)

	iterDir := filepath.Join(jobDir, fmt.Sprintf("iter_%d", paramValue))
	os.MkdirAll(iterDir, 0o755)

	outputPath := filepath.Join(iterDir, "encoded.mp4")
	seg := Segment{StartFrame: 0, EndFrame: cfg.TotalFrames - 1}
	if err := encodeSegment(ctx, inputPath, outputPath, seg, params, logger); err != nil {
		return 0, err
	}

	noop := func(context.Context, messages.JobProgress) {}
	results, err := exec.CollectMetrics(ctx, msg, referencePath, outputPath, cfg.Metrics, noop)
	if err != nil {
		return 0, fmt.Errorf("collect metrics: %w", err)
	}

	return computeAverage(results, cfg.TargetMetric), nil
}

func computeAverage(results map[string]map[string]map[string]float64, metric string) float64 {
	distData, ok := results[metric]
	if !ok {
		return 0
	}
	frames, ok := distData["0"]
	if !ok {
		return 0
	}

	var sum float64
	for _, v := range frames {
		sum += v
	}
	if len(frames) == 0 {
		return 0
	}
	return sum / float64(len(frames))
}

func buildOptReport(msg messages.JobMessage, cfg MetricOptConfig, iterations []OptIteration, optimal int) []byte {
	report := map[string]any{
		"header": map[string]any{
			"version":       "0.1",
			"type":          "metric_optimizer",
			"software":      "kvqtool-web",
			"target_metric": cfg.TargetMetric,
			"target_value":  cfg.TargetValue,
			"tolerance":     cfg.Tolerance,
			"search_param":  cfg.SearchParam,
			"search_range":  []int{cfg.SearchMin, cfg.SearchMax},
			"job_id":        msg.JobID.String(),
			"project_id":    msg.ProjectID.String(),
		},
		"optimal_value": optimal,
		"iterations":    iterations,
	}
	data, _ := json.MarshalIndent(report, "", "  ")
	return data
}
