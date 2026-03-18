package activities

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/kirbyevanj/kvqtool-kvq-models/types"
	"go.temporal.io/sdk/activity"
	"golang.org/x/sync/errgroup"
)

// FileMetricAnalysis runs VMAF/SSIM/PSNR on local reference and distorted files.
func (a *Activities) FileMetricAnalysis(ctx context.Context, input types.ActivityInput) (*types.ActivityOutput, error) {
	refPath := input.Params["reference_path"]
	if refPath == "" {
		refPath = resolveUpstreamLocalPath(input)
	}
	distPath := input.Params["distorted_path"]
	if distPath == "" {
		distPath = resolveUpstreamParam(input, "distorted_path")
	}
	if refPath == "" || distPath == "" {
		return fail(input.NodeID, "reference_path and distorted_path required"), nil
	}

	pipeline := buildMetricPipeline(refPath, distPath, input.Params)
	a.Logger.Info("FileMetricAnalysis", "pipeline", pipeline)

	if err := runGstPipeline(ctx, pipeline); err != nil {
		return fail(input.NodeID, fmt.Sprintf("metrics: %s", err)), nil
	}

	report := buildMetricReport(refPath, distPath, input.Params)

	outputDir := filepath.Join(a.TmpDir, input.NodeID)
	os.MkdirAll(outputDir, 0o755)
	reportPath := filepath.Join(outputDir, "metrics.json")
	reportJSON, _ := json.MarshalIndent(report, "", "  ")
	os.WriteFile(reportPath, reportJSON, 0o644)

	activity.RecordHeartbeat(ctx, "metrics complete")
	return okJSON(input.NodeID, "", report), nil
}

// RemoteFileMetricAnalysis concurrently streams both S3 objects, runs metrics, and uploads the report.
// Both downloads start simultaneously and write to temp files (GStreamer iqa requires seekable inputs).
func (a *Activities) RemoteFileMetricAnalysis(ctx context.Context, input types.ActivityInput) (*types.ActivityOutput, error) {
	refKey := input.Params["reference_s3_key"]
	distKey := input.Params["distorted_s3_key"]
	if refKey == "" || distKey == "" {
		return fail(input.NodeID, "reference_s3_key and distorted_s3_key required"), nil
	}

	workDir := filepath.Join(a.TmpDir, input.NodeID)
	os.MkdirAll(workDir, 0o755)
	defer os.RemoveAll(workDir)

	refPath := filepath.Join(workDir, "reference.mp4")
	distPath := filepath.Join(workDir, "distorted.mp4")

	// Download both streams simultaneously.
	eg, dlCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		if err := a.S3.Download(dlCtx, refKey, refPath); err != nil {
			return fmt.Errorf("download ref: %w", err)
		}
		return nil
	})
	eg.Go(func() error {
		if err := a.S3.Download(dlCtx, distKey, distPath); err != nil {
			return fmt.Errorf("download dist: %w", err)
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return fail(input.NodeID, err.Error()), nil
	}
	activity.RecordHeartbeat(ctx, "both downloads complete")

	pipeline := buildMetricPipeline(refPath, distPath, input.Params)
	a.Logger.Info("RemoteFileMetricAnalysis", "pipeline", pipeline)

	if err := runGstPipeline(ctx, pipeline); err != nil {
		return fail(input.NodeID, fmt.Sprintf("metrics: %s", err)), nil
	}

	report := buildMetricReport(refPath, distPath, input.Params)
	reportJSON, _ := json.MarshalIndent(report, "", "  ")

	reportName := input.Params["output_name"]
	if reportName == "" {
		reportName = "metrics.json"
	}
	uploadKey := fmt.Sprintf("projects/%s/media/%s-%s", input.ProjectID, input.NodeID, reportName)

	reportPath := filepath.Join(workDir, reportName)
	os.WriteFile(reportPath, reportJSON, 0o644)

	if err := a.S3.Upload(ctx, reportPath, uploadKey, "application/json"); err != nil {
		return fail(input.NodeID, fmt.Sprintf("upload: %s", err)), nil
	}
	activity.RecordHeartbeat(ctx, "uploaded")

	a.registerResource(ctx, input.ProjectID, reportName, uploadKey, "application/json")
	return okJSON(input.NodeID, uploadKey, report), nil
}

func buildMetricReport(refPath, distPath string, params map[string]string) *types.MetricReports {
	var metrics []string
	if params["vmaf"] != "false" {
		metrics = append(metrics, "vmaf")
	}
	if params["ssim"] != "false" {
		metrics = append(metrics, "ssim")
	}
	if params["psnr"] != "false" {
		metrics = append(metrics, "psnr")
	}

	return &types.MetricReports{
		Header: types.MetricHeader{
			Version:   "0.1",
			Metrics:   metrics,
			Reference: filepath.Base(refPath),
			Dist:      map[string]string{"0": filepath.Base(distPath)},
		},
		Vmaf: map[string]map[string]string{"0": {}},
		Ssim: map[string]map[string]string{"0": {}},
		Psnr: map[string]map[string]string{"0": {}},
	}
}
