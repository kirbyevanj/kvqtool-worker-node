package activities

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/kirbyevanj/kvqtool-kvq-models/types"
	"go.temporal.io/sdk/activity"
)

// FileMetricAnalysis computes VMAF/SSIM/PSNR on local reference and distorted files using ffmpeg.
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

	workDir := filepath.Join(a.TmpDir, input.NodeID)
	os.MkdirAll(workDir, 0o755)
	defer os.RemoveAll(workDir)

	report, err := a.runFFmpegMetrics(ctx, refPath, distPath, workDir, input.Params)
	if err != nil {
		return fail(input.NodeID, fmt.Sprintf("metrics: %s", err)), nil
	}
	activity.RecordHeartbeat(ctx, "metrics complete")
	return okJSON(input.NodeID, "", report), nil
}

// RemoteFileMetricAnalysis uses presigned S3 URLs as ffmpeg inputs — no temp file downloads.
// ffmpeg can seek both streams via HTTP range requests, enabling accurate metric computation.
func (a *Activities) RemoteFileMetricAnalysis(ctx context.Context, input types.ActivityInput) (*types.ActivityOutput, error) {
	refKey := input.Params["reference_s3_key"]
	distKey := input.Params["distorted_s3_key"]
	if refKey == "" || distKey == "" {
		return fail(input.NodeID, "reference_s3_key and distorted_s3_key required"), nil
	}

	refURL, err := a.S3.PresignGet(ctx, refKey, 2*time.Hour)
	if err != nil {
		return fail(input.NodeID, fmt.Sprintf("presign ref: %s", err)), nil
	}
	distURL, err := a.S3.PresignGet(ctx, distKey, 2*time.Hour)
	if err != nil {
		return fail(input.NodeID, fmt.Sprintf("presign dist: %s", err)), nil
	}
	activity.RecordHeartbeat(ctx, "presigned")

	workDir := filepath.Join(a.TmpDir, input.NodeID)
	os.MkdirAll(workDir, 0o755)
	defer os.RemoveAll(workDir)

	report, err := a.runFFmpegMetrics(ctx, refURL, distURL, workDir, input.Params)
	if err != nil {
		return fail(input.NodeID, fmt.Sprintf("metrics: %s", err)), nil
	}

	reportJSON, _ := json.MarshalIndent(report, "", "  ")
	reportName := input.Params["output_name"]
	if reportName == "" {
		reportName = "metrics.json"
	}
	uploadKey := fmt.Sprintf("projects/%s/media/%s-%s", input.ProjectID, input.NodeID, reportName)
	if err := a.S3.UploadStream(ctx, bytes.NewReader(reportJSON), uploadKey, "application/json"); err != nil {
		return fail(input.NodeID, fmt.Sprintf("upload: %s", err)), nil
	}
	activity.RecordHeartbeat(ctx, "uploaded")

	a.registerResource(ctx, input.ProjectID, reportName, uploadKey, "application/json")
	return okJSON(input.NodeID, uploadKey, report), nil
}

// runFFmpegMetrics computes enabled metrics via ffmpeg's ssim, psnr, and libvmaf filters.
// refInput and distInput may be local file paths or HTTP presigned URLs.
func (a *Activities) runFFmpegMetrics(ctx context.Context, refInput, distInput, tmpDir string, params map[string]string) (*types.MetricReports, error) {
	doVMAF := params["vmaf"] != "false"
	doSSIM := params["ssim"] != "false"
	doPSNR := params["psnr"] != "false"

	var vmafMean, ssimAll, psnrAvg float64

	if doSSIM || doPSNR {
		s, p, err := runFFmpegSSIMPSNR(ctx, refInput, distInput, doSSIM, doPSNR)
		if err != nil {
			return nil, fmt.Errorf("ssim/psnr: %w", err)
		}
		ssimAll = s
		psnrAvg = p
	}

	if doVMAF {
		v, err := runFFmpegVMAF(ctx, refInput, distInput, tmpDir)
		if err != nil {
			// libvmaf is optional; degrade gracefully so the activity still succeeds.
			a.Logger.Warn("VMAF failed (libvmaf may not be available in this ffmpeg build)", "err", err)
			doVMAF = false
		} else {
			vmafMean = v
		}
	}

	var metrics []string
	if doVMAF {
		metrics = append(metrics, "vmaf")
	}
	if doSSIM {
		metrics = append(metrics, "ssim")
	}
	if doPSNR {
		metrics = append(metrics, "psnr")
	}

	return &types.MetricReports{
		Header: types.MetricHeader{
			Version:   "0.1",
			Metrics:   metrics,
			Reference: sourceLabel(refInput),
			Dist:      map[string]string{"0": sourceLabel(distInput)},
		},
		Vmaf: map[string]map[string]string{"0": {"mean": fmt.Sprintf("%.4f", vmafMean)}},
		Ssim: map[string]map[string]string{"0": {"mean": fmt.Sprintf("%.6f", ssimAll)}},
		Psnr: map[string]map[string]string{"0": {"average": fmt.Sprintf("%.4f", psnrAvg)}},
	}, nil
}

// runFFmpegSSIMPSNR computes SSIM and/or PSNR in a single ffmpeg pass.
// scale2ref scales the distorted stream to the reference resolution so that
// SSIM/PSNR filters (which require identical dimensions) never fail on
// mismatched inputs (e.g. 1920x1080 reference vs 1280x720 distorted).
func runFFmpegSSIMPSNR(ctx context.Context, refInput, distInput string, doSSIM, doPSNR bool) (ssim, psnr float64, err error) {
	var filter string
	var mapArgs []string
	if doSSIM && doPSNR {
		// scale2ref: [to_scale][size_ref] → [scaled][ref_pass]
		filter = "[1:v][0:v]scale2ref[ds][r];" +
			"[r]split=2[r0][r1];" +
			"[ds]split=2[d0][d1];" +
			"[r0][d0]ssim[vs];" +
			"[r1][d1]psnr[vp]"
		mapArgs = []string{"-map", "[vs]", "-f", "null", "/dev/null", "-map", "[vp]", "-f", "null", "/dev/null"}
	} else if doSSIM {
		filter = "[1:v][0:v]scale2ref[ds][r];[r][ds]ssim[vs]"
		mapArgs = []string{"-map", "[vs]", "-f", "null", "/dev/null"}
	} else {
		filter = "[1:v][0:v]scale2ref[ds][r];[r][ds]psnr[vp]"
		mapArgs = []string{"-map", "[vp]", "-f", "null", "/dev/null"}
	}
	args := append([]string{"-y", "-i", refInput, "-i", distInput, "-filter_complex", filter}, mapArgs...)
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf
	if runErr := cmd.Run(); runErr != nil {
		return 0, 0, fmt.Errorf("ffmpeg: %w: %s", runErr, stderrBuf.String())
	}

	out := stderrBuf.String()
	if doSSIM {
		ssim = parseSSIMFromStderr(out)
	}
	if doPSNR {
		psnr = parsePSNRFromStderr(out)
	}
	return ssim, psnr, nil
}

// runFFmpegVMAF runs libvmaf and returns the pooled mean VMAF score from the JSON log.
// scale2ref ensures the distorted stream matches reference resolution before comparison.
// libvmaf input order: [distorted][reference].
func runFFmpegVMAF(ctx context.Context, refInput, distInput, tmpDir string) (float64, error) {
	vmafLog := filepath.Join(tmpDir, "vmaf.json")
	// [1:v]=distorted, [0:v]=reference; scale2ref outputs [ds]=scaled_dist, [r]=ref.
	filter := fmt.Sprintf("[1:v][0:v]scale2ref[ds][r];[ds][r]libvmaf=log_path=%s:log_fmt=json[vmafout]", vmafLog)
	args := []string{
		"-y", "-i", refInput, "-i", distInput,
		"-filter_complex", filter,
		"-map", "[vmafout]", "-f", "null", "/dev/null",
	}

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf
	if runErr := cmd.Run(); runErr != nil {
		return 0, fmt.Errorf("ffmpeg vmaf: %w: %s", runErr, stderrBuf.String())
	}
	return parseVMAFFromLog(vmafLog)
}

// parseSSIMFromStderr extracts the all-channel SSIM mean from ffmpeg stderr.
// ffmpeg prints: "SSIM Mean Y:0.987654 (18.97) ... All:0.989123 (19.58)"
var ssimAllRe = regexp.MustCompile(`\bAll:(\d+\.\d+)`)

func parseSSIMFromStderr(s string) float64 {
	m := ssimAllRe.FindStringSubmatch(s)
	if len(m) < 2 {
		return 0
	}
	v, _ := strconv.ParseFloat(m[1], 64)
	return v
}

// parsePSNRFromStderr extracts the average PSNR from ffmpeg stderr.
// ffmpeg prints: "PSNR y:43.97 ... average:44.80 min:38.43 max:68.07"
var psnrAvgRe = regexp.MustCompile(`\baverage:(\d+\.\d+)`)

func parsePSNRFromStderr(s string) float64 {
	m := psnrAvgRe.FindStringSubmatch(s)
	if len(m) < 2 {
		return 0
	}
	v, _ := strconv.ParseFloat(m[1], 64)
	return v
}

// parseVMAFFromLog reads the pooled mean VMAF score from the libvmaf JSON log file.
func parseVMAFFromLog(logPath string) (float64, error) {
	data, err := os.ReadFile(logPath)
	if err != nil {
		return 0, fmt.Errorf("read vmaf log: %w", err)
	}
	var result struct {
		PooledMetrics struct {
			VMAF struct {
				Mean float64 `json:"mean"`
			} `json:"vmaf"`
		} `json:"pooled_metrics"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return 0, fmt.Errorf("parse vmaf json: %w", err)
	}
	return result.PooledMetrics.VMAF.Mean, nil
}

// sourceLabel returns a short display label for a file path or presigned HTTP URL.
func sourceLabel(input string) string {
	if strings.HasPrefix(input, "http") {
		parts := strings.Split(input, "/")
		if len(parts) > 0 {
			key := parts[len(parts)-1]
			if idx := strings.Index(key, "?"); idx != -1 {
				key = key[:idx]
			}
			return key
		}
	}
	return filepath.Base(input)
}
