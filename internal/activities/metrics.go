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
// The JSON report is written to disk so a downstream ResourceUpload node can read it via local_path.
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
	// workDir is intentionally NOT deferred-removed: the output file must persist
	// so a downstream ResourceUpload activity can read it via local_path.

	report, err := a.runFFmpegMetrics(ctx, refPath, distPath, workDir, input.Params)
	if err != nil {
		os.RemoveAll(workDir)
		return fail(input.NodeID, fmt.Sprintf("metrics: %s", err)), nil
	}

	outputName := input.Params["output_name"]
	if outputName == "" {
		outputName = "metrics.json"
	}
	reportPath := filepath.Join(workDir, outputName)
	reportJSON, _ := json.MarshalIndent(report, "", "  ")
	if err := os.WriteFile(reportPath, reportJSON, 0o644); err != nil {
		os.RemoveAll(workDir)
		return fail(input.NodeID, fmt.Sprintf("write report: %s", err)), nil
	}

	activity.RecordHeartbeat(ctx, "metrics complete")
	return ok(input.NodeID, "", map[string]string{
		"local_path":   reportPath,
		"output_name":  outputName,
		"content_type": "application/x-metric-report",
	}), nil
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

	a.registerResource(ctx, input.ProjectID, reportName, uploadKey, "application/x-metric-report")
	return okJSON(input.NodeID, uploadKey, report), nil
}

// scaleAlgo returns the validated ffmpeg scale algorithm name from params, defaulting to bicubic.
// This is used in scale2ref to control the interpolation method when the distorted stream
// must be resized to match the reference before metric computation.
func scaleAlgo(params map[string]string) string {
	valid := map[string]bool{
		"bicubic": true, "bilinear": true, "fast_bilinear": true,
		"lanczos": true, "sinc": true, "spline": true,
		"neighbor": true, "area": true, "gauss": true,
	}
	if m := params["scale_method"]; valid[m] {
		return m
	}
	return "bicubic"
}

// runFFmpegMetrics computes enabled metrics via ffmpeg's ssim, psnr, and libvmaf filters.
// refInput and distInput may be local file paths or HTTP presigned URLs.
func (a *Activities) runFFmpegMetrics(ctx context.Context, refInput, distInput, tmpDir string, params map[string]string) (*types.MetricReports, error) {
	doVMAF := params["vmaf"] != "false"
	doSSIM := params["ssim"] != "false"
	doPSNR := params["psnr"] != "false"
	algo := scaleAlgo(params)

	var vmafMean, ssimAll, psnrAvg float64
	var ssimFrames, psnrFrames, vmafFrames map[string]string

	if doSSIM || doPSNR {
		sf, pf, err := runFFmpegSSIMPSNR(ctx, refInput, distInput, algo, doSSIM, doPSNR)
		if err != nil {
			return nil, fmt.Errorf("ssim/psnr: %w", err)
		}
		ssimFrames = sf
		psnrFrames = pf
		if v, ok := sf["mean"]; ok {
			ssimAll, _ = strconv.ParseFloat(v, 64)
		}
		if v, ok := pf["average"]; ok {
			psnrAvg, _ = strconv.ParseFloat(v, 64)
		}
	}

	if doVMAF {
		vf, v, err := runFFmpegVMAF(ctx, refInput, distInput, tmpDir, algo)
		if err != nil {
			// libvmaf is optional; degrade gracefully so the activity still succeeds.
			a.Logger.Warn("VMAF failed (libvmaf may not be available in this ffmpeg build)", "err", err)
			doVMAF = false
		} else {
			vmafMean = v
			vmafFrames = vf
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

	// mergeAggregate inserts the aggregate key/value if not already present (from per-frame parse).
	mergeAggregate := func(frames map[string]string, key, val string) map[string]string {
		if frames == nil {
			frames = make(map[string]string)
		}
		if _, ok := frames[key]; !ok {
			frames[key] = val
		}
		return frames
	}

	return &types.MetricReports{
		Header: types.MetricHeader{
			Version:   "0.1",
			Metrics:   metrics,
			Reference: sourceLabel(refInput),
			Dist:      map[string]string{"0": sourceLabel(distInput)},
		},
		Vmaf: map[string]map[string]string{"0": mergeAggregate(vmafFrames, "mean", fmt.Sprintf("%.4f", vmafMean))},
		Ssim: map[string]map[string]string{"0": mergeAggregate(ssimFrames, "mean", fmt.Sprintf("%.6f", ssimAll))},
		Psnr: map[string]map[string]string{"0": mergeAggregate(psnrFrames, "average", fmt.Sprintf("%.4f", psnrAvg))},
	}, nil
}

// runFFmpegSSIMPSNR computes SSIM and/or PSNR in a single ffmpeg pass.
// Returns per-frame data as string maps (frame index → value) with aggregate keys "mean"/"average".
func runFFmpegSSIMPSNR(ctx context.Context, refInput, distInput, algo string, doSSIM, doPSNR bool) (ssimFrames, psnrFrames map[string]string, err error) {
	s2r := fmt.Sprintf("[1:v][0:v]scale2ref=flags=%s[ds][r]", algo)
	var filter string
	var mapArgs []string
	if doSSIM && doPSNR {
		filter = s2r + ";[r]split=2[r0][r1];[ds]split=2[d0][d1];[r0][d0]ssim[vs];[r1][d1]psnr[vp]"
		mapArgs = []string{"-map", "[vs]", "-f", "null", "/dev/null", "-map", "[vp]", "-f", "null", "/dev/null"}
	} else if doSSIM {
		filter = s2r + ";[r][ds]ssim[vs]"
		mapArgs = []string{"-map", "[vs]", "-f", "null", "/dev/null"}
	} else {
		filter = s2r + ";[r][ds]psnr[vp]"
		mapArgs = []string{"-map", "[vp]", "-f", "null", "/dev/null"}
	}
	args := append([]string{"-y", "-i", refInput, "-i", distInput, "-filter_complex", filter}, mapArgs...)
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf
	if runErr := cmd.Run(); runErr != nil {
		return nil, nil, fmt.Errorf("ffmpeg: %w: %s", runErr, stderrBuf.String())
	}

	out := stderrBuf.String()
	ssimFrames = make(map[string]string)
	psnrFrames = make(map[string]string)
	if doSSIM {
		ssimFrames = parseSSIMFrames(out)
	}
	if doPSNR {
		psnrFrames = parsePSNRFrames(out)
	}
	return ssimFrames, psnrFrames, nil
}

// runFFmpegVMAF runs libvmaf and returns per-frame scores plus the pooled mean.
// algo is the ffmpeg scale interpolation method used by scale2ref.
// libvmaf input order: [distorted][reference].
func runFFmpegVMAF(ctx context.Context, refInput, distInput, tmpDir, algo string) (map[string]string, float64, error) {
	vmafLog := filepath.Join(tmpDir, "vmaf.json")
	filter := fmt.Sprintf("[1:v][0:v]scale2ref=flags=%s[ds][r];[ds][r]libvmaf=log_path=%s:log_fmt=json[vmafout]", algo, vmafLog)
	args := []string{
		"-y", "-i", refInput, "-i", distInput,
		"-filter_complex", filter,
		"-map", "[vmafout]", "-f", "null", "/dev/null",
	}

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf
	if runErr := cmd.Run(); runErr != nil {
		return nil, 0, fmt.Errorf("ffmpeg vmaf: %w: %s", runErr, stderrBuf.String())
	}
	return parseVMAFFromLog(vmafLog)
}

// parseSSIMFrames extracts per-frame SSIM (All channel) from ffmpeg stderr.
// Per-frame line: "SSIM [N] Y:... All:0.989123 (...)"
// Aggregate line: "SSIM Mean Y:... All:0.989123 (...)"
// Returns map: frame index string → value string, plus "mean" key for aggregate.
var ssimFrameRe = regexp.MustCompile(`SSIM \[(\d+)\][^\n]*\bAll:(\d+\.\d+)`)
var ssimMeanRe = regexp.MustCompile(`SSIM Mean[^\n]*\bAll:(\d+\.\d+)`)

func parseSSIMFrames(s string) map[string]string {
	m := make(map[string]string)
	for _, match := range ssimFrameRe.FindAllStringSubmatch(s, -1) {
		m[match[1]] = match[2]
	}
	if agg := ssimMeanRe.FindStringSubmatch(s); len(agg) >= 2 {
		m["mean"] = agg[1]
	}
	return m
}

// parsePSNRFrames extracts per-frame PSNR average from ffmpeg stderr.
// Per-frame line: "PSNR [N] y:... average:44.80 ..."
// Aggregate line: "PSNR y:... average:44.80 ..."
// Returns map: frame index string → value string, plus "average" key.
var psnrFrameRe = regexp.MustCompile(`PSNR \[(\d+)\][^\n]*\baverage:(\d+\.\d+)`)
var psnrMeanRe = regexp.MustCompile(`PSNR y:[^\n]*\baverage:(\d+\.\d+)`)

func parsePSNRFrames(s string) map[string]string {
	m := make(map[string]string)
	for _, match := range psnrFrameRe.FindAllStringSubmatch(s, -1) {
		m[match[1]] = match[2]
	}
	if agg := psnrMeanRe.FindStringSubmatch(s); len(agg) >= 2 {
		m["average"] = agg[1]
	}
	return m
}

// parseVMAFFromLog reads per-frame VMAF scores and pooled mean from the libvmaf JSON log.
// Returns frames map (frame index → score string), mean float, and any parse error.
func parseVMAFFromLog(logPath string) (map[string]string, float64, error) {
	data, err := os.ReadFile(logPath)
	if err != nil {
		return nil, 0, fmt.Errorf("read vmaf log: %w", err)
	}
	var result struct {
		Frames []struct {
			FrameNum int `json:"frameNum"`
			Metrics  struct {
				VMAF float64 `json:"vmaf"`
			} `json:"metrics"`
		} `json:"frames"`
		PooledMetrics struct {
			VMAF struct {
				Mean float64 `json:"mean"`
			} `json:"vmaf"`
		} `json:"pooled_metrics"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, 0, fmt.Errorf("parse vmaf json: %w", err)
	}
	frames := make(map[string]string, len(result.Frames))
	for _, f := range result.Frames {
		frames[strconv.Itoa(f.FrameNum)] = fmt.Sprintf("%.4f", f.Metrics.VMAF)
	}
	mean := result.PooledMetrics.VMAF.Mean
	frames["mean"] = fmt.Sprintf("%.4f", mean)
	return frames, mean, nil
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
