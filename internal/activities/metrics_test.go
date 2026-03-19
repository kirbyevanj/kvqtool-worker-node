package activities

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// --- scaleAlgo ---

func TestScaleAlgo_DefaultBicubic(t *testing.T) {
	if got := scaleAlgo(map[string]string{}); got != "bicubic" {
		t.Errorf("got %q", got)
	}
}

func TestScaleAlgo_AcceptsValidAlgos(t *testing.T) {
	valid := []string{"bicubic", "bilinear", "fast_bilinear", "lanczos", "sinc", "spline", "neighbor", "area", "gauss"}
	for _, algo := range valid {
		got := scaleAlgo(map[string]string{"scale_method": algo})
		if got != algo {
			t.Errorf("scale_method=%q: got %q", algo, got)
		}
	}
}

func TestScaleAlgo_RejectsInvalidAlgo(t *testing.T) {
	if got := scaleAlgo(map[string]string{"scale_method": "invalid_algo"}); got != "bicubic" {
		t.Errorf("expected bicubic fallback, got %q", got)
	}
}

func TestScaleAlgo_EmptyStringFallsToDefault(t *testing.T) {
	if got := scaleAlgo(map[string]string{"scale_method": ""}); got != "bicubic" {
		t.Errorf("got %q", got)
	}
}

// --- sourceLabel ---

func TestSourceLabel_LocalPath(t *testing.T) {
	got := sourceLabel("/tmp/work/reference.mp4")
	if got != "reference.mp4" {
		t.Errorf("got %q", got)
	}
}

func TestSourceLabel_HTTPURLExtractsFilename(t *testing.T) {
	url := "http://minio:9000/kvq-bucket/projects/p/media/football.mp4?X-Amz-Date=20260101"
	got := sourceLabel(url)
	if got != "football.mp4" {
		t.Errorf("got %q", got)
	}
}

func TestSourceLabel_HTTPSURLStripQueryString(t *testing.T) {
	url := "https://s3.example.com/bucket/projects/p/file.mp4?Expires=99999&Sig=abc"
	got := sourceLabel(url)
	if got != "file.mp4" {
		t.Errorf("got %q", got)
	}
}

func TestSourceLabel_LocalPathNoDirectory(t *testing.T) {
	got := sourceLabel("video.mp4")
	if got != "video.mp4" {
		t.Errorf("got %q", got)
	}
}

// --- parseSSIMFrames ---

func TestParseSSIMFrames_EmptyString(t *testing.T) {
	m := parseSSIMFrames("")
	if len(m) != 0 {
		t.Errorf("expected empty, got %v", m)
	}
}

func TestParseSSIMFrames_ParsesFrameEntries(t *testing.T) {
	stderr := `[Parsed_ssim_0 @ 0x...] SSIM [0] Y:0.994 U:0.997 V:0.996 All:0.995 (22.00)
[Parsed_ssim_0 @ 0x...] SSIM [1] Y:0.990 U:0.993 V:0.992 All:0.991 (21.00)`
	m := parseSSIMFrames(stderr)
	if _, ok := m["0"]; !ok {
		t.Error("missing frame 0")
	}
	if _, ok := m["1"]; !ok {
		t.Error("missing frame 1")
	}
}

func TestParseSSIMFrames_ParsesMeanLine(t *testing.T) {
	stderr := `[Parsed_ssim_0] SSIM Y:0.994 U:0.997 V:0.996 All:0.995 (22.00)`
	m := parseSSIMFrames(stderr)
	if v, ok := m["mean"]; !ok || v == "" {
		t.Errorf("missing mean: %v", m)
	}
}

// --- parsePSNRFrames ---

func TestParsePSNRFrames_EmptyString(t *testing.T) {
	m := parsePSNRFrames("")
	if len(m) != 0 {
		t.Errorf("expected empty, got %v", m)
	}
}

func TestParsePSNRFrames_ParsesFrameEntries(t *testing.T) {
	stderr := `[Parsed_psnr_1 @ 0x...] PSNR [0] y:44.80 u:48.00 v:48.00 average:44.80 min:40.00 max:50.00
[Parsed_psnr_1 @ 0x...] PSNR [1] y:43.50 u:47.00 v:47.00 average:43.50 min:39.00 max:49.00`
	m := parsePSNRFrames(stderr)
	if _, ok := m["0"]; !ok {
		t.Error("missing frame 0")
	}
	if _, ok := m["1"]; !ok {
		t.Error("missing frame 1")
	}
}

func TestParsePSNRFrames_ParsesMeanLine(t *testing.T) {
	stderr := `[Parsed_psnr_1] PSNR y:44.80 u:48.00 v:48.00 average:44.80 min:40.00 max:50.00`
	m := parsePSNRFrames(stderr)
	if v, ok := m["average"]; !ok || v == "" {
		t.Errorf("missing average: %v", m)
	}
}

// --- parseVMAFFromLog ---

func TestParseVMAFFromLog_ValidLog(t *testing.T) {
	log := map[string]interface{}{
		"frames": []interface{}{
			map[string]interface{}{
				"frameNum": 0,
				"metrics":  map[string]interface{}{"vmaf": 95.123},
			},
			map[string]interface{}{
				"frameNum": 1,
				"metrics":  map[string]interface{}{"vmaf": 93.456},
			},
		},
		"pooled_metrics": map[string]interface{}{
			"vmaf": map[string]interface{}{"mean": 94.289},
		},
	}
	data, _ := json.Marshal(log)
	f := t.TempDir()
	logPath := filepath.Join(f, "vmaf.json")
	os.WriteFile(logPath, data, 0o644)

	frames, mean, err := parseVMAFFromLog(logPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mean < 94.0 || mean > 95.0 {
		t.Errorf("mean=%f", mean)
	}
	if _, ok := frames["0"]; !ok {
		t.Error("missing frame 0")
	}
	if _, ok := frames["1"]; !ok {
		t.Error("missing frame 1")
	}
}

func TestParseVMAFFromLog_MissingFile(t *testing.T) {
	_, _, err := parseVMAFFromLog("/nonexistent/vmaf.json")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestParseVMAFFromLog_EmptyFrames(t *testing.T) {
	log := map[string]interface{}{
		"frames": []interface{}{},
		"pooled_metrics": map[string]interface{}{
			"vmaf": map[string]interface{}{"mean": 0.0},
		},
	}
	data, _ := json.Marshal(log)
	logPath := filepath.Join(t.TempDir(), "vmaf.json")
	os.WriteFile(logPath, data, 0o644)

	frames, mean, err := parseVMAFFromLog(logPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(frames) != 0 {
		t.Errorf("expected empty frames, got %v", frames)
	}
	if mean != 0.0 {
		t.Errorf("mean=%f", mean)
	}
}

func TestParseVMAFFromLog_InvalidJSON(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "vmaf.json")
	os.WriteFile(logPath, []byte(`{invalid`), 0o644)
	_, _, err := parseVMAFFromLog(logPath)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

// --- buildFFmpegX264StreamArgs (from transcode.go) ---

func TestBuildFFmpegX264StreamArgs_ContainsInputURL(t *testing.T) {
	args := buildFFmpegX264StreamArgs("http://minio/key.mp4", map[string]string{})
	found := false
	for _, a := range args {
		if strings.Contains(a, "http://minio/key.mp4") {
			found = true
		}
	}
	if !found {
		t.Errorf("input URL not found in args: %v", args)
	}
}

func TestBuildFFmpegX264StreamArgs_DefaultsApplied(t *testing.T) {
	args := buildFFmpegX264StreamArgs("http://example.com/v.mp4", map[string]string{})
	joined := strings.Join(args, " ")
	// Should include x264 codec
	if !strings.Contains(joined, "libx264") {
		t.Errorf("missing libx264: %v", args)
	}
}
