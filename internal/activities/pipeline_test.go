package activities

import (
	"strings"
	"testing"
)

// --- hasTrimParams ---

func TestHasTrimParams_TrueWhenStartSet(t *testing.T) {
	if !hasTrimParams(map[string]string{"start_time": "00:00:05.000"}) {
		t.Error("expected true")
	}
}

func TestHasTrimParams_TrueWhenEndSet(t *testing.T) {
	if !hasTrimParams(map[string]string{"end_time": "00:00:30.000"}) {
		t.Error("expected true")
	}
}

func TestHasTrimParams_TrueWhenBothSet(t *testing.T) {
	if !hasTrimParams(map[string]string{"start_time": "00:00:00.000", "end_time": "00:01:00.000"}) {
		t.Error("expected true")
	}
}

func TestHasTrimParams_FalseWhenNeitherSet(t *testing.T) {
	if hasTrimParams(map[string]string{}) {
		t.Error("expected false")
	}
}

func TestHasTrimParams_FalseWhenEmpty(t *testing.T) {
	if hasTrimParams(map[string]string{"start_time": "", "end_time": ""}) {
		t.Error("expected false for empty strings")
	}
}

// --- buildScaleChain ---

func TestBuildScaleChain_NoneWhenNoParams(t *testing.T) {
	if got := buildScaleChain(map[string]string{}); got != "" {
		t.Errorf("expected empty, got %q", got)
	}
}

func TestBuildScaleChain_WithWidthAndHeight(t *testing.T) {
	got := buildScaleChain(map[string]string{"scale_width": "1280", "scale_height": "720"})
	if !strings.Contains(got, "videoscale") || !strings.Contains(got, "width=1280") || !strings.Contains(got, "height=720") {
		t.Errorf("unexpected scale chain: %q", got)
	}
}

func TestBuildScaleChain_WithWidthHeightAndMethod(t *testing.T) {
	got := buildScaleChain(map[string]string{"scale_width": "1920", "scale_height": "1080", "scale_method": "bilinear"})
	if !strings.Contains(got, "method=bilinear") {
		t.Errorf("missing method: %q", got)
	}
}

func TestBuildScaleChain_NoneWhenOnlyWidthSet(t *testing.T) {
	// Requires both width AND height to apply scale
	got := buildScaleChain(map[string]string{"scale_width": "1280"})
	if got != "" {
		t.Errorf("expected empty when only width set, got %q", got)
	}
}

func TestBuildScaleChain_NoneWhenOnlyHeightSet(t *testing.T) {
	got := buildScaleChain(map[string]string{"scale_height": "720"})
	if got != "" {
		t.Errorf("expected empty when only height set, got %q", got)
	}
}

// --- buildSceneCutPipeline ---

func TestBuildSceneCutPipeline_ContainsInputPath(t *testing.T) {
	got := buildSceneCutPipeline("/data/video.mp4")
	if !strings.Contains(got, "/data/video.mp4") {
		t.Errorf("missing input path: %q", got)
	}
}

func TestBuildSceneCutPipeline_ContainsVideoanalyse(t *testing.T) {
	got := buildSceneCutPipeline("in.mp4")
	if !strings.Contains(got, "videoanalyse") {
		t.Errorf("missing videoanalyse: %q", got)
	}
}

func TestBuildSceneCutPipeline_ContainsFakesink(t *testing.T) {
	got := buildSceneCutPipeline("in.mp4")
	if !strings.Contains(got, "fakesink") {
		t.Errorf("missing fakesink: %q", got)
	}
}

// --- buildX264Pipeline ---

func TestBuildX264Pipeline_ContainsPaths(t *testing.T) {
	got := buildX264Pipeline("/in.mp4", "/out.mp4", map[string]string{})
	if !strings.Contains(got, "/in.mp4") || !strings.Contains(got, "/out.mp4") {
		t.Errorf("missing paths: %q", got)
	}
}

func TestBuildX264Pipeline_DefaultPresetMedium(t *testing.T) {
	got := buildX264Pipeline("in", "out", map[string]string{})
	if !strings.Contains(got, "speed-preset=medium") {
		t.Errorf("missing default preset: %q", got)
	}
}

func TestBuildX264Pipeline_DefaultProfileHigh(t *testing.T) {
	got := buildX264Pipeline("in", "out", map[string]string{})
	if !strings.Contains(got, "profile=high") {
		t.Errorf("missing default profile: %q", got)
	}
}

func TestBuildX264Pipeline_BitrateSetsCBR(t *testing.T) {
	got := buildX264Pipeline("in", "out", map[string]string{"bitrate_kbps": "4000"})
	if !strings.Contains(got, "pass=cbr") {
		t.Errorf("expected cbr pass: %q", got)
	}
	if !strings.Contains(got, "bitrate=4000") {
		t.Errorf("expected bitrate: %q", got)
	}
}

func TestBuildX264Pipeline_NoBitrateUsesQuant(t *testing.T) {
	got := buildX264Pipeline("in", "out", map[string]string{})
	if !strings.Contains(got, "pass=quant") {
		t.Errorf("expected quant pass: %q", got)
	}
}

func TestBuildX264Pipeline_CRFPropagated(t *testing.T) {
	got := buildX264Pipeline("in", "out", map[string]string{"crf": "23"})
	if !strings.Contains(got, "quantizer=23") {
		t.Errorf("expected quantizer: %q", got)
	}
}

func TestBuildX264Pipeline_CustomPreset(t *testing.T) {
	got := buildX264Pipeline("in", "out", map[string]string{"preset": "slow"})
	if !strings.Contains(got, "speed-preset=slow") {
		t.Errorf("expected slow preset: %q", got)
	}
}

func TestBuildX264Pipeline_CustomProfile(t *testing.T) {
	got := buildX264Pipeline("in", "out", map[string]string{"profile": "baseline"})
	if !strings.Contains(got, "profile=baseline") {
		t.Errorf("expected baseline profile: %q", got)
	}
}

func TestBuildX264Pipeline_GOPLength(t *testing.T) {
	got := buildX264Pipeline("in", "out", map[string]string{"gop_length": "60"})
	if !strings.Contains(got, "key-int-max=60") {
		t.Errorf("expected key-int-max: %q", got)
	}
}

func TestBuildX264Pipeline_Tune(t *testing.T) {
	got := buildX264Pipeline("in", "out", map[string]string{"tune": "film"})
	if !strings.Contains(got, "tune=film") {
		t.Errorf("expected tune: %q", got)
	}
}

func TestBuildX264Pipeline_ContainsX264enc(t *testing.T) {
	got := buildX264Pipeline("in", "out", map[string]string{})
	if !strings.Contains(got, "x264enc") {
		t.Errorf("missing x264enc: %q", got)
	}
}
