package activities

import (
	"testing"
)

// --- transnetParams ---

func TestTransnetParams_Defaults(t *testing.T) {
	threshold, fw, fh := transnetParams(map[string]string{})
	if threshold != 0.5 {
		t.Errorf("threshold=%f", threshold)
	}
	if fw != transnetDefaultWidth {
		t.Errorf("fw=%d", fw)
	}
	if fh != transnetDefaultHeight {
		t.Errorf("fh=%d", fh)
	}
}

func TestTransnetParams_CustomThreshold(t *testing.T) {
	threshold, _, _ := transnetParams(map[string]string{"threshold": "0.8"})
	if threshold != 0.8 {
		t.Errorf("threshold=%f", threshold)
	}
}

func TestTransnetParams_InvalidThresholdUsesDefault(t *testing.T) {
	threshold, _, _ := transnetParams(map[string]string{"threshold": "not-a-number"})
	if threshold != 0.5 {
		t.Errorf("threshold=%f", threshold)
	}
}

func TestTransnetParams_CustomFrameDimensions(t *testing.T) {
	_, fw, fh := transnetParams(map[string]string{"frame_width": "96", "frame_height": "54"})
	if fw != 96 {
		t.Errorf("fw=%d", fw)
	}
	if fh != 54 {
		t.Errorf("fh=%d", fh)
	}
}

func TestTransnetParams_ZeroWidthUsesDefault(t *testing.T) {
	_, fw, _ := transnetParams(map[string]string{"frame_width": "0"})
	if fw != transnetDefaultWidth {
		t.Errorf("fw=%d, expected default %d", fw, transnetDefaultWidth)
	}
}

func TestTransnetParams_NegativeWidthUsesDefault(t *testing.T) {
	_, fw, _ := transnetParams(map[string]string{"frame_width": "-10"})
	if fw != transnetDefaultWidth {
		t.Errorf("fw=%d", fw)
	}
}

func TestTransnetParams_InvalidWidthUsesDefault(t *testing.T) {
	_, fw, _ := transnetParams(map[string]string{"frame_width": "abc"})
	if fw != transnetDefaultWidth {
		t.Errorf("fw=%d", fw)
	}
}

// --- parseDurationFromFFmpegStderr ---

func TestParseDurationFromFFmpegStderr_Standard(t *testing.T) {
	stderr := `Input #0, mov,mp4,m4a ... Duration: 00:02:00.13, start: 0.00`
	got := parseDurationFromFFmpegStderr(stderr)
	// 2*60 + 0.13 = 120.13
	if got < 120.0 || got > 121.0 {
		t.Errorf("got %f", got)
	}
}

func TestParseDurationFromFFmpegStderr_OneHour(t *testing.T) {
	stderr := `Duration: 01:00:00.00, start: 0.00`
	got := parseDurationFromFFmpegStderr(stderr)
	if got != 3600.0 {
		t.Errorf("got %f", got)
	}
}

func TestParseDurationFromFFmpegStderr_NoDuration(t *testing.T) {
	got := parseDurationFromFFmpegStderr("no duration here")
	if got != 0 {
		t.Errorf("got %f", got)
	}
}

func TestParseDurationFromFFmpegStderr_ShortVideo(t *testing.T) {
	stderr := `Duration: 00:00:05.50, start: 0`
	got := parseDurationFromFFmpegStderr(stderr)
	if got < 5.4 || got > 5.6 {
		t.Errorf("got %f", got)
	}
}

func TestParseDurationFromFFmpegStderr_EmptyString(t *testing.T) {
	got := parseDurationFromFFmpegStderr("")
	if got != 0 {
		t.Errorf("got %f", got)
	}
}
