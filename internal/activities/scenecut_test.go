package activities

import (
	"strings"
	"testing"
)

func TestFormatTimestamp_Zero(t *testing.T) {
	got := formatTimestamp(0)
	if got != "00:00:00.000" {
		t.Errorf("got %q", got)
	}
}

func TestFormatTimestamp_Seconds(t *testing.T) {
	got := formatTimestamp(5.5)
	if !strings.HasPrefix(got, "00:00:05") {
		t.Errorf("got %q", got)
	}
}

func TestFormatTimestamp_Minutes(t *testing.T) {
	got := formatTimestamp(90.0) // 1m30s
	if got != "00:01:30.000" {
		t.Errorf("got %q", got)
	}
}

func TestFormatTimestamp_Hours(t *testing.T) {
	got := formatTimestamp(3661.5) // 1h01m01.5s
	if !strings.HasPrefix(got, "01:01:01") {
		t.Errorf("got %q", got)
	}
}

func TestFormatTimestamp_SubsecondPrecision(t *testing.T) {
	got := formatTimestamp(1.123)
	if !strings.Contains(got, "1.123") {
		t.Errorf("got %q, expected millisecond precision", got)
	}
}

func TestFormatTimestamp_TwoHours(t *testing.T) {
	got := formatTimestamp(7200.0)
	if !strings.HasPrefix(got, "02:00:00") {
		t.Errorf("got %q", got)
	}
}
