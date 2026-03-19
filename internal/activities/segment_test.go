package activities

import (
	"testing"
)

func TestBuildSegmentFileFromDuration_DefaultSegmentDuration(t *testing.T) {
	scf := buildSegmentFileFromDuration("video.mp4", 30.0, map[string]string{})
	// 30s / 10s default = 3 segments
	if len(scf.Segments) != 3 {
		t.Fatalf("expected 3 segments, got %d", len(scf.Segments))
	}
}

func TestBuildSegmentFileFromDuration_CustomSegmentDuration(t *testing.T) {
	scf := buildSegmentFileFromDuration("video.mp4", 60.0, map[string]string{"segment_duration": "30"})
	if len(scf.Segments) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(scf.Segments))
	}
}

func TestBuildSegmentFileFromDuration_SetsSourceAndVersion(t *testing.T) {
	scf := buildSegmentFileFromDuration("s3://my-bucket/video.mp4", 10.0, map[string]string{})
	if scf.Source != "s3://my-bucket/video.mp4" {
		t.Errorf("source=%q", scf.Source)
	}
	if scf.Version != "0.1" {
		t.Errorf("version=%q", scf.Version)
	}
}

func TestBuildSegmentFileFromDuration_SegmentIndexesAreSequential(t *testing.T) {
	scf := buildSegmentFileFromDuration("v.mp4", 30.0, map[string]string{})
	for i, seg := range scf.Segments {
		if seg.Index != i {
			t.Errorf("segment[%d].Index=%d", i, seg.Index)
		}
	}
}

func TestBuildSegmentFileFromDuration_LastSegmentClampedToDuration(t *testing.T) {
	// 25s video, 10s segments → 3 segments: [0-10],[10-20],[20-25]
	scf := buildSegmentFileFromDuration("v.mp4", 25.0, map[string]string{})
	if len(scf.Segments) != 3 {
		t.Fatalf("expected 3 segments, got %d", len(scf.Segments))
	}
	last := scf.Segments[2]
	if last.EndTime != "00:00:25.000" {
		t.Errorf("last EndTime=%q, want 00:00:25.000", last.EndTime)
	}
}

func TestBuildSegmentFileFromDuration_SingleSegmentWhenDurationLessThanSegSize(t *testing.T) {
	scf := buildSegmentFileFromDuration("v.mp4", 5.0, map[string]string{})
	if len(scf.Segments) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(scf.Segments))
	}
}

func TestBuildSegmentFileFromDuration_ZeroDurationEmptySegments(t *testing.T) {
	scf := buildSegmentFileFromDuration("v.mp4", 0.0, map[string]string{})
	if len(scf.Segments) != 0 {
		t.Fatalf("expected 0 segments, got %d", len(scf.Segments))
	}
}

func TestBuildSegmentFileFromDuration_InvalidSegmentDurationIgnored(t *testing.T) {
	// Invalid value falls back to default 10s
	scf := buildSegmentFileFromDuration("v.mp4", 30.0, map[string]string{"segment_duration": "invalid"})
	if len(scf.Segments) != 3 {
		t.Fatalf("expected 3 segments (default 10s), got %d", len(scf.Segments))
	}
}

func TestBuildSegmentFileFromDuration_ZeroSegmentDurationIgnored(t *testing.T) {
	// Zero or negative falls back to default
	scf := buildSegmentFileFromDuration("v.mp4", 30.0, map[string]string{"segment_duration": "0"})
	if len(scf.Segments) != 3 {
		t.Fatalf("expected 3 segments (default 10s), got %d", len(scf.Segments))
	}
}
