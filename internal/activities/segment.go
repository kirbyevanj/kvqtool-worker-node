package activities

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/kirbyevanj/kvqtool-kvq-models/types"
	"go.temporal.io/sdk/activity"
)

// SegmentMedia generates a SceneCutFile with fixed-duration segments (no physical splitting).
func (a *Activities) SegmentMedia(ctx context.Context, input types.ActivityInput) (*types.ActivityOutput, error) {
	localPath := input.Params["local_path"]
	if localPath == "" {
		localPath = resolveUpstreamLocalPath(input)
	}
	if localPath == "" {
		return fail(input.NodeID, "no local_path"), nil
	}

	scf, err := buildSegmentFile(ctx, localPath, input.Params)
	if err != nil {
		return fail(input.NodeID, err.Error()), nil
	}

	activity.RecordHeartbeat(ctx, "segmentation complete")
	return okJSON(input.NodeID, "", scf), nil
}

// RemoteSegmentMedia streams from S3 → ffprobe stdin to get duration (no temp file).
// Segments are computed in-memory and the JSON result is uploaded directly.
func (a *Activities) RemoteSegmentMedia(ctx context.Context, input types.ActivityInput) (*types.ActivityOutput, error) {
	s3Key := input.Params["s3_key"]
	if s3Key == "" {
		s3Key = resolveUpstreamS3Key(input)
	}
	if s3Key == "" {
		return fail(input.NodeID, "no s3_key"), nil
	}

	r, err := a.S3.GetReader(ctx, s3Key)
	if err != nil {
		return fail(input.NodeID, fmt.Sprintf("s3 open: %s", err)), nil
	}
	defer r.Close()

	duration, err := probeDurationFromReader(ctx, r)
	if err != nil {
		return fail(input.NodeID, fmt.Sprintf("probe duration: %s", err)), nil
	}
	activity.RecordHeartbeat(ctx, "duration probed")

	scf := buildSegmentFileFromDuration(s3Key, duration, input.Params)
	scfBytes, _ := json.Marshal(scf)

	uploadKey := fmt.Sprintf("projects/%s/media/%s-segments.json", input.ProjectID, input.NodeID)
	if err := a.S3.UploadStream(ctx, strings.NewReader(string(scfBytes)), uploadKey, "application/json"); err != nil {
		return fail(input.NodeID, fmt.Sprintf("upload: %s", err)), nil
	}

	a.registerResource(ctx, input.ProjectID, "segments.json", uploadKey, "application/json")
	activity.RecordHeartbeat(ctx, "segmentation complete")
	return okJSON(input.NodeID, uploadKey, scf), nil
}

func buildSegmentFile(ctx context.Context, localPath string, params map[string]string) (*types.SceneCutFile, error) {
	duration, err := probeDuration(ctx, localPath)
	if err != nil {
		return nil, fmt.Errorf("probe duration: %w", err)
	}
	return buildSegmentFileFromDuration(localPath, duration, params), nil
}

// buildSegmentFileFromDuration creates a SceneCutFile given a known duration.
func buildSegmentFileFromDuration(source string, duration float64, params map[string]string) *types.SceneCutFile {
	segDur := 10.0
	if s := params["segment_duration"]; s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v > 0 {
			segDur = v
		}
	}

	scf := &types.SceneCutFile{
		Version: "0.1",
		Source:  source,
	}

	idx := 0
	for start := 0.0; start < duration; start += segDur {
		end := start + segDur
		if end > duration {
			end = duration
		}
		scf.Segments = append(scf.Segments, types.Segment{
			Index:     idx,
			StartTime: formatTimestamp(start),
			EndTime:   formatTimestamp(end),
		})
		idx++
	}
	return scf
}

