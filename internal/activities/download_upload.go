package activities

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/kirbyevanj/kvqtool-kvq-models/types"
	"go.temporal.io/sdk/activity"
)

func (a *Activities) ResourceDownload(ctx context.Context, input types.ActivityInput) (*types.ActivityOutput, error) {
	s3Key := input.Params["s3_key"]
	if s3Key == "" {
		s3Key = resolveUpstreamS3Key(input)
	}
	if s3Key == "" {
		return fail(input.NodeID, "no s3_key in params or upstream (set resource_id in node config)"), nil
	}

	localPath := filepath.Join(a.TmpDir, input.NodeID, "input.mp4")
	if err := a.S3.Download(ctx, s3Key, localPath); err != nil {
		return fail(input.NodeID, err.Error()), nil
	}

	activity.RecordHeartbeat(ctx, "downloaded")
	return ok(input.NodeID, s3Key, map[string]string{"local_path": localPath}), nil
}

func (a *Activities) ResourceUpload(ctx context.Context, input types.ActivityInput) (*types.ActivityOutput, error) {
	localPath := input.Params["local_path"]
	if localPath == "" {
		localPath = resolveUpstreamLocalPath(input)
	}
	outputName := input.Params["output_name"]
	if outputName == "" {
		outputName = "output.mp4"
	}
	projectID := input.ProjectID
	s3Key := fmt.Sprintf("projects/%s/media/%s-%s", projectID, input.NodeID, outputName)

	contentType := "video/mp4"
	if strings.HasSuffix(outputName, ".json") {
		contentType = "application/json"
	}

	if err := a.S3.Upload(ctx, localPath, s3Key, contentType); err != nil {
		return fail(input.NodeID, err.Error()), nil
	}

	a.registerResource(ctx, projectID, outputName, s3Key, contentType)

	activity.RecordHeartbeat(ctx, "uploaded")
	return ok(input.NodeID, s3Key, map[string]string{"output_name": outputName}), nil
}
