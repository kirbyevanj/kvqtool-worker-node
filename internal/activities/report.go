package activities

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/kirbyevanj/kvqtool-kvq-models/types"
	"go.temporal.io/sdk/activity"
)

func (a *Activities) GenerateReport(ctx context.Context, input types.ActivityInput) (*types.ActivityOutput, error) {
	outputDir := filepath.Join(a.TmpDir, input.NodeID)
	os.MkdirAll(outputDir, 0o755)
	reportPath := filepath.Join(outputDir, "report.json")

	report := fmt.Sprintf(`{"header":{"version":"0.1","type":"workflow_report","software":"kvqtool-web","node_id":"%s"}}`, input.NodeID)
	os.WriteFile(reportPath, []byte(report), 0o644)

	activity.RecordHeartbeat(ctx, "report generated")
	return ok(input.NodeID, "", map[string]string{"local_path": reportPath}), nil
}

func (a *Activities) FragmentedMP4Repackage(ctx context.Context, input types.ActivityInput) (*types.ActivityOutput, error) {
	localPath := input.Params["local_path"]
	if localPath == "" {
		localPath = resolveUpstreamLocalPath(input)
	}

	outputDir := filepath.Join(a.TmpDir, input.NodeID)
	os.MkdirAll(outputDir, 0o755)
	outputPath := filepath.Join(outputDir, "fragmented.mp4")

	cmd := exec.CommandContext(ctx, "ffmpeg", "-i", localPath,
		"-c", "copy", "-movflags", "frag_keyframe+empty_moov+default_base_moof",
		outputPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fail(input.NodeID, fmt.Sprintf("ffmpeg fmp4: %s: %s", err, string(out))), nil
	}

	activity.RecordHeartbeat(ctx, "repackaged")
	return ok(input.NodeID, "", map[string]string{"local_path": outputPath}), nil
}
