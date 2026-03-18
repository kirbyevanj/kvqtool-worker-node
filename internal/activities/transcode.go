package activities

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/kirbyevanj/kvqtool-kvq-models/types"
	"go.temporal.io/sdk/activity"
)

// X264Transcode encodes a local file using x264 via GStreamer.
// If start_time/end_time are set, ffmpeg pre-trims before encoding.
func (a *Activities) X264Transcode(ctx context.Context, input types.ActivityInput) (*types.ActivityOutput, error) {
	localPath := input.Params["local_path"]
	if localPath == "" {
		localPath = resolveUpstreamLocalPath(input)
	}
	if localPath == "" {
		return fail(input.NodeID, "no local_path"), nil
	}

	outputDir := filepath.Join(a.TmpDir, input.NodeID)
	os.MkdirAll(outputDir, 0o755)

	encodeInput, cleanup, err := trimmedInput(ctx, localPath, outputDir, input.Params)
	if err != nil {
		return fail(input.NodeID, fmt.Sprintf("trim: %s", err)), nil
	}
	defer cleanup()

	outputPath := filepath.Join(outputDir, "encoded.mp4")
	pipeline := buildX264Pipeline(encodeInput, outputPath, input.Params)
	a.Logger.Info("x264Transcode", "pipeline", pipeline)

	if err := runGstPipeline(ctx, pipeline); err != nil {
		return fail(input.NodeID, err.Error()), nil
	}

	activity.RecordHeartbeat(ctx, "encoded")
	return ok(input.NodeID, "", map[string]string{"local_path": outputPath}), nil
}

// X264RemoteTranscode uses a presigned S3 URL as ffmpeg input (seekable via HTTP range requests),
// and streams the fragmented MP4 output directly to S3 multipart upload. No temp files.
func (a *Activities) X264RemoteTranscode(ctx context.Context, input types.ActivityInput) (*types.ActivityOutput, error) {
	s3Key := input.Params["s3_key"]
	if s3Key == "" {
		s3Key = resolveUpstreamS3Key(input)
	}
	if s3Key == "" {
		return fail(input.NodeID, "no s3_key (select input resource)"), nil
	}

	outputName := input.Params["output_name"]
	if outputName == "" {
		outputName = "encoded.mp4"
	}
	uploadKey := fmt.Sprintf("projects/%s/media/%s-%s", input.ProjectID, input.NodeID, outputName)

	// Presigned URL allows ffmpeg to seek via HTTP range requests (required for MP4 moov atom).
	presignedURL, err := a.S3.PresignGet(ctx, s3Key, 2*time.Hour)
	if err != nil {
		return fail(input.NodeID, fmt.Sprintf("presign: %s", err)), nil
	}

	args := buildFFmpegX264StreamArgs(presignedURL, input.Params)
	a.Logger.Info("x264RemoteTranscode", "s3_key", s3Key)

	// ffmpeg reads from presigned URL (HTTP), writes fragmented MP4 to stdout.
	pr, pw := io.Pipe()
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	cmd.Stdout = pw

	if err := cmd.Start(); err != nil {
		pw.Close()
		pr.Close()
		return fail(input.NodeID, fmt.Sprintf("ffmpeg start: %s", err)), nil
	}

	go func() {
		err := cmd.Wait()
		pw.CloseWithError(err)
	}()
	activity.RecordHeartbeat(ctx, "transcode started")

	if err := a.S3.UploadStream(ctx, pr, uploadKey, "video/mp4"); err != nil {
		return fail(input.NodeID, fmt.Sprintf("upload stream: %s", err)), nil
	}
	activity.RecordHeartbeat(ctx, "uploaded")

	a.registerResource(ctx, input.ProjectID, outputName, uploadKey, "video/mp4")
	return ok(input.NodeID, uploadKey, map[string]string{"output_name": outputName}), nil
}

// buildFFmpegX264StreamArgs constructs ffmpeg args for URL→stdout x264 encoding.
// inputURL may be a presigned HTTP URL or a local file path.
// Output is fragmented MP4 written to pipe:1 (stdout).
func buildFFmpegX264StreamArgs(inputURL string, params map[string]string) []string {
	args := []string{"-y", "-i", inputURL}

	// Decode-based trim (works on non-seekable stdin, placed after -i).
	if params["start_time"] != "" {
		args = append(args, "-ss", params["start_time"])
	}
	if params["end_time"] != "" {
		args = append(args, "-to", params["end_time"])
	}

	// Video codec settings.
	args = append(args, "-c:v", "libx264")

	if bps := params["bitrate_kbps"]; bps != "" {
		args = append(args, "-b:v", bps+"k")
	} else if crf := params["crf"]; crf != "" {
		args = append(args, "-crf", crf)
	} else {
		args = append(args, "-crf", "23")
	}

	preset := params["preset"]
	if preset == "" {
		preset = "medium"
	}
	args = append(args, "-preset", preset)

	if gop := params["gop_length"]; gop != "" {
		args = append(args, "-g", gop)
	}
	if tune := params["tune"]; tune != "" {
		args = append(args, "-tune", tune)
	}
	if profile := params["profile"]; profile != "" {
		args = append(args, "-profile:v", profile)
	}

	// Scaling (scale filter).
	if w, h := params["scale_width"], params["scale_height"]; w != "" && h != "" {
		method := params["scale_method"]
		if method == "" {
			method = "lanczos"
		}
		args = append(args, "-vf", fmt.Sprintf("scale=%s:%s:flags=%s", w, h, method))
	}

	// Audio passthrough.
	args = append(args, "-c:a", "aac")

	// Fragmented MP4 enables streaming output to stdout.
	args = append(args, "-movflags", "frag_keyframe+empty_moov+default_base_moof")
	args = append(args, "-f", "mp4", "pipe:1")

	return args
}
