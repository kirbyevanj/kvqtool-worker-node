package activities

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"

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

// X264RemoteTranscode streams from S3 → ffmpeg stdin → ffmpeg stdout → S3 multipart upload.
// No temp file is written to disk. Fragmented MP4 output enables streaming to S3.
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

	// Build ffmpeg args for x264 encoding from stdin to stdout.
	// Trim params placed after -i so they work on non-seekable stdin.
	args := buildFFmpegX264StreamArgs(input.Params)
	a.Logger.Info("x264RemoteTranscode stream", "args", args)

	// Connect: S3 reader → ffmpeg stdin → ffmpeg stdout → S3 multipart upload.
	s3Reader, err := a.S3.GetReader(ctx, s3Key)
	if err != nil {
		return fail(input.NodeID, fmt.Sprintf("s3 open: %s", err)), nil
	}
	defer s3Reader.Close()

	pr, pw := io.Pipe()
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	cmd.Stdin = s3Reader
	cmd.Stdout = pw

	if err := cmd.Start(); err != nil {
		pw.Close()
		pr.Close()
		return fail(input.NodeID, fmt.Sprintf("ffmpeg start: %s", err)), nil
	}

	// Close the write-end of the pipe when ffmpeg exits (success or error).
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

// buildFFmpegX264StreamArgs constructs ffmpeg args for stdin→stdout x264 encoding.
// Input is read from pipe:0; output is fragmented MP4 written to pipe:1.
func buildFFmpegX264StreamArgs(params map[string]string) []string {
	args := []string{"-y", "-i", "pipe:0"}

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
