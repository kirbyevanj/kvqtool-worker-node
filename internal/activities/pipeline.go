package activities

import "fmt"

// buildX264Pipeline constructs a GStreamer pipeline string for x264 encoding.
// Timestamp slicing is handled upstream by ffmpegTrim before this is called.
func buildX264Pipeline(inputPath, outputPath string, params map[string]string) string {
	src := fmt.Sprintf("filesrc location=%s ! decodebin", inputPath)
	scaleChain := buildScaleChain(params)

	pass := "quant"
	encProps := ""
	if bps, ok := params["bitrate_kbps"]; ok && bps != "" {
		pass = "cbr"
		encProps += " bitrate=" + bps
	}
	if crf, ok := params["crf"]; ok && crf != "" {
		encProps += " quantizer=" + crf
	}
	preset := params["preset"]
	if preset == "" {
		preset = "medium"
	}
	encProps += " speed-preset=" + preset
	if gop, ok := params["gop_length"]; ok && gop != "" {
		encProps += " key-int-max=" + gop
	}
	if tune, ok := params["tune"]; ok && tune != "" {
		encProps += " tune=" + tune
	}
	profile := params["profile"]
	if profile == "" {
		profile = "high"
	}

	return fmt.Sprintf("%s ! videoconvert%s ! x264enc pass=%s%s ! video/x-h264,profile=%s ! mp4mux ! filesink location=%s",
		src, scaleChain, pass, encProps, profile, outputPath)
}

// hasTrimParams returns true if start_time or end_time are set.
func hasTrimParams(params map[string]string) bool {
	return params["start_time"] != "" || params["end_time"] != ""
}

func buildScaleChain(params map[string]string) string {
	scaleW := params["scale_width"]
	scaleH := params["scale_height"]
	scaleMethod := params["scale_method"]
	if scaleW != "" && scaleH != "" && scaleMethod != "" {
		return fmt.Sprintf(" ! videoscale method=%s ! video/x-raw,width=%s,height=%s", scaleMethod, scaleW, scaleH)
	}
	if scaleW != "" && scaleH != "" {
		return fmt.Sprintf(" ! videoscale ! video/x-raw,width=%s,height=%s", scaleW, scaleH)
	}
	return ""
}

// buildMetricPipeline constructs a GStreamer pipeline for VMAF/SSIM/PSNR analysis.
// Timestamp slicing must be handled upstream via ffmpegTrim before calling this.
func buildMetricPipeline(refPath, distPath string, params map[string]string) string {
	metrics := "do-vmaf=true do-ssim=true do-psnr=true"
	if params["vmaf"] == "false" {
		metrics = "do-vmaf=false"
	}
	return fmt.Sprintf(
		"filesrc location=%s ! decodebin ! videoconvert ! video/x-raw,format=I420 ! iqa.sink_0 "+
			"filesrc location=%s ! decodebin ! videoconvert ! video/x-raw,format=I420 ! iqa.sink_1 "+
			"iqa name=iqa %s ! fakesink",
		refPath, distPath, metrics,
	)
}

// buildSceneCutPipeline constructs a GStreamer pipeline for scene change detection.
func buildSceneCutPipeline(inputPath string) string {
	return fmt.Sprintf(
		"filesrc location=%s ! decodebin ! videoconvert ! videoanalyse ! fakesink",
		inputPath,
	)
}
