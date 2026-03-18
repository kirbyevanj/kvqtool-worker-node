package main

import (
	"log"
	"log/slog"
	"os"

	"github.com/kirbyevanj/kvqtool-kvq-models/types"
	"github.com/kirbyevanj/kvqtool-worker-node/internal/activities"
	"github.com/kirbyevanj/kvqtool-worker-node/internal/config"
	"github.com/kirbyevanj/kvqtool-worker-node/internal/s3client"
	wf "github.com/kirbyevanj/kvqtool-worker-node/internal/workflow"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	cfg := config.Load()
	logger := initLogger(cfg)

	s3, err := s3client.New(cfg.S3Endpoint, cfg.S3Bucket, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Region, logger)
	if err != nil {
		log.Fatalf("s3: %v", err)
	}

	tc, err := client.Dial(client.Options{
		HostPort:  cfg.TemporalHost,
		Namespace: types.TemporalNamespace,
	})
	if err != nil {
		log.Fatalf("temporal: %v", err)
	}
	defer tc.Close()

	w := worker.New(tc, types.TemporalTaskQueue, worker.Options{
		EnableSessionWorker: true,
	})

	w.RegisterWorkflow(wf.InterpreterWorkflow)

	acts := &activities.Activities{
		S3:            s3,
		TmpDir:        cfg.TempDir,
		ApiURL:        cfg.ApiURL,
		TransnetModel: cfg.TransnetModel,
		Logger:        logger,
	}

	// Storage
	w.RegisterActivityWithOptions(acts.ResourceDownload, actOpts(types.ActivityResDownload))
	w.RegisterActivityWithOptions(acts.ResourceUpload, actOpts(types.ActivityResUpload))

	// Local processing (session-pinned)
	w.RegisterActivityWithOptions(acts.X264Transcode, actOpts(types.ActivityX264Transcode))
	w.RegisterActivityWithOptions(acts.FileMetricAnalysis, actOpts(types.ActivityFileMetricAnalysis))

	// Remote processing
	w.RegisterActivityWithOptions(acts.X264RemoteTranscode, actOpts(types.ActivityX264RemoteTranscode))
	w.RegisterActivityWithOptions(acts.RemoteFileMetricAnalysis, actOpts(types.ActivityRemoteFileMetric))

	// Scene detection
	w.RegisterActivityWithOptions(acts.SceneCut, actOpts(types.ActivitySceneCut))
	w.RegisterActivityWithOptions(acts.RemoteSceneCut, actOpts(types.ActivityRemoteSceneCut))
	w.RegisterActivityWithOptions(acts.TransnetV2SceneCut, actOpts(types.ActivityTransnetV2SceneCut))

	// Segmentation
	w.RegisterActivityWithOptions(acts.SegmentMedia, actOpts(types.ActivitySegmentMedia))
	w.RegisterActivityWithOptions(acts.RemoteSegmentMedia, actOpts(types.ActivityRemoteSegmentMedia))

	// Utility
	w.RegisterActivityWithOptions(acts.GenerateReport, actOpts(types.ActivityGenerateReport))
	w.RegisterActivityWithOptions(acts.FragmentedMP4Repackage, actOpts(types.ActivityFMP4Repackage))

	// Interpreter support
	w.RegisterActivityWithOptions(acts.FetchWorkflowDAG, actOpts(types.ActivityFetchWorkflowDAG))

	logger.Info("worker started", "taskQueue", types.TemporalTaskQueue, "temporal", cfg.TemporalHost)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("worker: %v", err)
	}
}

func actOpts(name string) activity.RegisterOptions {
	return activity.RegisterOptions{Name: name}
}

func initLogger(cfg *config.Config) *slog.Logger {
	opts := &slog.HandlerOptions{Level: cfg.LogLevel}
	if cfg.IsDev() {
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}
