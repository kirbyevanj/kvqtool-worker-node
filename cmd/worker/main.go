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
		S3:     s3,
		TmpDir: cfg.TempDir,
		Logger: logger,
	}
	w.RegisterActivityWithOptions(acts.ResourceDownload, activity_options(types.ActivityResDownload))
	w.RegisterActivityWithOptions(acts.ResourceUpload, activity_options(types.ActivityResUpload))
	w.RegisterActivityWithOptions(acts.GStreamerEncode, activity_options(types.ActivityGstEncode))
	w.RegisterActivityWithOptions(acts.GStreamerMetrics, activity_options(types.ActivityGstMetrics))
	w.RegisterActivityWithOptions(acts.SplitVideo, activity_options(types.ActivitySplitVideo))
	w.RegisterActivityWithOptions(acts.ConcatVideo, activity_options(types.ActivityConcatVideo))
	w.RegisterActivityWithOptions(acts.GenerateReport, activity_options(types.ActivityGenerateReport))
	w.RegisterActivityWithOptions(acts.FragmentedMP4Repackage, activity_options(types.ActivityFMP4Repackage))

	logger.Info("worker started", "taskQueue", types.TemporalTaskQueue, "temporal", cfg.TemporalHost)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("worker: %v", err)
	}
}

func activity_options(name string) activity.RegisterOptions { //nolint
	return activity.RegisterOptions{Name: name}
}

func initLogger(cfg *config.Config) *slog.Logger {
	opts := &slog.HandlerOptions{Level: cfg.LogLevel}
	if cfg.IsDev() {
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}
