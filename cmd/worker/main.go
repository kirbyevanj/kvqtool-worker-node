package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-gst/go-gst/gst"
	"github.com/kirbyevanj/kvqtool-kvq-models/messages"
	"github.com/kirbyevanj/kvqtool-worker-node/internal/compiler"
	"github.com/kirbyevanj/kvqtool-worker-node/internal/config"
	"github.com/kirbyevanj/kvqtool-worker-node/internal/consumer"
	"github.com/kirbyevanj/kvqtool-worker-node/internal/executor"
	"github.com/kirbyevanj/kvqtool-worker-node/internal/s3client"
)

func main() {
	gst.Init(nil)

	cfg := config.Load()
	logger := initLogger(cfg)

	s3, err := s3client.New(cfg.S3Endpoint, cfg.S3Bucket, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Region, logger)
	if err != nil {
		logger.Error("s3 init failed", "err", err)
		os.Exit(1)
	}

	exec := executor.New(s3, cfg.TempDir, logger)

	handler := func(ctx context.Context, msg messages.JobMessage) error {
		return processJob(ctx, exec, msg, logger)
	}

	cons, err := consumer.New(cfg.ValkeyURL, handler, logger)
	if err != nil {
		logger.Error("consumer init failed", "err", err)
		os.Exit(1)
	}
	defer cons.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger.Info("worker-node started")
	if err := cons.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Error("consumer error", "err", err)
		os.Exit(1)
	}
	logger.Info("worker-node stopped")
}

func processJob(ctx context.Context, exec *executor.Executor, msg messages.JobMessage, logger *slog.Logger) error {
	nodes, err := compiler.Compile(msg.DAGJson)
	if err != nil {
		return err
	}

	logger.Info("compiled DAG", "job_id", msg.JobID, "nodes", len(nodes))

	// Determine job type from nodes
	hasEncode := false
	for _, n := range nodes {
		if n.NodeType == "x264Encode" {
			hasEncode = true
		}
	}

	if hasEncode {
		onProgress := func(ctx context.Context, p messages.JobProgress) {
			logger.Debug("progress", "job_id", p.JobID, "pct", p.ProgressPct)
		}
		outputKey, err := exec.RunEncode(ctx, msg, nodes, onProgress)
		if err != nil {
			return err
		}
		logger.Info("encode complete", "job_id", msg.JobID, "output", outputKey)
	}

	return nil
}

func initLogger(cfg *config.Config) *slog.Logger {
	opts := &slog.HandlerOptions{Level: cfg.LogLevel}
	if cfg.IsDev() {
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}
