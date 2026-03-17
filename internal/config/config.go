package config

import (
	"log/slog"
	"os"
	"strings"
)

type Config struct {
	Env          string
	TemporalHost string
	S3Endpoint   string
	S3Bucket     string
	S3AccessKey  string
	S3SecretKey  string
	S3Region     string
	TempDir      string
	LogLevel     slog.Level
}

func Load() *Config {
	return &Config{
		Env:          envOr("KVQ_ENV", "development"),
		TemporalHost: envOr("KVQ_TEMPORAL_HOST", "localhost:7233"),
		S3Endpoint:   envOr("KVQ_S3_ENDPOINT", "http://localhost:9000"),
		S3Bucket:     envOr("KVQ_S3_BUCKET", "kvq-bucket"),
		S3AccessKey:  envOr("KVQ_S3_ACCESS_KEY", "kvqminio"),
		S3SecretKey:  envOr("KVQ_S3_SECRET_KEY", "kvqminiodev"),
		S3Region:     envOr("KVQ_S3_REGION", "us-east-1"),
		TempDir:      envOr("KVQ_TEMP_DIR", "/tmp/kvqtool"),
		LogLevel:     parseLogLevel(envOr("KVQ_LOG_LEVEL", "debug")),
	}
}

func (c *Config) IsDev() bool { return c.Env == "development" }

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parseLogLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
