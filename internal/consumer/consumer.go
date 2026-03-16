package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/kirbyevanj/kvqtool-kvq-models/messages"
	"github.com/valkey-io/valkey-go"
)

type JobHandler func(ctx context.Context, msg messages.JobMessage) error

type Consumer struct {
	client  valkey.Client
	logger  *slog.Logger
	handler JobHandler
}

func New(addr string, handler JobHandler, logger *slog.Logger) (*Consumer, error) {
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{addr},
	})
	if err != nil {
		return nil, fmt.Errorf("valkey connect: %w", err)
	}

	logger.Info("consumer connected to valkey", "addr", addr)
	return &Consumer{client: client, logger: logger, handler: handler}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	c.logger.Info("consumer started, waiting for jobs")

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("consumer shutting down")
			return ctx.Err()
		default:
		}

		result, err := c.blockPop(ctx)
		if err != nil {
			c.logger.Error("block pop failed", "err", err)
			continue
		}
		if result == "" {
			continue
		}

		var msg messages.JobMessage
		if err := json.Unmarshal([]byte(result), &msg); err != nil {
			c.logger.Error("unmarshal job message", "err", err)
			continue
		}

		c.logger.Info("received job", "job_id", msg.JobID, "workflow_id", msg.WorkflowID)

		if err := c.handler(ctx, msg); err != nil {
			c.logger.Error("job failed", "job_id", msg.JobID, "err", err)
			c.publishProgress(ctx, msg.JobID.String(), messages.JobProgress{
				JobID:   msg.JobID,
				Status:  "failed",
				Message: err.Error(),
			})
		}
	}
}

func (c *Consumer) blockPop(ctx context.Context) (string, error) {
	cmd := c.client.B().Brpop().Key(messages.JobQueueKey).Timeout(5).Build()
	resp, err := c.client.Do(ctx, cmd).AsStrSlice()
	if err != nil {
		if valkey.IsValkeyNil(err) {
			return "", nil
		}
		return "", err
	}
	if len(resp) < 2 {
		return "", nil
	}
	return resp[1], nil
}

func (c *Consumer) publishProgress(ctx context.Context, jobID string, progress messages.JobProgress) {
	data, err := json.Marshal(progress)
	if err != nil {
		return
	}
	channel := fmt.Sprintf(messages.JobProgressPrefix, jobID)
	_ = c.client.Do(ctx, c.client.B().Publish().Channel(channel).Message(string(data)).Build()).Error()
}

func (c *Consumer) PublishProgress(ctx context.Context, progress messages.JobProgress) {
	c.publishProgress(ctx, progress.JobID.String(), progress)
}

func (c *Consumer) Close() {
	c.client.Close()
}
