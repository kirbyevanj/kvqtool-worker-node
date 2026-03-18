package s3client

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Client struct {
	s3       *s3.Client
	uploader *manager.Uploader
	bucket   string
	logger   *slog.Logger
}

func New(endpoint, bucket, accessKey, secretKey, region string, logger *slog.Logger) (*Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	logger.Info("s3 client ready", "endpoint", endpoint, "bucket", bucket)
	return &Client{
		s3:       client,
		uploader: manager.NewUploader(client),
		bucket:   bucket,
		logger:   logger,
	}, nil
}

// Download fetches an S3 object to a local file. For streaming, use GetReader.
func (c *Client) Download(ctx context.Context, key, destPath string) error {
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	r, err := c.GetReader(ctx, key)
	if err != nil {
		return err
	}
	defer r.Close()

	f, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer f.Close()

	if _, err := io.Copy(f, r); err != nil {
		return fmt.Errorf("copy: %w", err)
	}
	c.logger.Debug("downloaded", "key", key, "dest", destPath)
	return nil
}

// GetReader returns the S3 object body as a streaming io.ReadCloser.
// The caller is responsible for closing the returned reader.
func (c *Client) GetReader(ctx context.Context, key string) (io.ReadCloser, error) {
	resp, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("get object %s: %w", key, err)
	}
	return resp.Body, nil
}

// Upload writes a local file to S3. For streaming (unknown size), use UploadStream.
func (c *Client) Upload(ctx context.Context, localPath, key, contentType string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", localPath, err)
	}
	defer f.Close()
	if err := c.UploadStream(ctx, f, key, contentType); err != nil {
		return err
	}
	c.logger.Debug("uploaded", "key", key, "src", localPath)
	return nil
}

// PresignGet returns a presigned GET URL for the given key valid for the specified duration.
// The URL can be passed directly to ffmpeg/ffprobe as an HTTP input, enabling HTTP range
// requests so the tool can seek within the stream (required for MP4 container parsing).
func (c *Client) PresignGet(ctx context.Context, key string, expiry time.Duration) (string, error) {
	presigner := s3.NewPresignClient(c.s3)
	req, err := presigner.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(expiry))
	if err != nil {
		return "", fmt.Errorf("presign %s: %w", key, err)
	}
	return req.URL, nil
}

// UploadStream uploads data from an io.Reader to S3 using multipart upload.
// No Content-Length is required; the manager handles chunking automatically.
func (c *Client) UploadStream(ctx context.Context, r io.Reader, key, contentType string) error {
	_, err := c.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		Body:        r,
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return fmt.Errorf("upload stream %s: %w", key, err)
	}
	c.logger.Debug("uploaded stream", "key", key)
	return nil
}
