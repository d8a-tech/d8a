package cmd

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	gcsstorage "cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/urfave/cli/v3"
	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/blob/s3blob"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

func createBucket(ctx context.Context, c *cli.Command) (*blob.Bucket, func() error, error) {
	switch strings.ToLower(c.String(ObjectStorageFlagsSpec.Queue.Type.Name)) {
	case "s3":
		return createS3Bucket(ctx, c)
	case "gcs":
		return createGCSBucket(ctx, c)
	default:
		return nil, nil, fmt.Errorf("unsupported object storage type: %s", c.String(ObjectStorageFlagsSpec.Queue.Type.Name))
	}
}

func createS3Bucket(ctx context.Context, c *cli.Command) (*blob.Bucket, func() error, error) {
	return createS3BucketWithFlags(ctx, c, &ObjectStorageFlagsSpec.Queue)
}

func createS3BucketWithFlags(
	ctx context.Context,
	c *cli.Command,
	flags *ObjectStorageFlagSet,
) (*blob.Bucket, func() error, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				c.String(flags.S3AccessKey.Name),
				c.String(flags.S3SecretKey.Name),
				"",
			),
		),
		config.WithRegion(c.String(flags.S3Region.Name)),
	)

	if err != nil {
		return nil, nil, fmt.Errorf("load aws config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(
			fmt.Sprintf(
				"%s://%s:%d",
				c.String(flags.S3Protocol.Name),
				c.String(flags.S3Host.Name),
				c.Int(flags.S3Port.Name),
			),
		)
		o.UsePathStyle = true
	})

	bucketName := c.String(flags.S3Bucket.Name)
	if bucketName == "" {
		return nil, nil, fmt.Errorf("s3 bucket name is required: set %s", flags.S3Bucket.Name)
	}
	if c.Bool(flags.S3CreateBucket.Name) {
		_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: &bucketName,
		})
		if err != nil {
			// Check if bucket already exists and handle gracefully
			var owned *types.BucketAlreadyOwnedByYou
			var exists *types.BucketAlreadyExists
			switch {
			case errors.As(err, &owned):
				// noop
			case errors.As(err, &exists):
				// noop
			default:
				return nil, nil, fmt.Errorf("create s3 bucket: %w", err)
			}
		}
	}

	bucket, err := s3blob.OpenBucketV2(ctx, s3Client, bucketName, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("open s3 bucket: %w", err)
	}
	return bucket, bucket.Close, nil
}

// Authentication order:
// - If QUEUE_OBJECT_STORAGE_GCS_CREDS_JSON is set (raw or base64), use it.
// - Else fall back to ADC (env var GOOGLE_APPLICATION_CREDENTIALS, GCE metadata, gcloud ADC, etc.).
// nolint:funlen // straightforward setup
func createGCSBucket(
	ctx context.Context, c *cli.Command,
) (*blob.Bucket, func() error, error) {
	return createGCSBucketWithFlags(ctx, c, &ObjectStorageFlagsSpec.Queue)
}

// Authentication order:
// - If GCS_CREDS_JSON is set (raw or base64), use it.
// - Else fall back to ADC (env var GOOGLE_APPLICATION_CREDENTIALS, GCE metadata, gcloud ADC, etc.).
// nolint:funlen // straightforward setup
func createGCSBucketWithFlags(
	ctx context.Context,
	c *cli.Command,
	flags *ObjectStorageFlagSet,
) (*blob.Bucket, func() error, error) {
	bucketName := c.String(flags.GCSBucket.Name)
	if bucketName == "" {
		return nil, nil, fmt.Errorf("gcs bucket name is required: set %s", flags.GCSBucket.Name)
	}

	var httpClient *gcp.HTTPClient
	var err error
	var ts oauth2.TokenSource

	credsJSON := strings.TrimSpace(c.String(flags.GCSCredsJSON.Name))
	if credsJSON != "" {
		// Support base64-encoded JSON for convenience
		raw := []byte(credsJSON)
		if decoded, decErr := base64.StdEncoding.DecodeString(credsJSON); decErr == nil {
			raw = decoded
		}
		googleCreds, credErr := google.CredentialsFromJSONWithType(
			ctx,
			raw,
			google.ServiceAccount,
			gcsstorage.ScopeFullControl,
		)
		if credErr != nil {
			return nil, nil, fmt.Errorf("parse provided gcs credentials json: %w", credErr)
		}
		ts = googleCreds.TokenSource
		httpClient, err = gcp.NewHTTPClient(gcp.DefaultTransport(), ts)
		if err != nil {
			return nil, nil, fmt.Errorf("create gcp http client: %w", err)
		}
	} else {
		// Application Default Credentials
		dc, derr := gcp.DefaultCredentials(ctx)
		if derr != nil {
			return nil, nil, fmt.Errorf("load default gcp credentials: %w", derr)
		}
		ts = gcp.CredentialsTokenSource(dc)
		httpClient, err = gcp.NewHTTPClient(gcp.DefaultTransport(), ts)
		if err != nil {
			return nil, nil, fmt.Errorf("create gcp http client: %w", err)
		}
	}

	// No bucket creation logic for GCS; bucket must already exist
	bkt, err := gcsblob.OpenBucket(ctx, httpClient, bucketName, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("open gcs bucket: %w", err)
	}
	return bkt, bkt.Close, nil
}
