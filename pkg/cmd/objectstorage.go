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
	switch strings.ToLower(c.String(objectStorageTypeFlag.Name)) {
	case "s3":
		return createS3Bucket(ctx, c)
	case "gcs":
		return createGCSBucket(ctx, c)
	default:
		return nil, nil, fmt.Errorf("unsupported object storage type: %s", c.String(objectStorageTypeFlag.Name))
	}
}

func createS3Bucket(ctx context.Context, c *cli.Command) (*blob.Bucket, func() error, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				c.String(objectStorageS3AccessKeyFlag.Name),
				c.String(objectStorageS3SecretKeyFlag.Name),
				"",
			),
		),
		config.WithRegion(c.String(objectStorageS3RegionFlag.Name)),
	)

	if err != nil {
		return nil, nil, fmt.Errorf("load aws config: %w", err)
	}

	// Create S3 client with MinIO endpoint
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(
			fmt.Sprintf(
				"%s://%s:%d",
				c.String(objectStorageS3ProtocolFlag.Name),
				c.String(objectStorageS3HostFlag.Name),
				c.Int(objectStorageS3PortFlag.Name),
			),
		)
		o.UsePathStyle = true
	})

	// Create bucket first
	bucketName := c.String(objectStorageS3BucketFlag.Name)
	if bucketName == "" {
		return nil, nil, fmt.Errorf("s3 bucket name is required: set %s", objectStorageS3BucketFlag.Name)
	}
	if c.Bool(objectStorageS3CreateBucketFlag.Name) {
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

	// Create bucket using Go CDK
	bucket, err := s3blob.OpenBucketV2(ctx, s3Client, bucketName, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("open s3 bucket: %w", err)
	}
	return bucket, bucket.Close, nil
}

// createGCSBucket initializes a Go CDK bucket backed by Google Cloud Storage.
// Authentication order:
// - If OBJECT_STORAGE_GCS_CREDS_JSON is set (raw or base64), use it.
// - Else fall back to ADC (env var GOOGLE_APPLICATION_CREDENTIALS, GCE metadata, gcloud ADC, etc.).
// nolint:funlen // straightforward setup
func createGCSBucket(
	ctx context.Context, c *cli.Command,
) (*blob.Bucket, func() error, error) {
	bucketName := c.String(objectStorageGCSBucketFlag.Name)
	if bucketName == "" {
		return nil, nil, fmt.Errorf("gcs bucket name is required: set %s", objectStorageGCSBucketFlag.Name)
	}

	// Resolve credentials
	var httpClient *gcp.HTTPClient
	var err error
	var ts oauth2.TokenSource

	credsJSON := strings.TrimSpace(c.String(objectStorageGCSCredsJSONFlag.Name))
	if credsJSON != "" {
		// Support base64-encoded JSON for convenience
		raw := []byte(credsJSON)
		if decoded, decErr := base64.StdEncoding.DecodeString(credsJSON); decErr == nil {
			raw = decoded
		}
		// Build token source from JSON
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
	// Open Go CDK bucket
	bkt, err := gcsblob.OpenBucket(ctx, httpClient, bucketName, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("open gcs bucket: %w", err)
	}
	return bkt, bkt.Close, nil
}
