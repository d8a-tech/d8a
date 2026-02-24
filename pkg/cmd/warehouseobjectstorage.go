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
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"
	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/blob/s3blob"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// createWarehouseBucket initializes a Go CDK bucket for warehouse operations.
// Supports S3 and GCS providers based on warehouse-object-storage-type flag.
// Applies prefix using blob.PrefixedBucket if warehouse-object-storage-prefix is set.
func createWarehouseBucket(ctx context.Context, cmd *cli.Command) (*blob.Bucket, func() error, error) {
	storageType := strings.ToLower(cmd.String(ObjectStorageFlagsSpec.Warehouse.Type.Name))
	logrus.Infof("Creating warehouse bucket with provider: %s", storageType)

	var bucket *blob.Bucket
	var cleanup func() error
	var err error

	switch storageType {
	case "s3":
		bucket, cleanup, err = createWarehouseS3Bucket(ctx, cmd)
	case "gcs":
		bucket, cleanup, err = createWarehouseGCSBucket(ctx, cmd)
	default:
		return nil, nil, fmt.Errorf("unsupported warehouse object storage type: %s", storageType)
	}

	if err != nil {
		return nil, nil, err
	}

	// Apply prefix if configured
	prefix := strings.TrimSpace(cmd.String(ObjectStorageFlagsSpec.Warehouse.Prefix.Name))
	if prefix != "" {
		bucket = blob.PrefixedBucket(bucket, prefix)
	}

	return bucket, cleanup, nil
}

func createWarehouseS3Bucket(ctx context.Context, c *cli.Command) (*blob.Bucket, func() error, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				c.String(ObjectStorageFlagsSpec.Warehouse.S3AccessKey.Name),
				c.String(ObjectStorageFlagsSpec.Warehouse.S3SecretKey.Name),
				"",
			),
		),
		config.WithRegion(c.String(ObjectStorageFlagsSpec.Warehouse.S3Region.Name)),
	)

	if err != nil {
		return nil, nil, fmt.Errorf("load aws config: %w", err)
	}

	// Create S3 client with MinIO endpoint
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(
			fmt.Sprintf(
				"%s://%s:%d",
				c.String(ObjectStorageFlagsSpec.Warehouse.S3Protocol.Name),
				c.String(ObjectStorageFlagsSpec.Warehouse.S3Host.Name),
				c.Int(ObjectStorageFlagsSpec.Warehouse.S3Port.Name),
			),
		)
		o.UsePathStyle = true
	})

	// Create bucket first
	bucketName := c.String(ObjectStorageFlagsSpec.Warehouse.S3Bucket.Name)
	if bucketName == "" {
		return nil, nil, fmt.Errorf("s3 bucket name is required: set %s", ObjectStorageFlagsSpec.Warehouse.S3Bucket.Name)
	}
	if c.Bool(ObjectStorageFlagsSpec.Warehouse.S3CreateBucket.Name) {
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

// createWarehouseGCSBucket initializes a Go CDK bucket backed by Google Cloud Storage.
// Authentication order:
// - If WAREHOUSE_OBJECT_STORAGE_GCS_CREDS_JSON is set (raw or base64), use it.
// - Else fall back to ADC (env var GOOGLE_APPLICATION_CREDENTIALS, GCE metadata, gcloud ADC, etc.).
// nolint:funlen // straightforward setup
func createWarehouseGCSBucket(
	ctx context.Context, c *cli.Command,
) (*blob.Bucket, func() error, error) {
	bucketName := c.String(ObjectStorageFlagsSpec.Warehouse.GCSBucket.Name)
	if bucketName == "" {
		return nil, nil, fmt.Errorf("gcs bucket name is required: set %s", ObjectStorageFlagsSpec.Warehouse.GCSBucket.Name)
	}

	// Resolve credentials
	var httpClient *gcp.HTTPClient
	var err error
	var ts oauth2.TokenSource

	credsJSON := strings.TrimSpace(c.String(ObjectStorageFlagsSpec.Warehouse.GCSCredsJSON.Name))
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
