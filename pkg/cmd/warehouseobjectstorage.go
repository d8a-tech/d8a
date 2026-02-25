package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"
	"gocloud.dev/blob"
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
	return createS3BucketWithFlags(ctx, c, &ObjectStorageFlagsSpec.Warehouse)
}

// createWarehouseGCSBucket initializes a Go CDK bucket backed by Google Cloud Storage.
// Authentication order:
// - If WAREHOUSE_OBJECT_STORAGE_GCS_CREDS_JSON is set (raw or base64), use it.
// - Else fall back to ADC (env var GOOGLE_APPLICATION_CREDENTIALS, GCE metadata, gcloud ADC, etc.).
// nolint:funlen // straightforward setup
func createWarehouseGCSBucket(
	ctx context.Context, c *cli.Command,
) (*blob.Bucket, func() error, error) {
	return createGCSBucketWithFlags(ctx, c, &ObjectStorageFlagsSpec.Warehouse)
}
