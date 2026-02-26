package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/urfave/cli/v3"
	"gocloud.dev/blob"
)

func createWarehouseCDKBucket(ctx context.Context, storageType string, cmd *cli.Command) (*blob.Bucket, error) {

	var bucket *blob.Bucket
	var cleanup func() error
	var err error

	switch storageType {
	case "s3":
		bucket, cleanup, err = createWarehouseS3Bucket(ctx, cmd)
	case "gcs":
		bucket, cleanup, err = createWarehouseGCSBucket(ctx, cmd)
	default:
		return nil, fmt.Errorf("unsupported warehouse object storage type: %s", storageType)
	}

	if err != nil {
		return nil, err
	}

	_ = cleanup

	prefix := strings.TrimSpace(cmd.String(objectStorageFlagsSpec.Warehouse.Prefix.Name))
	if prefix != "" {
		bucket = blob.PrefixedBucket(bucket, prefix)
	}

	return bucket, nil
}

func createWarehouseS3Bucket(ctx context.Context, c *cli.Command) (*blob.Bucket, func() error, error) {
	return createS3BucketWithFlags(ctx, c, &objectStorageFlagsSpec.Warehouse)
}

// Authentication order:
// - If WAREHOUSE_FILES_GCS_CREDS_JSON is set (raw or base64), use it.
// - Else fall back to ADC (env var GOOGLE_APPLICATION_CREDENTIALS, GCE metadata, gcloud ADC, etc.).
// nolint:funlen // straightforward setup
func createWarehouseGCSBucket(
	ctx context.Context, c *cli.Command,
) (*blob.Bucket, func() error, error) {
	return createGCSBucketWithFlags(ctx, c, &objectStorageFlagsSpec.Warehouse)
}
