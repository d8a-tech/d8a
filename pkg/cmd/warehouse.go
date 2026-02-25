package cmd

import (
	"context"
	"encoding/base64"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	whBigQuery "github.com/d8a-tech/d8a/pkg/warehouse/bigquery"
	whClickhouse "github.com/d8a-tech/d8a/pkg/warehouse/clickhouse"
	whFiles "github.com/d8a-tech/d8a/pkg/warehouse/files"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

const (
	storageTypeS3         = "s3"
	storageTypeGCS        = "gcs"
	storageTypeFilesystem = "filesystem"
)

func warehouseRegistry(ctx context.Context, cmd *cli.Command) warehouse.Registry {
	warehouseType := strings.ToLower(cmd.String(warehouseDriverFlag.Name))
	if warehouseType == "" {
		warehouseType = warehouseDriverFlag.Value
	}

	switch warehouseType {
	case "bigquery":
		return createBigQueryWarehouse(ctx, cmd)
	case "clickhouse":
		return createClickHouseWarehouse(ctx, cmd)
	case "files":
		return createFilesWarehouse(ctx, cmd)
	case "console", "":
		return warehouse.NewStaticBatchedDriverRegistry(
			ctx,
			warehouse.NewConsoleDriver(),
		)
	case "noop":
		return warehouse.NewStaticBatchedDriverRegistry(
			ctx,
			warehouse.NewNoopDriver(),
		)
	default:
		logrus.Fatalf("unsupported warehouse %s", warehouseType)
		return nil
	}
}

func createBigQueryWarehouse(ctx context.Context, cmd *cli.Command) warehouse.Registry {
	projectID := cmd.String(warehouseBigQueryProjectIDFlag.Name)
	if projectID == "" {
		logrus.Fatalf("warehouse-bigquery-project-id must be set when warehouse-driver=bigquery")
	}

	datasetName := cmd.String(warehouseBigQueryDatasetNameFlag.Name)
	if datasetName == "" {
		logrus.Fatalf("warehouse-bigquery-dataset-name must be set when warehouse-driver=bigquery")
	}

	credsJSON := strings.TrimSpace(cmd.String(warehouseBigQueryCredsJSONFlag.Name))
	if credsJSON == "" {
		logrus.Fatalf("warehouse-bigquery-creds-json must be set when warehouse-driver=bigquery")
	}

	// Support base64-encoded JSON for convenience
	raw := []byte(credsJSON)
	if decoded, decErr := base64.StdEncoding.DecodeString(credsJSON); decErr == nil {
		raw = decoded
	}

	// Build credentials from JSON
	googleCreds, credErr := google.CredentialsFromJSONWithType(
		ctx,
		raw,
		google.ServiceAccount,
		"https://www.googleapis.com/auth/bigquery",
	)
	if credErr != nil {
		logrus.Fatalf("failed to parse BigQuery credentials JSON: %v", credErr)
	}

	// Create BigQuery client
	client, err := bigquery.NewClient(
		ctx,
		projectID,
		option.WithCredentials(googleCreds),
	)
	if err != nil {
		logrus.Fatalf("failed to create BigQuery client: %v", err)
	}

	// Create writer based on type
	writer := createBigQueryWriter(cmd, client, datasetName)

	partitionOpt := createBigQueryPartitionOption(cmd)

	return warehouse.NewStaticBatchedDriverRegistry(
		ctx,
		whBigQuery.NewBigQueryTableDriver(
			client,
			datasetName,
			writer,
			whBigQuery.WithTableCreationTimeout(cmd.Duration(warehouseBigQueryTableCreationTimeoutFlag.Name)),
			whBigQuery.WithQueryTimeout(cmd.Duration(warehouseBigQueryQueryTimeoutFlag.Name)),
			partitionOpt,
		),
	)
}

// createBigQueryPartitionOption creates a BigQuery partition option from command flags.
// Returns nil if partition field is not set.
func createBigQueryPartitionOption(cmd *cli.Command) whBigQuery.BigQueryTableDriverOption {
	partitionField := strings.TrimSpace(cmd.String(warehouseBigQueryPartitionFieldFlag.Name))
	if partitionField == "" {
		return nil
	}

	intervalRaw := strings.ToUpper(strings.TrimSpace(cmd.String(warehouseBigQueryPartitionIntervalFlag.Name)))
	var interval whBigQuery.PartitionInterval
	switch intervalRaw {
	case string(whBigQuery.PartitionIntervalHour):
		interval = whBigQuery.PartitionIntervalHour
	case string(whBigQuery.PartitionIntervalDay), "":
		interval = whBigQuery.PartitionIntervalDay
	case string(whBigQuery.PartitionIntervalMonth):
		interval = whBigQuery.PartitionIntervalMonth
	case string(whBigQuery.PartitionIntervalYear):
		interval = whBigQuery.PartitionIntervalYear
	default:
		logrus.Fatalf("unsupported bigquery partition interval %q (expected HOUR, DAY, MONTH, YEAR)", intervalRaw)
	}

	return whBigQuery.WithPartitionBy(whBigQuery.PartitioningConfig{
		Interval:       interval,
		Field:          partitionField,
		ExpirationDays: cmd.Int(warehouseBigQueryPartitionExpirationDaysFlag.Name),
	})
}

func createBigQueryWriter(
	cmd *cli.Command,
	client *bigquery.Client,
	datasetName string,
) whBigQuery.Writer {
	writerType := strings.ToLower(cmd.String(warehouseBigQueryWriterTypeFlag.Name))
	queryTimeout := cmd.Duration(warehouseBigQueryQueryTimeoutFlag.Name)

	switch writerType {
	case "streaming":
		return whBigQuery.NewStreamingWriter(
			client,
			datasetName,
			queryTimeout,
			whBigQuery.NewFieldTypeMapper(),
		)
	case "loadjob", "":
		return whBigQuery.NewLoadJobWriter(
			client,
			datasetName,
			queryTimeout,
			whBigQuery.NewFieldTypeMapper(),
		)
	default:
		logrus.Fatalf("unsupported bigquery writer type: %s", writerType)
		return nil
	}
}

func createClickHouseWarehouse(ctx context.Context, cmd *cli.Command) warehouse.Registry {
	host := cmd.String(warehouseClickhouseHostFlag.Name)
	if host == "" {
		logrus.Fatalf("warehouse-clickhouse-host must be set when warehouse-driver=clickhouse")
	}

	port := cmd.String(warehouseClickhousePortFlag.Name)
	if port == "" {
		port = warehouseClickhousePortFlag.Value
	}

	database := cmd.String(warehouseClickhouseDatabaseFlag.Name)
	if database == "" {
		logrus.Fatalf("warehouse-clickhouse-database must be set when warehouse-driver=clickhouse")
	}

	options := &clickhouse.Options{
		Addr: []string{
			fmt.Sprintf("%s:%s", host, port),
		},
		Auth: clickhouse.Auth{
			Database: database,
			Username: cmd.String(warehouseClickhouseUsernameFlag.Name),
			Password: cmd.String(warehouseClickhousePasswordFlag.Name),
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: time.Second * 30,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Debug:                cmd.Bool(debugFlag.Name),
		BlockBufferSize:      10,
		MaxCompressionBuffer: 10240,
	}

	// Build ClickHouse driver options from flags
	var opts []whClickhouse.Options
	orderByStr := strings.TrimSpace(cmd.String(warehouseClickhouseOrderByFlag.Name))
	if orderByStr != "" {
		orderByParts := strings.Split(orderByStr, ",")
		orderBy := make([]string, 0, len(orderByParts))
		for _, part := range orderByParts {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				orderBy = append(orderBy, trimmed)
			}
		}
		if len(orderBy) > 0 {
			opts = append(opts, whClickhouse.WithOrderBy(orderBy))
		}
	}

	partitionByStr := strings.TrimSpace(cmd.String(warehouseClickhousePartitionByFlag.Name))
	if partitionByStr != "" {
		opts = append(opts, whClickhouse.WithPartitionBy(partitionByStr))
	}

	return warehouse.NewStaticBatchedDriverRegistry(
		ctx,
		whClickhouse.NewClickHouseTableDriver(
			options,
			database,
			opts...,
		),
	)
}

func createFilesWarehouse(ctx context.Context, cmd *cli.Command) warehouse.Registry {
	format := cmd.String(warehouseFilesFormatFlag.Name)
	flushInterval := cmd.Duration(warehouseFilesFlushIntervalFlag.Name)

	// Check if spool is enabled
	if !cmd.Bool(storageSpoolEnabledFlag.Name) {
		logrus.Fatal("files warehouse requires spool to be enabled (--storage-spool-enabled)")
	}

	baseSpoolDir := cmd.String(storageSpoolDirectoryFlag.Name)
	spoolDir := filepath.Join(baseSpoolDir, "warehouse/files")

	// Create format
	var fmt whFiles.Format
	switch format {
	case "csv":
		fmt = whFiles.NewCSVFormat()
	default:
		logrus.Fatalf("unsupported files format: %s", format)
	}

	// Create uploader based on storage type
	storageType := strings.ToLower(cmd.String(warehouseFilesStorageFlag.Name))
	var uploader whFiles.Uploader

	switch storageType {
	case storageTypeS3, storageTypeGCS:
		// Create bucket for cloud storage
		bucket, cleanup, err := createWarehouseBucket(ctx, cmd)
		if err != nil {
			logrus.WithError(err).Fatal("failed to create warehouse object storage bucket")
		}
		defer func() {
			if cleanupErr := cleanup(); cleanupErr != nil {
				logrus.WithError(cleanupErr).Error("failed to cleanup warehouse bucket")
			}
		}() // NOTE: In real impl, store cleanup in run.go

		uploader = whFiles.NewBlobUploader(bucket)

	case storageTypeFilesystem:
		// Create filesystem uploader
		filesystemPath := cmd.String(warehouseFilesFilesystemPathFlag.Name)
		if filesystemPath == "" {
			logrus.Fatal("--warehouse-files-filesystem-path is required when warehouse-files-storage=filesystem")
		}

		var err error
		uploader, err = whFiles.NewFilesystemUploader(filesystemPath)
		if err != nil {
			logrus.WithError(err).Fatal("failed to create filesystem uploader")
		}

	default:
		logrus.Fatal("--warehouse-files-storage must be set to s3, gcs, or filesystem")
	}

	// Create spool driver with timer-based flush
	//nolint:contextcheck // long-lived driver spawns goroutine with context.Background(), not request context
	driver := whFiles.NewSpoolDriver(uploader, fmt, spoolDir, flushInterval)

	// Wrap with batching
	return warehouse.NewStaticBatchedDriverRegistry(ctx, driver)
}
