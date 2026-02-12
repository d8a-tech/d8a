package cmd

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	whBigQuery "github.com/d8a-tech/d8a/pkg/warehouse/bigquery"
	whClickhouse "github.com/d8a-tech/d8a/pkg/warehouse/clickhouse"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
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
	projectID := cmd.String(bigQueryProjectIDFlag.Name)
	if projectID == "" {
		logrus.Fatalf("bigquery-project-id must be set when warehouse-driver=bigquery")
	}

	datasetName := cmd.String(bigQueryDatasetNameFlag.Name)
	if datasetName == "" {
		logrus.Fatalf("bigquery-dataset-name must be set when warehouse-driver=bigquery")
	}

	credsJSON := strings.TrimSpace(cmd.String(bigQueryCredsJSONFlag.Name))
	if credsJSON == "" {
		logrus.Fatalf("bigquery-creds-json must be set when warehouse-driver=bigquery")
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
		google.ServiceAccountCredentialsType,
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
			whBigQuery.WithTableCreationTimeout(cmd.Duration(bigQueryTableCreationTimeoutFlag.Name)),
			whBigQuery.WithQueryTimeout(cmd.Duration(bigQueryQueryTimeoutFlag.Name)),
			partitionOpt,
		),
	)
}

// createBigQueryPartitionOption creates a BigQuery partition option from command flags.
// Returns nil if partition field is not set.
func createBigQueryPartitionOption(cmd *cli.Command) whBigQuery.BigQueryTableDriverOption {
	partitionField := strings.TrimSpace(cmd.String(bigQueryPartitionFieldFlag.Name))
	if partitionField == "" {
		return nil
	}

	intervalRaw := strings.ToUpper(strings.TrimSpace(cmd.String(bigQueryPartitionIntervalFlag.Name)))
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
		ExpirationDays: cmd.Int(bigQueryPartitionExpirationDaysFlag.Name),
	})
}

func createBigQueryWriter(
	cmd *cli.Command,
	client *bigquery.Client,
	datasetName string,
) whBigQuery.Writer {
	writerType := strings.ToLower(cmd.String(bigQueryWriterTypeFlag.Name))
	queryTimeout := cmd.Duration(bigQueryQueryTimeoutFlag.Name)

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
	host := cmd.String(clickhouseHostFlag.Name)
	if host == "" {
		logrus.Fatalf("clickhouse-host must be set when warehouse-driver=clickhouse")
	}

	port := cmd.String(clickhousePortFlag.Name)
	if port == "" {
		port = clickhousePortFlag.Value
	}

	database := cmd.String(clickhouseDatabaseFlag.Name)
	if database == "" {
		logrus.Fatalf("clickhouse-database must be set when warehouse-driver=clickhouse")
	}

	options := &clickhouse.Options{
		Addr: []string{
			fmt.Sprintf("%s:%s", host, port),
		},
		Auth: clickhouse.Auth{
			Database: database,
			Username: cmd.String(clickhouseUsernameFlag.Name),
			Password: cmd.String(clickhousePasswordFlag.Name),
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
	orderByStr := strings.TrimSpace(cmd.String(clickhouseOrderByFlag.Name))
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

	partitionByStr := strings.TrimSpace(cmd.String(clickhousePartitionByFlag.Name))
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
