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
	warehouseType := strings.ToLower(cmd.String(warehouseFlag.Name))
	if warehouseType == "" {
		warehouseType = warehouseFlag.Value
	}

	switch warehouseType {
	case "bigquery":
		return createBigQueryWarehouse(ctx, cmd)
	case "clickhouse":
		return createClickHouseWarehouse(cmd)
	case "console", "":
		return warehouse.NewStaticDriverRegistry(
			warehouse.NewConsoleDriver(),
		)
	default:
		logrus.Fatalf("unsupported warehouse %s", warehouseType)
		return nil
	}
}

func createBigQueryWarehouse(ctx context.Context, cmd *cli.Command) warehouse.Registry {
	projectID := cmd.String(bigQueryProjectIDFlag.Name)
	if projectID == "" {
		logrus.Fatalf("bigquery-project-id must be set when warehouse=bigquery")
	}

	datasetName := cmd.String(bigQueryDatasetNameFlag.Name)
	if datasetName == "" {
		logrus.Fatalf("bigquery-dataset-name must be set when warehouse=bigquery")
	}

	credsJSON := strings.TrimSpace(cmd.String(bigQueryCredsJSONFlag.Name))
	if credsJSON == "" {
		logrus.Fatalf("bigquery-creds-json must be set when warehouse=bigquery")
	}

	// Support base64-encoded JSON for convenience
	raw := []byte(credsJSON)
	if decoded, decErr := base64.StdEncoding.DecodeString(credsJSON); decErr == nil {
		raw = decoded
	}

	// Build credentials from JSON
	googleCreds, credErr := google.CredentialsFromJSON(
		ctx,
		raw,
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

	return warehouse.NewStaticDriverRegistry(
		whBigQuery.NewBigQueryTableDriver(
			client,
			datasetName,
			writer,
			cmd.Duration(bigQueryTableCreationTimeoutFlag.Name),
		),
	)
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

func createClickHouseWarehouse(cmd *cli.Command) warehouse.Registry {
	host := cmd.String(clickhouseHostFlag.Name)
	if host == "" {
		logrus.Fatalf("clickhouse-host must be set when warehouse=clickhouse")
	}

	port := cmd.String(clickhousePortFlag.Name)
	if port == "" {
		port = clickhousePortFlag.Value
	}

	database := cmd.String(clickhouseDatabaseFlag.Name)
	if database == "" {
		logrus.Fatalf("clickhouse-database must be set when warehouse=clickhouse")
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

	return warehouse.NewStaticDriverRegistry(
		whClickhouse.NewClickHouseTableDriver(
			options,
			database,
			whClickhouse.WithOrderBy([]string{"id"}),
		),
	)
}
