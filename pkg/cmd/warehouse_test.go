package cmd

import (
	"context"
	"reflect"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	whBigQuery "github.com/d8a-tech/d8a/pkg/warehouse/bigquery"
	whClickhouse "github.com/d8a-tech/d8a/pkg/warehouse/clickhouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

type testWarehouseDriver struct{}

func (d *testWarehouseDriver) CreateTable(_ string, _ *arrow.Schema) error { return nil }

func (d *testWarehouseDriver) AddColumn(_ string, _ *arrow.Field) error { return nil }

func (d *testWarehouseDriver) Write(_ context.Context, _ string, _ *arrow.Schema, _ []map[string]any) error {
	return nil
}

func (d *testWarehouseDriver) MissingColumns(_ string, _ *arrow.Schema) ([]*arrow.Field, error) {
	return nil, nil
}

func (d *testWarehouseDriver) Close() error { return nil }

type testBigQueryWriter struct{}

func (w *testBigQueryWriter) Write(
	_ context.Context,
	_ string,
	_ *arrow.Schema,
	_ []map[string]any,
) error {
	return nil
}

func TestWarehouseRegistry_WiresDriversWithoutBatchingWrapper(t *testing.T) {
	testCases := []struct {
		name               string
		args               func(t *testing.T) []string
		expectedDriverType string
	}{
		{
			name: "console driver is registered directly",
			args: func(_ *testing.T) []string {
				return []string{"d8a-test", "--warehouse-driver=console"}
			},
			expectedDriverType: "*warehouse.consoleDriver",
		},
		{
			name: "noop driver is registered directly",
			args: func(_ *testing.T) []string {
				return []string{"d8a-test", "--warehouse-driver=noop"}
			},
			expectedDriverType: "*warehouse.noopDriver",
		},
		{
			name: "files driver is registered directly",
			args: func(t *testing.T) []string {
				baseDir := t.TempDir()
				return []string{
					"d8a-test",
					"--warehouse-driver=files",
					"--storage-spool-enabled=true",
					"--storage-spool-directory=" + baseDir + "/spool",
					"--warehouse-files-storage=filesystem",
					"--warehouse-files-filesystem-path=" + baseDir + "/out",
				}
			},
			expectedDriverType: "*files.SpoolDriver",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// given
			args := testCase.args(t)

			app := &cli.Command{
				Name:  "d8a-test",
				Flags: mergeFlags([]cli.Flag{configFlag}, getServerFlags()),
				Action: func(ctx context.Context, cmd *cli.Command) error {
					// when
					registry := warehouseRegistry(ctx, cmd)
					driver, err := registry.Get("property-id")
					require.NoError(t, err)

					// then
					assert.Equal(t, testCase.expectedDriverType, reflect.TypeOf(driver).String())
					return registry.Close()
				},
			}

			require.NoError(t, app.Run(context.Background(), args))
		})
	}
}

func TestWarehouseRegistry_WiresBigQueryDriverWithoutBatchingWrapper(t *testing.T) {
	// given
	origCredentialsFn := bigQueryCredentialsFromJSONWithTypeFn
	origClientFn := newBigQueryClientFn
	origWriterFn := createBigQueryWriterFn
	origPartitionFn := createBigQueryPartitionOptionFn
	origTableDriverFn := newBigQueryTableDriverFn
	t.Cleanup(func() {
		bigQueryCredentialsFromJSONWithTypeFn = origCredentialsFn
		newBigQueryClientFn = origClientFn
		createBigQueryWriterFn = origWriterFn
		createBigQueryPartitionOptionFn = origPartitionFn
		newBigQueryTableDriverFn = origTableDriverFn
	})

	expectedDriver := &testWarehouseDriver{}

	bigQueryCredentialsFromJSONWithTypeFn = func(
		_ context.Context,
		_ []byte,
		_ google.CredentialsType,
		_ ...string,
	) (*google.Credentials, error) {
		return &google.Credentials{}, nil
	}

	newBigQueryClientFn = func(
		_ context.Context,
		_ string,
		_ ...option.ClientOption,
	) (*bigquery.Client, error) {
		return &bigquery.Client{}, nil
	}

	createBigQueryWriterFn = func(_ *cli.Command, _ *bigquery.Client, _ string) whBigQuery.Writer {
		return &testBigQueryWriter{}
	}

	createBigQueryPartitionOptionFn = func(_ *cli.Command) whBigQuery.BigQueryTableDriverOption {
		return nil
	}

	newBigQueryTableDriverFn = func(
		_ *bigquery.Client,
		_ string,
		_ whBigQuery.Writer,
		_ ...whBigQuery.BigQueryTableDriverOption,
	) warehouse.Driver {
		return expectedDriver
	}

	args := []string{
		"d8a-test",
		"--warehouse-driver=bigquery",
		"--warehouse-bigquery-project-id=test-project",
		"--warehouse-bigquery-dataset-name=test_dataset",
		"--warehouse-bigquery-creds-json={}",
	}

	app := &cli.Command{
		Name:  "d8a-test",
		Flags: mergeFlags([]cli.Flag{configFlag}, getServerFlags()),
		Action: func(ctx context.Context, cmd *cli.Command) error {
			// when
			registry := warehouseRegistry(ctx, cmd)
			driver, err := registry.Get("property-id")
			require.NoError(t, err)

			// then
			assert.Same(t, expectedDriver, driver)
			return registry.Close()
		},
	}

	require.NoError(t, app.Run(context.Background(), args))
}

func TestWarehouseRegistry_WiresClickHouseDriverWithoutBatchingWrapper(t *testing.T) {
	// given
	origClickHouseDriverFn := newClickHouseTableDriverFn
	t.Cleanup(func() {
		newClickHouseTableDriverFn = origClickHouseDriverFn
	})

	expectedDriver := &testWarehouseDriver{}
	newClickHouseTableDriverFn = func(
		_ *clickhouse.Options,
		_ string,
		_ ...whClickhouse.Options,
	) warehouse.Driver {
		return expectedDriver
	}

	args := []string{
		"d8a-test",
		"--warehouse-driver=clickhouse",
		"--warehouse-clickhouse-host=localhost",
		"--warehouse-clickhouse-database=default",
	}

	app := &cli.Command{
		Name:  "d8a-test",
		Flags: mergeFlags([]cli.Flag{configFlag}, getServerFlags()),
		Action: func(ctx context.Context, cmd *cli.Command) error {
			// when
			registry := warehouseRegistry(ctx, cmd)
			driver, err := registry.Get("property-id")
			require.NoError(t, err)

			// then
			assert.Same(t, expectedDriver, driver)
			return registry.Close()
		},
	}

	require.NoError(t, app.Run(context.Background(), args))
}
