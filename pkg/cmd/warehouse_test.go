package cmd

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/spools"
	whFiles "github.com/d8a-tech/d8a/pkg/warehouse/files"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

type closeOrderDriverStub struct {
	order *[]string
	err   error
}

func (d *closeOrderDriverStub) CreateTable(string, *arrow.Schema) error {
	return nil
}

func (d *closeOrderDriverStub) AddColumn(string, *arrow.Field) error {
	return nil
}

func (d *closeOrderDriverStub) Write(context.Context, string, *arrow.Schema, []map[string]any) error {
	return nil
}

func (d *closeOrderDriverStub) MissingColumns(string, *arrow.Schema) ([]*arrow.Field, error) {
	return nil, nil
}

func (d *closeOrderDriverStub) Close() error {
	*d.order = append(*d.order, "driver")
	return d.err
}

type closeOrderFactoryStub struct {
	order *[]string
	err   error
}

func (f *closeOrderFactoryStub) Create(spools.FlushHandler) (spools.Spool, error) {
	return nil, errors.New("not implemented")
}

func (f *closeOrderFactoryStub) Close() error {
	*f.order = append(*f.order, "factory")
	return f.err
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
			expectedDriverType: "*files.FilesDriver",
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

func TestCreateFilesWarehouse_ConfiguresQuarantineAfterThreeFailures(t *testing.T) {
	baseDir := t.TempDir()
	args := []string{
		"d8a-test",
		"--warehouse-driver=files",
		"--storage-spool-enabled=true",
		"--storage-spool-directory=" + baseDir + "/spool",
		"--warehouse-files-storage=filesystem",
		"--warehouse-files-filesystem-path=" + baseDir + "/out",
	}

	app := &cli.Command{
		Name:  "d8a-test",
		Flags: mergeFlags([]cli.Flag{configFlag}, getServerFlags()),
		Action: func(ctx context.Context, cmd *cli.Command) error {
			registry := warehouseRegistry(ctx, cmd)
			driver, err := registry.Get("property-id")
			require.NoError(t, err)

			filesDriver, ok := driver.(*whFiles.FilesDriver)
			require.True(t, ok)

			driverValue := reflect.ValueOf(filesDriver).Elem()
			spoolField := driverValue.FieldByName("spool")
			require.True(t, spoolField.IsValid())

			spoolValue := reflect.NewAt(spoolField.Type(), unsafe.Pointer(spoolField.UnsafeAddr())).Elem()
			concreteSpoolValue := reflect.ValueOf(spoolValue.Interface()).Elem()

			maxFailuresField := concreteSpoolValue.FieldByName("maxFailures")
			require.True(t, maxFailuresField.IsValid())
			maxFailuresValue := reflect.NewAt(maxFailuresField.Type(), unsafe.Pointer(maxFailuresField.UnsafeAddr())).Elem()
			assert.Equal(t, int64(3), maxFailuresValue.Int())

			flushIntervalField := concreteSpoolValue.FieldByName("flushInterval")
			require.True(t, flushIntervalField.IsValid())
			flushIntervalValue := reflect.NewAt(
				flushIntervalField.Type(),
				unsafe.Pointer(flushIntervalField.UnsafeAddr()),
			).Elem()
			assert.Equal(t, int64(time.Hour), flushIntervalValue.Int())

			failureStrategyField := concreteSpoolValue.FieldByName("failureStrategy")
			require.True(t, failureStrategyField.IsValid())
			failureStrategyValue := reflect.NewAt(
				failureStrategyField.Type(),
				unsafe.Pointer(failureStrategyField.UnsafeAddr()),
			).Elem()
			assert.Equal(t, "*spools.quarantineStrategy", reflect.TypeOf(failureStrategyValue.Interface()).String())

			return registry.Close()
		},
	}

	require.NoError(t, app.Run(context.Background(), args))
}

func TestCreateFilesWarehouse_UsesMaxSegmentAgeForFlushInterval(t *testing.T) {
	// given
	baseDir := t.TempDir()
	args := []string{
		"d8a-test",
		"--warehouse-driver=files",
		"--storage-spool-enabled=true",
		"--storage-spool-directory=" + baseDir + "/spool",
		"--warehouse-files-storage=filesystem",
		"--warehouse-files-filesystem-path=" + baseDir + "/out",
		"--warehouse-files-max-segment-age=37m",
	}

	app := &cli.Command{
		Name:  "d8a-test",
		Flags: mergeFlags([]cli.Flag{configFlag}, getServerFlags()),
		Action: func(ctx context.Context, cmd *cli.Command) error {
			registry := warehouseRegistry(ctx, cmd)
			driver, err := registry.Get("property-id")
			require.NoError(t, err)

			filesDriver, ok := driver.(*whFiles.FilesDriver)
			require.True(t, ok)

			driverValue := reflect.ValueOf(filesDriver).Elem()
			spoolField := driverValue.FieldByName("spool")
			require.True(t, spoolField.IsValid())

			spoolValue := reflect.NewAt(spoolField.Type(), unsafe.Pointer(spoolField.UnsafeAddr())).Elem()
			concreteSpoolValue := reflect.ValueOf(spoolValue.Interface()).Elem()

			flushIntervalField := concreteSpoolValue.FieldByName("flushInterval")
			require.True(t, flushIntervalField.IsValid())
			flushIntervalValue := reflect.NewAt(
				flushIntervalField.Type(),
				unsafe.Pointer(flushIntervalField.UnsafeAddr()),
			).Elem()
			assert.Equal(t, int64(37*time.Minute), flushIntervalValue.Int())

			return registry.Close()
		},
	}

	require.NoError(t, app.Run(context.Background(), args))
}

func TestFilesRegistryWithFactoryClose_Close_ClosesFactoryBeforeDriverAndJoinsErrors(t *testing.T) {
	// given
	order := make([]string, 0, 2)
	factoryErr := errors.New("factory close failed")
	driverErr := errors.New("driver close failed")

	registry := &filesRegistryWithFactoryClose{
		driver:  &closeOrderDriverStub{order: &order, err: driverErr},
		factory: &closeOrderFactoryStub{order: &order, err: factoryErr},
	}

	// when
	err := registry.Close()

	// then
	require.Error(t, err)
	require.Equal(t, []string{"factory", "driver"}, order)
	assert.ErrorIs(t, err, factoryErr)
	assert.ErrorIs(t, err, driverErr)
}

func TestResolveFilesWarehouseFormat_ParquetCompressionSelection(t *testing.T) {
	testCases := []struct {
		name              string
		compression       string
		expectedCodecType string
	}{
		{
			name:              "empty compression leaves parquet uncompressed",
			compression:       "",
			expectedCodecType: "",
		},
		{
			name:              "snappy compression uses parquet snappy codec",
			compression:       "snappy",
			expectedCodecType: "*snappy.Codec",
		},
		{
			name:              "gzip compression uses parquet gzip codec",
			compression:       "gzip",
			expectedCodecType: "*gzip.Codec",
		},
		{
			name:              "zstd compression uses parquet zstd codec",
			compression:       "zstd",
			expectedCodecType: "*zstd.Codec",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// when
			format, err := resolveFilesWarehouseFormat("parquet", testCase.compression, 4)

			// then
			require.NoError(t, err)
			assert.Equal(t, "parquet", format.Extension())
			assert.Equal(t, testCase.expectedCodecType, parquetCodecType(t, format))
		})
	}
}

func TestResolveFilesWarehouseFormat_CSVCompressionBehavior(t *testing.T) {
	testCases := []struct {
		name               string
		compression        string
		expectedErrMessage string
		expectedExtension  string
	}{
		{
			name:              "gzip compression is supported",
			compression:       "gzip",
			expectedExtension: "csv.gz",
		},
		{
			name:               "snappy compression is rejected for csv",
			compression:        "snappy",
			expectedErrMessage: "unsupported compression for csv: snappy",
		},
		{
			name:               "zstd compression is rejected for csv",
			compression:        "zstd",
			expectedErrMessage: "unsupported compression for csv: zstd",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// when
			format, err := resolveFilesWarehouseFormat("csv", testCase.compression, 5)

			// then
			if testCase.expectedErrMessage != "" {
				require.Error(t, err)
				assert.Nil(t, format)
				assert.Equal(t, testCase.expectedErrMessage, err.Error())
				return
			}

			require.NoError(t, err)
			assert.Equal(t, testCase.expectedExtension, format.Extension())
		})
	}
}

func TestResolveFilesWarehouseFormat_UnsupportedValues(t *testing.T) {
	// when
	format, err := resolveFilesWarehouseFormat("parquet", "brotli", 3)

	// then
	require.Error(t, err)
	assert.Nil(t, format)
	assert.Equal(t, "unsupported compression for parquet: brotli", err.Error())

	// when
	format, err = resolveFilesWarehouseFormat("json", "", 3)

	// then
	require.Error(t, err)
	assert.Nil(t, format)
	assert.Equal(t, "unsupported files format: json", err.Error())
}

func parquetCodecType(t *testing.T, format whFiles.Format) string {
	t.Helper()

	formatValue := reflect.ValueOf(format)
	require.Equal(t, "*files.parquetFormat", formatValue.Type().String())

	compressionCodecField := formatValue.Elem().FieldByName("compressionCodec")
	require.True(t, compressionCodecField.IsValid())

	compressionCodecValue := reflect.NewAt(
		compressionCodecField.Type(),
		unsafe.Pointer(compressionCodecField.UnsafeAddr()),
	).Elem()

	if compressionCodecValue.IsNil() {
		return ""
	}

	codecType := reflect.TypeOf(compressionCodecValue.Interface()).String()
	if strings.HasPrefix(codecType, "*") {
		parts := strings.Split(codecType[1:], "/")
		codecType = "*" + parts[len(parts)-1]
	}

	return codecType
}
