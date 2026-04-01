package cmd

import (
	"context"
	"reflect"
	"testing"
	"unsafe"

	whFiles "github.com/d8a-tech/d8a/pkg/warehouse/files"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

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

			filesDriver, ok := driver.(*whFiles.SpoolDriver)
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
