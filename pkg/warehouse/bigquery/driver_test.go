package bigquery

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"google.golang.org/api/option"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/d8a-tech/d8a/pkg/warehouse/meta"
	"github.com/d8a-tech/d8a/pkg/warehouse/testutils"
	tcbigquery "github.com/testcontainers/testcontainers-go/modules/gcloud/bigquery"
)

func TestFieldToBQFieldSchema(t *testing.T) {
	tests := []struct {
		name            string
		field           *arrow.Field
		fieldSchema     SpecificBigQueryType
		wantDescription string
	}{
		{
			name: "with description metadata",
			field: &arrow.Field{
				Name:     "test_field",
				Type:     arrow.BinaryTypes.String,
				Nullable: false,
				Metadata: warehouse.MergeArrowMetadata(
					arrow.Metadata{},
					meta.ColumnDescriptionMetadataKey,
					"Test column description",
				),
			},
			fieldSchema: SpecificBigQueryType{
				FieldType: bigquery.StringFieldType,
				Required:  true,
				Repeated:  false,
			},
			wantDescription: "Test column description",
		},
		{
			name: "without description metadata",
			field: &arrow.Field{
				Name:     "test_field",
				Type:     arrow.BinaryTypes.String,
				Nullable: false,
				Metadata: arrow.Metadata{},
			},
			fieldSchema: SpecificBigQueryType{
				FieldType: bigquery.StringFieldType,
				Required:  true,
				Repeated:  false,
			},
			wantDescription: "",
		},
		{
			name: "with empty description metadata",
			field: &arrow.Field{
				Name:     "test_field",
				Type:     arrow.BinaryTypes.String,
				Nullable: false,
				Metadata: warehouse.MergeArrowMetadata(
					arrow.Metadata{},
					meta.ColumnDescriptionMetadataKey,
					"",
				),
			},
			fieldSchema: SpecificBigQueryType{
				FieldType: bigquery.StringFieldType,
				Required:  true,
				Repeated:  false,
			},
			wantDescription: "",
		},
		{
			name: "with nested schema",
			field: &arrow.Field{
				Name:     "nested_field",
				Type:     arrow.BinaryTypes.String,
				Nullable: false,
				Metadata: warehouse.MergeArrowMetadata(
					arrow.Metadata{},
					meta.ColumnDescriptionMetadataKey,
					"Nested field description",
				),
			},
			fieldSchema: SpecificBigQueryType{
				FieldType: bigquery.RecordFieldType,
				Required:  true,
				Repeated:  false,
				Schema: &bigquery.Schema{
					{Name: "inner", Type: bigquery.StringFieldType},
				},
			},
			wantDescription: "Nested field description",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bqField := fieldToBQFieldSchema(tt.field, tt.fieldSchema)

			assert.Equal(t, tt.field.Name, bqField.Name)
			assert.Equal(t, tt.fieldSchema.FieldType, bqField.Type)
			assert.Equal(t, tt.fieldSchema.Required, bqField.Required)
			assert.Equal(t, tt.fieldSchema.Repeated, bqField.Repeated)
			assert.Equal(t, tt.wantDescription, bqField.Description)

			if tt.fieldSchema.Schema != nil {
				require.NotNil(t, bqField.Schema)
				assert.Equal(t, *tt.fieldSchema.Schema, bqField.Schema)
			} else {
				assert.Nil(t, bqField.Schema)
			}
		})
	}
}

// generateUniqueTableName generates a unique table name with timestamp that sorts alphabetically with newest first
// Format: prefix_YYYYMMDD_HHMMSS_nanoseconds
// The timestamp ensures uniqueness and proper sorting (newest first alphabetically)
func generateUniqueTableName(prefix string) string {
	now := time.Now()
	// Format timestamp to ensure alphabetical sorting with newest first
	timestamp := now.Format("20060102_150405")
	nanoseconds := now.Nanosecond()

	return fmt.Sprintf("%s_%s_%09d", prefix, timestamp, nanoseconds)
}

// testDriverConfig represents configuration for test drivers
type testDriverConfig struct {
	name              string
	driver            warehouse.Driver
	projectID         string
	datasetName       string
	supportsAddColumn bool
	supportsWrites    bool
	cleanup           func()
}

// createTestDrivers creates both emulator and real BigQuery drivers if available
func createTestDrivers(t *testing.T) []testDriverConfig {
	t.Helper()

	var configs []testDriverConfig

	// Always create emulator driver
	emulatorConfig := createEmulatorDriver(t)
	configs = append(configs, emulatorConfig)

	// Create emulator driver with batch writer
	emulatorWithBatchWriterConfig := createEmulatorDriver(t)
	emulatorWithBatchWriterConfig.name = "emulator_batch_writer"
	emulatorWithBatchWriterConfig.driver = NewBigQueryTableDriver( //nolint:forcetypeassert // test code
		emulatorWithBatchWriterConfig.driver.(*bigQueryTableDriver).db,
		emulatorWithBatchWriterConfig.datasetName,
		NewLoadJobWriter(
			emulatorWithBatchWriterConfig.driver.(*bigQueryTableDriver).db,
			emulatorWithBatchWriterConfig.datasetName,
			30*time.Second,
			NewFieldTypeMapper(),
		),
		WithTableCreationTimeout(5*time.Second), // Table creation timeout for tests
	)
	configs = append(configs, emulatorWithBatchWriterConfig)

	// Create real BigQuery driver if environment variables are present
	if realConfig := createRealBigQueryDriver(t); realConfig != nil {
		configs = append(configs, *realConfig)
	}

	return configs
}

// createEmulatorDriver creates a BigQuery emulator driver
func createEmulatorDriver(t *testing.T) testDriverConfig {
	t.Helper()

	ctx := context.Background()
	projectID := "testproject"
	datasetName := "test_dataset"

	bigQueryContainer, err := tcbigquery.Run(ctx,
		"ghcr.io/goccy/bigquery-emulator:0.6.1",
		tcbigquery.WithDataYAML(strings.NewReader(`projects:
 - id: `+projectID+`
   datasets:
   - id: `+datasetName+`
     tables: []
`)),
		tcbigquery.WithProjectID(projectID),
		testcontainers.WithImagePlatform("linux/amd64"), // Force AMD64 platform for ARM64 compatibility
	)
	require.NoError(t, err)

	client, err := bigquery.NewClient(
		ctx, projectID,
		option.WithEndpoint(bigQueryContainer.URI()),
		option.WithoutAuthentication(),
	)
	require.NoError(t, err)

	driver := NewBigQueryTableDriver(
		client, datasetName,
		NewStreamingWriter(client, datasetName, 30*time.Second, NewFieldTypeMapper()),
		WithTableCreationTimeout(5*time.Second), // Table creation timeout for tests
	)

	cleanup := func() {
		if err := testcontainers.TerminateContainer(bigQueryContainer); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}

	return testDriverConfig{
		name:              "emulator",
		driver:            driver,
		projectID:         projectID,
		datasetName:       datasetName,
		supportsAddColumn: false,
		supportsWrites:    true,
		cleanup:           cleanup,
	}
}

// createRealBigQueryDriver creates a real BigQuery driver if environment variables are present
func createRealBigQueryDriver(t *testing.T) *testDriverConfig {
	t.Helper()

	projectID := os.Getenv("BIGQUERY_PROJECT_ID")
	datasetName := os.Getenv("BIGQUERY_DATASET_NAME")

	// Check if required environment variables are present
	if projectID == "" {
		t.Logf("Skipping real BigQuery tests: BIGQUERY_PROJECT_ID not set")
		return nil
	}

	if datasetName == "" {
		datasetName = "test_dataset" // default dataset name
		t.Logf("Using default dataset name: %s", datasetName)
	}

	// Check for credentials
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		t.Logf("Skipping real BigQuery tests: no credentials found (GOOGLE_APPLICATION_CREDENTIALS)")
		return nil
	}

	ctx := context.Background()

	var client *bigquery.Client
	var err error

	// Try to create client with available credentials
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "" {
		client, err = bigquery.NewClient(ctx, projectID)
	}

	if err != nil {
		t.Logf("Skipping real BigQuery tests: failed to create client: %v", err)
		return nil
	}

	driver := NewBigQueryTableDriver(
		client, datasetName,
		NewLoadJobWriter(client, datasetName, 30*time.Second, NewFieldTypeMapper()),
		WithTableCreationTimeout(5*time.Second), // Table creation timeout for tests
	)

	return &testDriverConfig{
		name:              "bigquery",
		driver:            driver,
		projectID:         projectID,
		supportsAddColumn: true,
		supportsWrites:    true,
		datasetName:       datasetName,
		cleanup:           func() {}, // No cleanup needed for real BigQuery
	}
}

func TestBigqueryCreateTable(t *testing.T) {
	drivers := createTestDrivers(t)

	for _, config := range drivers {
		t.Run(config.name, func(t *testing.T) {
			defer config.cleanup()

			tableName := generateUniqueTableName("test_table")
			assert.NoError(t, config.driver.CreateTable(tableName, testutils.TestSchema()))
		})
	}
}

func TestBigQueryMissingColumns(t *testing.T) {
	drivers := createTestDrivers(t)

	for _, config := range drivers {
		t.Run(config.name, func(t *testing.T) {
			defer config.cleanup()

			tableName := generateUniqueTableName("test_missing_columns")
			testutils.TestMissingColumns(t, config.driver, tableName)
		})
	}
}

func TestBigQueryMissingColumnsTypeCompatibility(t *testing.T) {
	drivers := createTestDrivers(t)

	for _, config := range drivers {
		t.Run(config.name, func(t *testing.T) {
			defer config.cleanup()

			driver := config.driver

			tests := []struct {
				name              string
				existingFieldType arrow.DataType
				inputFieldType    arrow.DataType
				existingNullable  bool
				inputNullable     bool
				expectError       bool
				expectTypeError   bool
			}{
				{
					name:              "int32_to_int64_compatible",
					existingFieldType: arrow.PrimitiveTypes.Int32,
					inputFieldType:    arrow.PrimitiveTypes.Int64,
					existingNullable:  true,
					inputNullable:     true,
					expectError:       false,
				},
				{
					name:              "int64_to_int32_compatible",
					existingFieldType: arrow.PrimitiveTypes.Int64,
					inputFieldType:    arrow.PrimitiveTypes.Int32,
					existingNullable:  true,
					inputNullable:     true,
					expectError:       false,
				},
				{
					name:              "float32_to_float64_compatible",
					existingFieldType: arrow.PrimitiveTypes.Float32,
					inputFieldType:    arrow.PrimitiveTypes.Float64,
					existingNullable:  true,
					inputNullable:     true,
					expectError:       false,
				},
				{
					name:              "float64_to_float32_compatible",
					existingFieldType: arrow.PrimitiveTypes.Float64,
					inputFieldType:    arrow.PrimitiveTypes.Float32,
					existingNullable:  true,
					inputNullable:     true,
					expectError:       false,
				},
				{
					name:              "string_to_int_incompatible",
					existingFieldType: arrow.BinaryTypes.String,
					inputFieldType:    arrow.PrimitiveTypes.Int64,
					existingNullable:  true,
					inputNullable:     true,
					expectError:       true,
					expectTypeError:   true,
				},
				{
					name:              "int_to_string_incompatible",
					existingFieldType: arrow.PrimitiveTypes.Int64,
					inputFieldType:    arrow.BinaryTypes.String,
					existingNullable:  true,
					inputNullable:     true,
					expectError:       true,
					expectTypeError:   true,
				},
				{
					name:              "bool_to_string_incompatible",
					existingFieldType: arrow.FixedWidthTypes.Boolean,
					inputFieldType:    arrow.BinaryTypes.String,
					existingNullable:  true,
					inputNullable:     true,
					expectError:       true,
					expectTypeError:   true,
				},
				{
					name:              "same_type_compatible",
					existingFieldType: arrow.BinaryTypes.String,
					inputFieldType:    arrow.BinaryTypes.String,
					existingNullable:  true,
					inputNullable:     true,
					expectError:       false,
				},
				{
					name:              "nullable_to_non_nullable_incompatible",
					existingFieldType: arrow.BinaryTypes.String,
					inputFieldType:    arrow.BinaryTypes.String,
					existingNullable:  true,
					inputNullable:     false,
					expectError:       true,
					expectTypeError:   true,
				},
				{
					name:              "non_nullable_to_nullable_incompatible",
					existingFieldType: arrow.BinaryTypes.String,
					inputFieldType:    arrow.BinaryTypes.String,
					existingNullable:  false,
					inputNullable:     true,
					expectError:       true,
					expectTypeError:   true,
				},
				{
					name:              "both_non_nullable_compatible",
					existingFieldType: arrow.BinaryTypes.String,
					inputFieldType:    arrow.BinaryTypes.String,
					existingNullable:  false,
					inputNullable:     false,
					expectError:       false,
				},
				{
					name:              "nullable_int_to_non_nullable_string_incompatible",
					existingFieldType: arrow.PrimitiveTypes.Int64,
					inputFieldType:    arrow.BinaryTypes.String,
					existingNullable:  true,
					inputNullable:     false,
					expectError:       true,
					expectTypeError:   true,
				},
			}

			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					tableName := generateUniqueTableName("test_type_compat_" + strings.ReplaceAll(tc.name, "_", ""))

					// Create table with existing field type and nullability
					existingField := arrow.Field{Name: "test_column", Type: tc.existingFieldType, Nullable: tc.existingNullable}
					existingSchema := arrow.NewSchema([]arrow.Field{existingField}, nil)

					err := driver.CreateTable(tableName, existingSchema)
					require.NoError(t, err, "should create table successfully")

					// Create input schema with potentially incompatible type and nullability
					inputField := arrow.Field{Name: "test_column", Type: tc.inputFieldType, Nullable: tc.inputNullable}
					inputSchema := arrow.NewSchema([]arrow.Field{inputField}, nil)

					// Test MissingColumns
					missing, err := driver.MissingColumns(tableName, inputSchema)

					if tc.expectError {
						require.Error(t, err, "should return error for incompatible types")
						if tc.expectTypeError {
							var multiTypeError *warehouse.ErrMultipleTypeIncompatible
							require.ErrorAs(t, err, &multiTypeError, "should return ErrMultipleTypeIncompatible")
							require.Equal(t, fmt.Sprintf("%s.%s", config.datasetName, tableName), multiTypeError.TableName)
							require.Len(t, multiTypeError.Errors, 1, "should have exactly one type error")
							require.Equal(t, "test_column", multiTypeError.Errors[0].ColumnName)
						}
					} else {
						require.NoError(t, err, "should not return error for compatible types")
						require.Empty(t, missing, "should return no missing columns for compatible existing column")
					}
				})
			}
		})
	}
}

func TestBigQueryMultipleTypeIncompatibilities(t *testing.T) {
	drivers := createTestDrivers(t)

	for _, config := range drivers {
		t.Run(config.name, func(t *testing.T) {
			defer config.cleanup()

			driver := config.driver
			tableName := generateUniqueTableName("test_multi_incompatible")

			// given - create table with specific types
			existingFields := []arrow.Field{
				{Name: "string_field", Type: arrow.BinaryTypes.String, Nullable: true},
				{Name: "int_field", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
				{Name: "bool_field", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
			}
			existingSchema := arrow.NewSchema(existingFields, nil)

			err := driver.CreateTable(tableName, existingSchema)
			require.NoError(t, err, "should create table successfully")

			// when - check with incompatible types for multiple fields
			inputFields := []arrow.Field{
				{Name: "string_field", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, // string -> int64 (incompatible)
				{Name: "int_field", Type: arrow.BinaryTypes.String, Nullable: false},     // int64 -> string (incompatible)
				{Name: "bool_field", Type: arrow.PrimitiveTypes.Float64, Nullable: true}, // bool -> float64 (incompatible)
			}
			inputSchema := arrow.NewSchema(inputFields, nil)

			missing, err := driver.MissingColumns(tableName, inputSchema)

			// then - should return all type incompatibilities at once
			require.Error(t, err, "should return error for multiple incompatible types")
			var multiTypeError *warehouse.ErrMultipleTypeIncompatible
			require.ErrorAs(t, err, &multiTypeError, "should return ErrMultipleTypeIncompatible")
			require.Equal(t, fmt.Sprintf("%s.%s", config.datasetName, tableName), multiTypeError.TableName)
			require.Len(t, multiTypeError.Errors, 3, "should have exactly three type errors")
			require.Nil(t, missing, "should not return missing columns when there are type errors")

			// Check that all expected columns are in the error list
			errorColumns := make(map[string]*warehouse.ErrTypeIncompatible)
			for _, typeErr := range multiTypeError.Errors {
				errorColumns[typeErr.ColumnName] = typeErr
			}

			require.Contains(t, errorColumns, "string_field", "should include string_field error")
			require.Contains(t, errorColumns, "int_field", "should include int_field error")
			require.Contains(t, errorColumns, "bool_field", "should include bool_field error")

			require.Equal(t, arrow.BinaryTypes.String, errorColumns["string_field"].ExistingType)
			require.Equal(t, arrow.PrimitiveTypes.Int64, errorColumns["string_field"].ExpectedType)
		})
	}
}

func TestBigqueryWrite(t *testing.T) {
	drivers := createTestDrivers(t)

	for _, config := range drivers {
		if !config.supportsWrites {
			t.Logf("Skipping %s - does not support writes", config.name)
			continue
		}

		t.Run(config.name, func(t *testing.T) {
			defer config.cleanup()

			tableName := generateUniqueTableName("test_write")

			// Run the standard warehouse test first
			testutils.TestBasicWrites(t, config.driver, tableName)

			// Additional assertions to verify data was actually inserted
			t.Run("verify_data_inserted", func(t *testing.T) {
				// Skip for emulator-only tests if we don't have a BigQuery client
				bqDriver, ok := config.driver.(*bigQueryTableDriver)
				if !ok {
					t.Skip("Cannot verify data insertion - not a BigQuery driver")
					return
				}

				ctx := context.Background()

				countQuery := bqDriver.db.Query(fmt.Sprintf(
					"SELECT COUNT(*) as row_count FROM `%s.%s`",
					config.datasetName,
					tableName,
				))
				countIt, err := countQuery.Read(ctx)
				require.NoError(t, err, "should execute count query successfully")

				var countRow []bigquery.Value
				err = countIt.Next(&countRow)
				require.NoError(t, err, "should read count result")

				rowCount, ok := countRow[0].(int64)
				require.True(t, ok, "count should be int64")
				require.Greater(t, rowCount, int64(0), "should have inserted at least one row")
			})
		})
	}
}

func TestBigqueryAddColumn(t *testing.T) {
	drivers := createTestDrivers(t)

	for _, config := range drivers {
		if !config.supportsAddColumn {
			t.Logf("Skipping %s - does not support adding columns", config.name)
			continue
		}

		t.Run(config.name, func(t *testing.T) {
			defer config.cleanup()
			testutils.TestAddColumn(t, config.driver, generateUniqueTableName("test_add_column"))
		})
	}
}

func TestBigqueryCreateTableAlreadyExists(t *testing.T) {
	drivers := createTestDrivers(t)

	for _, config := range drivers {
		t.Run(config.name, func(t *testing.T) {
			defer config.cleanup()
			testutils.TestCreateTable(t, config.driver, generateUniqueTableName("test_create_table_already_exists"))
		})
	}
}

func TestBigQueryPartitioning(t *testing.T) {
	ctx := context.Background()
	projectID := "testproject"
	datasetName := "test_dataset"

	bigQueryContainer, err := tcbigquery.Run(ctx,
		"ghcr.io/goccy/bigquery-emulator:0.6.1",
		tcbigquery.WithDataYAML(strings.NewReader(`projects:
 - id: `+projectID+`
   datasets:
   - id: `+datasetName+`
     tables: []
`)),
		tcbigquery.WithProjectID(projectID),
		testcontainers.WithImagePlatform("linux/amd64"),
	)
	require.NoError(t, err)
	defer func() {
		if err := testcontainers.TerminateContainer(bigQueryContainer); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()

	client, err := bigquery.NewClient(
		ctx, projectID,
		option.WithEndpoint(bigQueryContainer.URI()),
		option.WithoutAuthentication(),
	)
	require.NoError(t, err)

	t.Run("default_no_partitioning", func(t *testing.T) {
		// given - driver without partitioning option
		driver := NewBigQueryTableDriver(
			client, datasetName,
			NewStreamingWriter(client, datasetName, 30*time.Second, NewFieldTypeMapper()),
			WithTableCreationTimeout(5*time.Second),
		)

		// when - create table
		tableName := generateUniqueTableName("test_no_partitioning")
		err := driver.CreateTable(tableName, testutils.TestSchema())
		require.NoError(t, err)

		// then - verify TimePartitioning is nil
		tableRef := client.Dataset(datasetName).Table(tableName)
		metadata, err := tableRef.Metadata(ctx)
		require.NoError(t, err)
		assert.Nil(t, metadata.TimePartitioning)
	})

	t.Run("with_partition_by_field_empty_panics", func(t *testing.T) {
		// given/when/then - WithPartitionByField with empty field should panic
		require.Panics(t, func() {
			_ = WithPartitionByField("")
		})
	})

	t.Run("with_partition_by_empty_field_panics", func(t *testing.T) {
		// given/when/then - WithPartitionBy with empty field should panic
		require.Panics(t, func() {
			_ = WithPartitionBy(PartitioningConfig{
				Interval: PartitionIntervalDay,
				Field:    "",
			})
		})
	})
}
