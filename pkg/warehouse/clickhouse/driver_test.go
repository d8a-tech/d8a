package clickhouse

import (
	"context"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/d8a-tech/d8a/pkg/warehouse/testutils"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

// ClickHouse CREATE TABLE query with primitive fields, collections, nested fields, and partitioning
const ClickHouseCreateTableQuery = `CREATE TABLE testdb.analytics_events (
	id String,
	user_id Int64,
	timestamp Nullable(DateTime64(0)),
	event_type String,
	session_id String,
	is_active Bool,
	score Float64,
	count Int64,
	items Int32,
	rating Float32,
	tags Array(String),
	metrics Array(Float64),
	properties Nested(
		key String,
		value String
	  ),
	  created_date Nullable(Date32)
  ) ENGINE = MergeTree()
  PARTITION BY toYYYYMM(timestamp)
  ORDER BY (event_type, user_id, timestamp)
  SETTINGS index_granularity = 8192;`

// ClickHouseTestSuite is a test suite that sets up a single ClickHouse container
// for all tests to share, improving test performance
type ClickHouseTestSuite struct {
	suite.Suite
	container *tcclickhouse.ClickHouseContainer
	opts      *clickhouse.Options
	driver    warehouse.Driver
}

// SetupSuite runs once before all tests in the suite
func (suite *ClickHouseTestSuite) SetupSuite() {
	ctx := context.Background()

	user := "clickhouse"
	password := "password"
	dbname := "testdb"

	var err error
	suite.container, err = tcclickhouse.Run(ctx,
		"clickhouse/clickhouse-server:23.3.8.21-alpine",
		tcclickhouse.WithUsername(user),
		tcclickhouse.WithPassword(password),
		tcclickhouse.WithDatabase(dbname),
	)
	suite.Require().NoError(err, "should start ClickHouse container successfully")

	host, err := suite.container.ConnectionHost(ctx)
	suite.Require().NoError(err, "should get connection host")

	suite.opts = &clickhouse.Options{
		Addr: []string{host},
		Auth: clickhouse.Auth{
			Database: dbname,
			Username: user,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: time.Second * 30,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Debug:                true,
		BlockBufferSize:      10,
		MaxCompressionBuffer: 10240,
	}

	// Create a default driver instance for the suite
	suite.driver = NewClickHouseTableDriver(suite.opts, "testdb",
		WithOrderBy([]string{"tuple()"}),
	)
}

// TearDownSuite runs once after all tests in the suite
func (suite *ClickHouseTestSuite) TearDownSuite() {
	if suite.container != nil {
		if err := testcontainers.TerminateContainer(suite.container); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}
}

// SetupTest runs before each individual test
func (suite *ClickHouseTestSuite) SetupTest() {
	// Recreate driver for each test to ensure clean state
	suite.driver = NewClickHouseTableDriver(suite.opts, "testdb",
		WithOrderBy([]string{"tuple()"}),
	)
}

// TearDownTest runs after each individual test to clean up tables
func (suite *ClickHouseTestSuite) TearDownTest() {
	// Clean up all tables in testdb to prevent interference between tests
	suite.cleanupDatabase()
}

// cleanupDatabase drops all tables in the testdb database
func (suite *ClickHouseTestSuite) cleanupDatabase() {
	db := clickhouse.OpenDB(suite.opts)
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("failed to close database connection: %s", err)
		}
	}()

	// Get all table names in testdb
	query := `SELECT name FROM system.tables WHERE database = 'testdb'`
	rows, err := db.Query(query)
	if err != nil {
		log.Printf("failed to query tables: %s", err)
		return
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("failed to close rows: %s", err)
		}
	}()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			log.Printf("failed to scan table name: %s", err)
			continue
		}
		tables = append(tables, tableName)
	}

	// Drop each table
	for _, tableName := range tables {
		dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS testdb.%s", tableName)
		if _, err := db.Exec(dropQuery); err != nil {
			log.Printf("failed to drop table %s: %s", tableName, err)
		}
	}
}

func (suite *ClickHouseTestSuite) TestCreateTable() {
	// when & then
	err := suite.driver.CreateTable("analytics_events", testutils.TestSchema())
	suite.Assert().NoError(err)
}

func (suite *ClickHouseTestSuite) TestMissingColumns() {
	testCases := []struct {
		name      string
		tableName string
	}{
		{
			name:      "simple table name",
			tableName: "test_missing_columns_table",
		},
		{
			name:      "another simple table name",
			tableName: "test_missing_columns_table_2",
		},
	}

	for _, tc := range testCases {
		suite.Run(tc.name, func() {
			// when & then
			testutils.TestMissingColumns(suite.T(), suite.driver, tc.tableName)
		})
	}
}

func (suite *ClickHouseTestSuite) TestMissingColumnsTypeCompatibility() {
	testCases := []struct {
		name               string
		existingType       string
		inputType          arrow.DataType
		shouldBeCompatible bool
		description        string
	}{
		{
			name:               "exact_int32_match",
			existingType:       "Int32",
			inputType:          arrow.PrimitiveTypes.Int32,
			shouldBeCompatible: true,
			description:        "Int32 should match Int32",
		},
		{
			name:               "exact_int64_match",
			existingType:       "Int64",
			inputType:          arrow.PrimitiveTypes.Int64,
			shouldBeCompatible: true,
			description:        "Int64 should match Int64",
		},
		{
			name:               "int32_vs_int64_incompatible",
			existingType:       "Int32",
			inputType:          arrow.PrimitiveTypes.Int64,
			shouldBeCompatible: false,
			description:        "Int32 should NOT match Int64 (strict matching)",
		},
		{
			name:               "int64_vs_int32_incompatible",
			existingType:       "Int64",
			inputType:          arrow.PrimitiveTypes.Int32,
			shouldBeCompatible: false,
			description:        "Int64 should NOT match Int32 (strict matching)",
		},
		{
			name:               "exact_float32_match",
			existingType:       "Float32",
			inputType:          arrow.PrimitiveTypes.Float32,
			shouldBeCompatible: true,
			description:        "Float32 should match Float32",
		},
		{
			name:               "exact_float64_match",
			existingType:       "Float64",
			inputType:          arrow.PrimitiveTypes.Float64,
			shouldBeCompatible: true,
			description:        "Float64 should match Float64",
		},
		{
			name:               "float32_vs_float64_incompatible",
			existingType:       "Float32",
			inputType:          arrow.PrimitiveTypes.Float64,
			shouldBeCompatible: false,
			description:        "Float32 should NOT match Float64 (strict matching)",
		},
		{
			name:               "float64_vs_float32_incompatible",
			existingType:       "Float64",
			inputType:          arrow.PrimitiveTypes.Float32,
			shouldBeCompatible: false,
			description:        "Float64 should NOT match Float32 (strict matching)",
		},
		{
			name:               "string_match",
			existingType:       "String",
			inputType:          arrow.BinaryTypes.String,
			shouldBeCompatible: true,
			description:        "String should match String",
		},
		{
			name:               "string_vs_int_incompatible",
			existingType:       "String",
			inputType:          arrow.PrimitiveTypes.Int32,
			shouldBeCompatible: false,
			description:        "String should NOT match Int32",
		},
	}

	for _, tc := range testCases {
		suite.Run(tc.name, func() {
			// Create table with single column of existing type
			tableName := "type_compatibility_test_" + strings.ReplaceAll(tc.name, "_", "")
			createQuery := fmt.Sprintf("CREATE TABLE testdb.%s (test_column %s) ENGINE = MergeTree() ORDER BY tuple()",
				tableName, tc.existingType)
			_, err := clickhouse.OpenDB(suite.opts).Exec(createQuery)
			suite.Require().NoError(err, "table creation should succeed")

			// Create input schema with single field of input type
			inputField := arrow.Field{Name: "test_column", Type: tc.inputType, Nullable: false}
			inputSchema := arrow.NewSchema([]arrow.Field{inputField}, nil)

			// when - check missing columns
			missing, err := suite.driver.MissingColumns(tableName, inputSchema)

			// then - validate based on compatibility expectation
			if tc.shouldBeCompatible {
				// Should succeed with no missing columns (types are compatible)
				suite.Assert().NoError(err, tc.description)
				suite.Assert().Empty(missing, "compatible types should result in no missing columns")
			} else {
				// Should fail with type incompatible error
				suite.Assert().Error(err, tc.description)
				var multiTypeError *warehouse.ErrMultipleTypeIncompatible
				suite.Assert().ErrorAs(err, &multiTypeError,
					"incompatible types should return ErrMultipleTypeIncompatible")
				suite.Assert().Len(multiTypeError.Errors, 1, "should have exactly one type error")
				suite.Assert().Equal("test_column", multiTypeError.Errors[0].ColumnName)
			}
		})
	}
}

func (suite *ClickHouseTestSuite) TestMissingColumnsErrorCases() {
	testCases := []struct {
		name          string
		tableName     string
		expectedError string
	}{
		{
			name:          "non-existent table",
			tableName:     "non_existent_table",
			expectedError: "not found",
		},
	}

	for _, tc := range testCases {
		suite.Run(tc.name, func() {
			// when - call MissingColumns with invalid parameters
			testSchema := testutils.TestSchema()
			_, err := suite.driver.MissingColumns(tc.tableName, testSchema)

			// then - should return appropriate error
			suite.Require().Error(err, "MissingColumns should fail for %s", tc.name)
			suite.Require().Contains(strings.ToLower(err.Error()), strings.ToLower(tc.expectedError),
				"error message should contain expected text for %s", tc.name)
		})
	}
}

func (suite *ClickHouseTestSuite) TestMultipleTypeIncompatibilities() {
	tableName := "test_multi_incompatible"

	// given - create table with specific types
	createQuery := fmt.Sprintf(`
		CREATE TABLE testdb.%s (
			string_field String,
			int_field Int64,
			bool_field Bool
		) ENGINE = MergeTree() ORDER BY tuple()
	`, tableName)
	_, err := clickhouse.OpenDB(suite.opts).Exec(createQuery)
	suite.Require().NoError(err, "should create table successfully")

	// when - check with incompatible types for multiple fields
	inputFields := []arrow.Field{
		{Name: "string_field", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, // string -> int64 (incompatible)
		{Name: "int_field", Type: arrow.BinaryTypes.String, Nullable: false},     // int64 -> string (incompatible)
		{Name: "bool_field", Type: arrow.PrimitiveTypes.Float64, Nullable: true}, // bool -> float64 (incompatible)
	}
	inputSchema := arrow.NewSchema(inputFields, nil)

	missing, err := suite.driver.MissingColumns(tableName, inputSchema)

	// then - should return all type incompatibilities at once
	suite.Require().Error(err, "should return error for multiple incompatible types")
	var multiTypeError *warehouse.ErrMultipleTypeIncompatible
	suite.Require().ErrorAs(err, &multiTypeError, "should return ErrMultipleTypeIncompatible")
	suite.Require().Equal(fmt.Sprintf("testdb.%s", tableName), multiTypeError.TableName)
	suite.Require().Len(multiTypeError.Errors, 3, "should have exactly three type errors")
	suite.Require().Nil(missing, "should not return missing columns when there are type errors")

	// Check that all expected columns are in the error list
	errorColumns := make(map[string]*warehouse.ErrTypeIncompatible)
	for _, typeErr := range multiTypeError.Errors {
		errorColumns[typeErr.ColumnName] = typeErr
	}

	suite.Require().Contains(errorColumns, "string_field", "should include string_field error")
	suite.Require().Contains(errorColumns, "int_field", "should include int_field error")
	suite.Require().Contains(errorColumns, "bool_field", "should include bool_field error")

	suite.Require().Equal(arrow.BinaryTypes.String, errorColumns["string_field"].ExistingType)
	suite.Require().Equal(arrow.PrimitiveTypes.Int64, errorColumns["string_field"].ExpectedType)
}

func (suite *ClickHouseTestSuite) TestWrite() {
	tableName := "test_write"

	// when & then - run the standard warehouse test
	testutils.TestWrite(suite.T(), suite.driver, tableName)

	// Additional assertions to verify data was actually inserted
	suite.Run("verify_data_inserted", func() {
		// given
		query := fmt.Sprintf("SELECT count(*) FROM testdb.%s", tableName)

		// when
		var count int64
		err := clickhouse.OpenDB(suite.opts).QueryRow(query).Scan(&count)

		// then
		suite.Require().NoError(err, "should execute count query successfully")
		suite.Assert().Greater(count, int64(0), "should have inserted at least one row")
	})
}

func (suite *ClickHouseTestSuite) TestAddColumn() {
	tableName := "test_add_column"

	// when & then - run the standard warehouse test
	testutils.TestAddColumn(suite.T(), suite.driver, tableName)
}

func (suite *ClickHouseTestSuite) TestCreateTableAlreadyExists() {
	tableName := "test_create_table_already_exists"

	// when & then - run the standard warehouse test
	testutils.TestCreateTable(suite.T(), suite.driver, tableName)
}

// TestClickHouseTestSuite runs the entire test suite
func TestClickHouseTestSuite(t *testing.T) {
	suite.Run(t, new(ClickHouseTestSuite))
}
