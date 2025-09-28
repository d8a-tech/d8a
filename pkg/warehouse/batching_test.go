package warehouse

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchingDriverWrite(t *testing.T) {
	// given - test data setup
	testSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	testRecords := []map[string]any{
		{"id": int64(1), "name": "test1"},
		{"id": int64(2), "name": "test2"},
	}

	tests := []struct {
		name          string
		maxBatchSize  int
		interval      time.Duration
		tableName     string
		schema        *arrow.Schema
		records       []map[string]any
		writeErrors   []error
		expectedError bool
		setupFunc     func(t *testing.T, driver *batchingDriver, mock *MockWarehouseDriver)
		assertFunc    func(t *testing.T, driver *batchingDriver, mock *MockWarehouseDriver)
	}{
		{
			name:          "successful_write_new_table",
			maxBatchSize:  10,
			interval:      10 * time.Millisecond,
			tableName:     "test_table",
			schema:        testSchema,
			records:       testRecords,
			expectedError: false,
			assertFunc: func(t *testing.T, _ *batchingDriver, mock *MockWarehouseDriver) {
				// then - should create table properties and wait for flush
				require.Eventually(t, func() bool {
					return mock.GetWriteCallCount() == 1
				}, time.Second, 10*time.Millisecond, "should call underlying Write once")

				writeCalls := mock.GetWriteCalls()
				require.Len(t, writeCalls, 1)
				assert.Equal(t, "test_table", writeCalls[0].Table)
				assert.Equal(t, testRecords, writeCalls[0].Records)
			},
		},
		{
			name:          "successful_write_existing_table",
			maxBatchSize:  10,
			interval:      10 * time.Millisecond,
			tableName:     "existing_table",
			schema:        testSchema,
			records:       testRecords,
			expectedError: false,
			setupFunc: func(_ *testing.T, driver *batchingDriver, _ *MockWarehouseDriver) {
				// Create table properties beforehand
				driver.lock.Lock()
				driver.tableProps["existing_table"] = &tableProps{
					schema:           testSchema,
					currentBatchSize: 0,
				}
				driver.lock.Unlock()
			},
			assertFunc: func(t *testing.T, _ *batchingDriver, mock *MockWarehouseDriver) {
				// then - should append to existing batch
				require.Eventually(t, func() bool {
					return mock.GetWriteCallCount() == 1
				}, time.Second, 10*time.Millisecond, "should call underlying Write once")

				writeCalls := mock.GetWriteCalls()
				require.Len(t, writeCalls, 1)
				assert.Equal(t, "existing_table", writeCalls[0].Table)
				assert.Equal(t, testRecords, writeCalls[0].Records)
			},
		},
		{
			name:          "write_error_propagation",
			maxBatchSize:  10,
			interval:      10 * time.Millisecond,
			tableName:     "error_table",
			schema:        testSchema,
			records:       testRecords,
			writeErrors:   []error{errors.New("write failed")},
			expectedError: false, // Batcher saves the data to the set, so it's not lost
			assertFunc: func(t *testing.T, _ *batchingDriver, mock *MockWarehouseDriver) {
				// then - should receive error from underlying driver
				require.Eventually(t, func() bool {
					return mock.GetWriteCallCount() > 1
				}, time.Second, 10*time.Millisecond, "should call underlying Write once")
			},
		},
		{
			name:          "empty_records_batch",
			maxBatchSize:  10,
			interval:      10 * time.Millisecond,
			tableName:     "empty_table",
			schema:        testSchema,
			records:       []map[string]any{},
			expectedError: false,
			assertFunc: func(t *testing.T, _ *batchingDriver, mock *MockWarehouseDriver) {
				assert.Equal(t, 0, mock.GetWriteCallCount(), "should not call underlying Write for empty records")
			},
		},
		{
			name:         "multiple_records_single_batch",
			maxBatchSize: 10,
			interval:     10 * time.Millisecond,
			tableName:    "multi_table",
			schema:       testSchema,
			records: []map[string]any{
				{"id": int64(1), "name": "test1"},
				{"id": int64(2), "name": "test2"},
				{"id": int64(3), "name": "test3"},
			},
			expectedError: false,
			assertFunc: func(t *testing.T, _ *batchingDriver, mock *MockWarehouseDriver) {
				// then - should batch all records together
				require.Eventually(t, func() bool {
					return mock.GetWriteCallCount() == 1
				}, time.Second, 10*time.Millisecond, "should call underlying Write once")

				writeCalls := mock.GetWriteCalls()
				require.Len(t, writeCalls, 1)
				assert.Len(t, writeCalls[0].Records, 3, "should batch all records together")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			mock := &MockWarehouseDriver{
				WriteErrors: tt.writeErrors,
			}

			stopCh := make(chan struct{})
			defer close(stopCh)

			driver, ok := NewBatchingDriver(
				mock,
				tt.maxBatchSize,
				tt.interval,
				storage.NewInMemorySet(),
				stopCh,
			).(*batchingDriver)

			if tt.setupFunc != nil {
				tt.setupFunc(t, driver, mock)
			}

			// when - run write in goroutine to avoid deadlock
			errCh := make(chan error, 1)
			go func() {
				errCh <- driver.Write(context.Background(), tt.tableName, tt.schema, tt.records)
			}()

			// Wait for write to complete or timeout
			var err error
			select {
			case err = <-errCh:
			case <-time.After(2 * time.Second):
				t.Fatal("Write operation timed out")
			}

			// then
			require.True(t, ok, "NewBatchingDriver should return *batchingDriver")
			if tt.expectedError {
				assert.Error(t, err, "should return error when underlying driver fails")
			} else {
				assert.NoError(t, err, "should not return error for successful write")
			}

			if tt.assertFunc != nil {
				// Give some time for background flush to complete
				time.Sleep(tt.interval + 50*time.Millisecond)
				tt.assertFunc(t, driver, mock)
			}
		})
	}
}

func TestBatchingDriverWriteTableIsolation(t *testing.T) {
	// given
	mock := &MockWarehouseDriver{}
	stopCh := make(chan struct{})
	defer close(stopCh)

	driver := NewBatchingDriver(mock, 10, 50*time.Millisecond, storage.NewInMemorySet(), stopCh)

	testSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	table1Records := []map[string]any{{"id": int64(1)}}
	table2Records := []map[string]any{{"id": int64(2)}}

	// when - write to different tables in goroutines to avoid deadlock
	var wg sync.WaitGroup
	var err1, err2 error

	wg.Add(2)
	go func() {
		defer wg.Done()
		err1 = driver.Write(context.Background(), "table1", testSchema, table1Records)
	}()
	go func() {
		defer wg.Done()
		err2 = driver.Write(context.Background(), "table2", testSchema, table2Records)
	}()

	wg.Wait()

	// then - both writes should succeed

	assert.NoError(t, err1, "write to table1 should succeed")
	assert.NoError(t, err2, "write to table2 should succeed")

	// Wait for background flush
	require.Eventually(t, func() bool {
		return mock.GetWriteCallCount() >= 2
	}, 2*time.Second, 10*time.Millisecond, "should call underlying Write for both tables")

	// Verify table isolation
	writeCalls := mock.GetWriteCalls()
	tableNames := make(map[string]bool)
	for _, call := range writeCalls {
		tableNames[call.Table] = true
	}

	assert.True(t, tableNames["table1"], "should write to table1")
	assert.True(t, tableNames["table2"], "should write to table2")
	assert.Len(t, tableNames, 2, "should write to exactly 2 tables")
}
