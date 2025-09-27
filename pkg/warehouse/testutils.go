package warehouse

import (
	"context"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
)

// MockWarehouseDriver implements Driver interface for testing
type MockWarehouseDriver struct {
	mu                sync.Mutex
	WriteCallCount    int
	WriteErrors       []error
	WriteCalls        []WriteCall
	CreateTableCalls  []CreateTableCall
	AddColumnCalls    []AddColumnCall
	MissingColumnResp []MissingColumnResp
}

// WriteCall is a call to the Write method of the MockWarehouseDriver
type WriteCall struct {
	Table   string
	Schema  *arrow.Schema
	Records []map[string]any
}

// CreateTableCall is a call to the CreateTable method of the MockWarehouseDriver
type CreateTableCall struct {
	table  string
	schema *arrow.Schema
}

// AddColumnCall is a call to the AddColumn method of the MockWarehouseDriver
type AddColumnCall struct {
	table string
	field *arrow.Field
}

// MissingColumnResp is a response to the MissingColumns method of the MockWarehouseDriver
type MissingColumnResp struct {
	fields []*arrow.Field
	err    error
}

// Write implements warehouse.Driver
func (m *MockWarehouseDriver) Write(
	_ context.Context,
	table string,
	schema *arrow.Schema,
	records []map[string]any,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.WriteCalls = append(m.WriteCalls, WriteCall{
		Table:   table,
		Schema:  schema,
		Records: records,
	})

	var err error
	if m.WriteCallCount < len(m.WriteErrors) {
		err = m.WriteErrors[m.WriteCallCount]
	}

	m.WriteCallCount++
	return err
}

// CreateTable implements warehouse.Driver
func (m *MockWarehouseDriver) CreateTable(table string, schema *arrow.Schema) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.CreateTableCalls = append(m.CreateTableCalls, CreateTableCall{
		table:  table,
		schema: schema,
	})
	return nil
}

// AddColumn implements warehouse.Driver
func (m *MockWarehouseDriver) AddColumn(table string, field *arrow.Field) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.AddColumnCalls = append(m.AddColumnCalls, AddColumnCall{
		table: table,
		field: field,
	})
	return nil
}

// MissingColumns implements warehouse.Driver
func (m *MockWarehouseDriver) MissingColumns(
	_ string,
	_ *arrow.Schema,
) ([]*arrow.Field, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.MissingColumnResp) > 0 {
		resp := m.MissingColumnResp[0]
		m.MissingColumnResp = m.MissingColumnResp[1:]
		return resp.fields, resp.err
	}
	return nil, nil
}

// GetWriteCalls returns the write calls
func (m *MockWarehouseDriver) GetWriteCalls() []WriteCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]WriteCall(nil), m.WriteCalls...)
}

// GetWriteCallCount returns the number of write calls
func (m *MockWarehouseDriver) GetWriteCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.WriteCallCount
}
