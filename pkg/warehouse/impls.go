package warehouse

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

type consoleDriver struct {
}

func (d *consoleDriver) CreateTable(_ string, _ *arrow.Schema) error {
	return nil
}

func (d *consoleDriver) AddColumn(_ string, _ *arrow.Field) error {
	return nil
}

func (d *consoleDriver) Write(_ context.Context, _ string, _ *arrow.Schema, rows []map[string]any) error {
	for _, row := range rows {
		rowJSON, err := json.Marshal(row)
		if err != nil {
			return fmt.Errorf("failed to marshal row to JSON: %w", err)
		}
		fmt.Println(string(rowJSON))
	}
	return nil
}

func (d *consoleDriver) MissingColumns(_ string, _ *arrow.Schema) ([]*arrow.Field, error) {
	return []*arrow.Field{}, nil
}

// NewConsoleDriver creates a new console driver that prints data to stdout.
func NewConsoleDriver() Driver {
	return &consoleDriver{}
}

// MockWrittenRows is a collection of rows written to a single table in mock driver.
type MockWrittenRows struct {
	Table string
	Rows  []map[string]any
}

// MockDriver is a mock driver that stores written rows in memory.
type MockDriver struct {
	Writes     []MockWrittenRows
	WriteError error
}

var _ Driver = &MockDriver{}

// CreateTable implements Driver
func (d *MockDriver) CreateTable(_ string, _ *arrow.Schema) error {
	return nil
}

// AddColumn implements Driver
func (d *MockDriver) AddColumn(_ string, _ *arrow.Field) error {
	return nil
}

// Write implements Driver
func (d *MockDriver) Write(_ context.Context, table string, _ *arrow.Schema, rows []map[string]any) error {
	d.Writes = append(d.Writes, MockWrittenRows{
		Table: table,
		Rows:  rows,
	})
	return d.WriteError
}

// MissingColumns implements Driver
func (d *MockDriver) MissingColumns(_ string, _ *arrow.Schema) ([]*arrow.Field, error) {
	return []*arrow.Field{}, nil
}

// NewMockDriver creates a new mock driver that stores written rows in memory.
func NewMockDriver() *MockDriver {
	return &MockDriver{}
}
