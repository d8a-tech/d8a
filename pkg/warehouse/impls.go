package warehouse

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/sirupsen/logrus"
)

type noopDriver struct {
}

func (d *noopDriver) CreateTable(_ string, _ *arrow.Schema) error {
	return nil
}

func (d *noopDriver) AddColumn(_ string, _ *arrow.Field) error {
	return nil
}

func (d *noopDriver) Write(_ context.Context, _ string, _ *arrow.Schema, _ []map[string]any) error {
	return nil
}

func (d *noopDriver) MissingColumns(_ string, _ *arrow.Schema) ([]*arrow.Field, error) {
	return []*arrow.Field{}, nil
}

// NewNoopDriver creates a new noop driver that does nothing.
func NewNoopDriver() Driver {
	return &noopDriver{}
}

type consoleDriver struct {
	driver Driver
}

func (d *consoleDriver) CreateTable(table string, schema *arrow.Schema) error {
	return d.driver.CreateTable(table, schema)
}

func (d *consoleDriver) AddColumn(table string, field *arrow.Field) error {
	return d.driver.AddColumn(table, field)
}

func (d *consoleDriver) Write(ctx context.Context, table string, schema *arrow.Schema, rows []map[string]any) error {
	for _, row := range rows {
		rowJSON, err := json.Marshal(row)
		if err != nil {
			return fmt.Errorf("failed to marshal row to JSON: %w", err)
		}
		fmt.Println(string(rowJSON))
	}
	return d.driver.Write(ctx, table, schema, rows)
}

func (d *consoleDriver) MissingColumns(table string, schema *arrow.Schema) ([]*arrow.Field, error) {
	return d.driver.MissingColumns(table, schema)
}

// NewConsoleDriver creates a new console driver that prints data to stdout.
func NewConsoleDriver() Driver {
	return &consoleDriver{
		driver: NewNoopDriver(),
	}
}

// NewDebuggingDriver creates a new logging driver that logs all writes JSON formatted to stdout.
func NewDebuggingDriver(driver Driver) Driver {
	return &consoleDriver{
		driver: driver,
	}
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

type loggingDriver struct {
	driver Driver
}

func (d *loggingDriver) Write(ctx context.Context, table string, schema *arrow.Schema, rows []map[string]any) error {
	logrus.Infof("writing `%d` records to `%s`", len(rows), table)
	return d.driver.Write(ctx, table, schema, rows)
}

func (d *loggingDriver) AddColumn(table string, field *arrow.Field) error {
	logrus.Infof("adding column `%s` to `%s`", field.Name, table)
	return d.driver.AddColumn(table, field)
}

func (d *loggingDriver) CreateTable(table string, schema *arrow.Schema) error {
	logrus.Infof("creating table `%s`", table)
	return d.driver.CreateTable(table, schema)
}

func (d *loggingDriver) MissingColumns(table string, schema *arrow.Schema) ([]*arrow.Field, error) {
	logrus.Infof("checking for missing columns in `%s`", table)
	return d.driver.MissingColumns(table, schema)
}

// NewLoggingDriver creates a new driver that logs all writes.
func NewLoggingDriver(driver Driver) Driver {
	return &loggingDriver{driver: driver}
}
