// Package bigquery provides implementation of BigQuery data warehouse
package bigquery

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/d8a-tech/d8a/pkg/warehouse"
)

// PartitionInterval represents the time-based partition interval type for BigQuery tables.
type PartitionInterval string

const (
	// PartitionIntervalHour partitions by hour.
	PartitionIntervalHour PartitionInterval = "HOUR"
	// PartitionIntervalDay partitions by day (default).
	PartitionIntervalDay PartitionInterval = "DAY"
	// PartitionIntervalMonth partitions by month.
	PartitionIntervalMonth PartitionInterval = "MONTH"
	// PartitionIntervalYear partitions by year.
	PartitionIntervalYear PartitionInterval = "YEAR"
)

// PartitioningConfig represents the configuration for BigQuery table partitioning.
type PartitioningConfig struct {
	// Interval is the partition interval type (HOUR, DAY, MONTH, YEAR).
	Interval PartitionInterval
	// Field is the name of the field to partition by. Must be a top-level TIMESTAMP or DATE field.
	// If empty, partitioning uses the pseudo column _PARTITIONTIME.
	Field string
	// ExpirationDays is the number of days to keep the storage for a partition.
	// If 0, the data in the partitions do not expire.
	ExpirationDays int
}

// toBQTimePartitioning converts a PartitioningConfig to bigquery.TimePartitioning.
// Expiration is not set here as it must be set via ALTER TABLE query.
func toBQTimePartitioning(cfg PartitioningConfig) *bigquery.TimePartitioning {
	return &bigquery.TimePartitioning{
		Type:  bigquery.TimePartitioningType(cfg.Interval),
		Field: cfg.Field,
	}
}

// BigQueryTableDriverOption configures a BigQuery table driver.
type BigQueryTableDriverOption func(*bigQueryTableDriver)

// WithTableCreationTimeout sets how long the driver waits for a created table to become queryable.
func WithTableCreationTimeout(timeout time.Duration) BigQueryTableDriverOption {
	return func(d *bigQueryTableDriver) {
		d.tableCreationTimeout = timeout
	}
}

// WithQueryTimeout sets the timeout used for BigQuery metadata operations performed by the driver.
func WithQueryTimeout(timeout time.Duration) BigQueryTableDriverOption {
	return func(d *bigQueryTableDriver) {
		d.queryTimeout = timeout
	}
}

// WithPartitionByField enables partitioning by the specified field with default DAY interval and no expiration.
// Panics if field is empty.
func WithPartitionByField(field string) BigQueryTableDriverOption {
	if field == "" {
		panic("field is required for partitioning")
	}
	return func(d *bigQueryTableDriver) {
		cfg := PartitioningConfig{
			Interval:       PartitionIntervalDay,
			Field:          field,
			ExpirationDays: 0,
		}
		d.partitioning = &cfg
	}
}

// WithPartitionBy enables partitioning with the provided configuration.
// Panics if cfg.Field is empty.
func WithPartitionBy(cfg PartitioningConfig) BigQueryTableDriverOption {
	if cfg.Field == "" {
		panic("field is required for partitioning")
	}
	return func(d *bigQueryTableDriver) {
		d.partitioning = &cfg
	}
}

// NewBigQueryTableDriver creates a new BigQuery table driver.
func NewBigQueryTableDriver(
	db *bigquery.Client,
	dataset string,
	writer Writer,
	opts ...BigQueryTableDriverOption,
) warehouse.Driver {
	d := &bigQueryTableDriver{
		db:                   db,
		dataset:              dataset,
		fieldTypeMapper:      NewFieldTypeMapper(),
		queryTimeout:         30 * time.Second,
		tableCreationTimeout: 10 * time.Second,
		writer:               writer,
		typeComparer: warehouse.NewTypeComparer(
			int32Int64CompatibilityRule,
			float32Float64CompatibilityRule,
		),
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(d)
	}

	return d
}

var (
	// validIdentifierPattern matches BigQuery identifiers that contain only
	// alphanumeric characters, dots, underscores, hyphens, and Unicode letters: [a-zA-Z0-9._\-\p{L}]+
	validIdentifierPattern = regexp.MustCompile(`^[a-zA-Z0-9._\-\p{L}]+$`)
)

// validateBigQueryIdentifier validates that an identifier contains only
// allowed characters, preventing SQL injection.
func validateBigQueryIdentifier(identifier string) error {
	if identifier == "" {
		return errors.New("identifier cannot be empty")
	}
	if !validIdentifierPattern.MatchString(identifier) {
		return fmt.Errorf("identifier contains invalid characters: %q (only [a-zA-Z0-9._\\-\\p{L}] allowed)", identifier)
	}
	return nil
}

// escapeBigQueryIdentifier safely escapes a BigQuery identifier by wrapping it in backticks
// and escaping any backticks within the identifier. This prevents SQL injection when
// constructing DDL statements with user-provided table/dataset names.
// The identifier is validated before escaping to ensure it only contains safe characters.
func escapeBigQueryIdentifier(identifier string) (string, error) {
	if err := validateBigQueryIdentifier(identifier); err != nil {
		return "", err
	}
	// Escape backticks by doubling them, then wrap in backticks
	escaped := strings.ReplaceAll(identifier, "`", "``")
	return "`" + escaped + "`", nil
}
