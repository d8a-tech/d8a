package properties

import (
	"github.com/d8a-tech/d8a/pkg/schema"
)

// CustomColumnScope defines where the resulting column is written.
type CustomColumnScope string

const (
	CustomColumnScopeEvent              CustomColumnScope = "event"
	CustomColumnScopeSession            CustomColumnScope = "session"
	CustomColumnScopeSessionScopedEvent CustomColumnScope = "session_scoped_event"
)

// CustomColumnType defines output type for the resulting column.
type CustomColumnType string

const (
	CustomColumnTypeString  CustomColumnType = "string"
	CustomColumnTypeInt64   CustomColumnType = "int64"
	CustomColumnTypeFloat64 CustomColumnType = "float64"
	CustomColumnTypeBool    CustomColumnType = "bool"
)

// NestedLookupPickStrategy defines deterministic value-picking strategy for repeated sources.
type NestedLookupPickStrategy string

const (
	NestedLookupPickStrategyLastNonNull NestedLookupPickStrategy = "last_non_null"
)

// NestedLookupSourceScope defines where the source records are read from.
type NestedLookupSourceScope string

const (
	NestedLookupSourceScopeEvent   NestedLookupSourceScope = "event"
	NestedLookupSourceScopeSession NestedLookupSourceScope = "session"
)

// NestedLookupConfig stores normalized nested source lookup details.
type NestedLookupConfig struct {
	SourceScope       NestedLookupSourceScope
	SourceInterfaceID schema.InterfaceID
	SourceField       string
	MatchField        string
	MatchEquals       any
	ValueField        string
	Pick              NestedLookupPickStrategy
}

// CustomColumnConfig is normalized custom-column config used by runtime builders.
type CustomColumnConfig struct {
	Name           string
	Scope          CustomColumnScope
	Type           CustomColumnType
	DependsOn      schema.DependsOnEntry
	Implementation NestedLookupConfig
}
