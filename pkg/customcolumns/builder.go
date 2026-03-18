package customcolumns

import (
	"fmt"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// BuiltColumn holds a single built column in one of the supported scopes.
type BuiltColumn struct {
	Event              schema.EventColumn
	Session            schema.SessionColumn
	SessionScopedEvent schema.SessionScopedEventColumn
}

// ColumnBuilder builds runtime columns from normalized custom column configs.
type ColumnBuilder interface {
	Build(defs []*properties.CustomColumnConfig) ([]BuiltColumn, error)
}

type noValueMarker struct{}

type sourceScope int

const (
	sourceScopeEvent sourceScope = iota + 1
	sourceScopeSession
)

var noValue = noValueMarker{}

type multiColumnBuilder struct {
	subBuilders []ColumnBuilder
}

// NewMultiColumnBuilder creates a builder that runs all sub-builders.
func NewMultiColumnBuilder(subBuilders ...ColumnBuilder) ColumnBuilder {
	builders := make([]ColumnBuilder, 0, len(subBuilders))
	for i := range subBuilders {
		if subBuilders[i] == nil {
			continue
		}
		builders = append(builders, subBuilders[i])
	}

	return &multiColumnBuilder{subBuilders: builders}
}

func (b *multiColumnBuilder) Build(defs []*properties.CustomColumnConfig) ([]BuiltColumn, error) {
	for i := range defs {
		def := defs[i]
		if def == nil {
			return nil, fmt.Errorf("build custom column at index %d: definition is nil", i)
		}

		if err := validateDefinition(def); err != nil {
			return nil, fmt.Errorf("build custom column %q at index %d: %w", def.Name, i, err)
		}
	}

	built := make([]BuiltColumn, 0)

	for i := range b.subBuilders {
		columns, err := b.subBuilders[i].Build(defs)
		if err != nil {
			return nil, err
		}
		built = append(built, columns...)
	}

	return built, nil
}

type eventColumnBuilder struct{}

// NewEventColumnBuilder creates a builder for event-scoped custom columns.
func NewEventColumnBuilder() ColumnBuilder {
	return &eventColumnBuilder{}
}

func (b *eventColumnBuilder) Build(defs []*properties.CustomColumnConfig) ([]BuiltColumn, error) {
	built := make([]BuiltColumn, 0, len(defs))

	for i := range defs {
		def := defs[i]
		if def == nil || def.Scope != properties.CustomColumnScopeEvent {
			continue
		}

		field, err := arrowField(def.Name, def.Type)
		if err != nil {
			return nil, fmt.Errorf("build custom column %q at index %d: %w", def.Name, i, err)
		}

		col := columns.NewSimpleEventColumn(
			interfaceID(def),
			field,
			func(event *schema.Event) (any, schema.D8AColumnWriteError) {
				value, ok := event.Values[def.Implementation.SourceField]
				if !ok {
					return noValue, nil
				}

				picked, pickErr := pickNested(def, value)
				if pickErr != nil {
					return nil, pickErr
				}

				return picked, nil
			},
			columns.WithEventColumnDependsOn(def.DependsOn),
			columns.WithEventColumnCast(func(value any) (any, schema.D8AColumnWriteError) {
				return cast(def.Type, value)
			}),
		)

		built = append(built, BuiltColumn{Event: col})
	}

	return built, nil
}

type sessionColumnBuilder struct{}

// NewSessionColumnBuilder creates a builder for session-scoped custom columns.
func NewSessionColumnBuilder() ColumnBuilder {
	return &sessionColumnBuilder{}
}

func (b *sessionColumnBuilder) Build(defs []*properties.CustomColumnConfig) ([]BuiltColumn, error) {
	built := make([]BuiltColumn, 0, len(defs))

	for i := range defs {
		def := defs[i]
		if def == nil || def.Scope != properties.CustomColumnScopeSession {
			continue
		}

		field, err := arrowField(def.Name, def.Type)
		if err != nil {
			return nil, fmt.Errorf("build custom column %q at index %d: %w", def.Name, i, err)
		}

		col := columns.NewSimpleSessionColumn(
			interfaceID(def),
			field,
			func(session *schema.Session) (any, schema.D8AColumnWriteError) {
				values, getErr := collectSessionValues(def, session)
				if getErr != nil {
					return nil, getErr
				}

				picked, ok := pickLastNonNull(values)
				if !ok {
					return noValue, nil
				}

				return picked, nil
			},
			columns.WithSessionColumnDependsOn(def.DependsOn),
			columns.WithSessionColumnCast(func(value any) (any, schema.D8AColumnWriteError) {
				return cast(def.Type, value)
			}),
		)

		built = append(built, BuiltColumn{Session: col})
	}

	return built, nil
}

func collectSessionValues(
	def *properties.CustomColumnConfig,
	session *schema.Session,
) ([]any, schema.D8AColumnWriteError) {
	source, err := resolveSessionSourceScope(def)
	if err != nil {
		return nil, schema.NewBrokenEventError(err.Error())
	}

	if source == sourceScopeSession {
		value, ok := session.Values[def.Implementation.SourceField]
		if !ok {
			return []any{}, nil
		}

		picked, pickErr := pickNested(def, value)
		if pickErr != nil {
			return nil, pickErr
		}
		if isNoValue(picked) {
			return []any{}, nil
		}
		return []any{picked}, nil
	}

	values := make([]any, 0, len(session.Events))
	for i := range session.Events {
		event := session.Events[i]
		value, ok := event.Values[def.Implementation.SourceField]
		if !ok {
			continue
		}

		picked, pickErr := pickNested(def, value)
		if pickErr != nil {
			return nil, pickErr
		}
		if isNoValue(picked) {
			continue
		}

		values = append(values, picked)
	}

	return values, nil
}

func pickNested(def *properties.CustomColumnConfig, value any) (any, schema.D8AColumnWriteError) {
	rows, err := readRepeatedRecords(value)
	if err != nil {
		return nil, schema.NewBrokenEventError(fmt.Sprintf("read repeated records: %s", err))
	}

	candidates := make([]any, 0, len(rows))
	for _, row := range rows {
		matched, ok := row[def.Implementation.MatchField]
		if !ok || matched != def.Implementation.MatchEquals {
			continue
		}

		picked, ok := row[def.Implementation.ValueField]
		if !ok {
			continue
		}

		candidates = append(candidates, picked)
	}

	picked, ok := pickByConfig(def.Implementation.Pick, candidates)
	if !ok {
		return noValue, nil
	}

	return picked, nil
}

func cast(columnType properties.CustomColumnType, value any) (any, schema.D8AColumnWriteError) {
	if isNoValue(value) {
		return nilValue(), nil
	}

	casted, err := castValue(columnType, value)
	if err != nil {
		return nil, err
	}

	if isNoValue(casted) {
		return nilValue(), nil
	}

	return casted, nil
}

func pickByConfig(strategy properties.NestedLookupPickStrategy, values []any) (any, bool) {
	switch strategy {
	case "", properties.NestedLookupPickStrategyLastNonNull:
		return pickLastNonNull(values)
	default:
		return nil, false
	}
}

func readRepeatedRecords(value any) ([]map[string]any, error) {
	if value == nil || isNoValue(value) {
		return nil, nil
	}

	switch typed := value.(type) {
	case []map[string]any:
		return typed, nil
	case []any:
		out := make([]map[string]any, 0, len(typed))
		for _, item := range typed {
			row, ok := item.(map[string]any)
			if !ok {
				continue
			}
			out = append(out, row)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("expected repeated records as []any or []map[string]any, got %T", value)
	}
}

func pickLastNonNull(values []any) (any, bool) {
	for i := len(values) - 1; i >= 0; i-- {
		if values[i] != nil {
			return values[i], true
		}
	}

	return nil, false
}

func castValue(columnType properties.CustomColumnType, value any) (any, schema.D8AColumnWriteError) {
	if value == nil || isNoValue(value) {
		return noValue, nil
	}

	switch columnType {
	case properties.CustomColumnTypeString:
		return columns.StrNilIfErrorOrEmpty(columns.CastToString(""))(value)
	case properties.CustomColumnTypeInt64:
		switch typed := value.(type) {
		case float64:
			return int64(typed), nil
		case string:
			if typed == "" {
				return noValue, nil
			}
			parsed, err := strconv.ParseInt(typed, 10, 64)
			if err != nil {
				return noValue, nil
			}
			return parsed, nil
		default:
			return noValue, nil
		}
	case properties.CustomColumnTypeFloat64:
		switch typed := value.(type) {
		case float64:
			return typed, nil
		case string:
			if typed == "" {
				return noValue, nil
			}
			parsed, err := strconv.ParseFloat(typed, 64)
			if err != nil {
				return noValue, nil
			}
			return parsed, nil
		default:
			return noValue, nil
		}
	case properties.CustomColumnTypeBool:
		return columns.CastToBool("")(value)
	default:
		return nil, schema.NewBrokenEventError(fmt.Sprintf("unsupported custom column type %q", columnType))
	}
}

func arrowField(name string, typ properties.CustomColumnType) (*arrow.Field, error) {
	if name == "" {
		return nil, fmt.Errorf("custom column name is required")
	}

	switch typ {
	case properties.CustomColumnTypeString:
		return &arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: true}, nil
	case properties.CustomColumnTypeInt64:
		return &arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int64, Nullable: true}, nil
	case properties.CustomColumnTypeFloat64:
		return &arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Float64, Nullable: true}, nil
	case properties.CustomColumnTypeBool:
		return &arrow.Field{Name: name, Type: arrow.FixedWidthTypes.Boolean, Nullable: true}, nil
	default:
		return nil, fmt.Errorf("unsupported custom column type %q", typ)
	}
}

func interfaceID(def *properties.CustomColumnConfig) schema.InterfaceID {
	return schema.InterfaceID("customcolumns.d8a.tech/" + string(def.Scope) + "/" + def.Name)
}

func isNoValue(value any) bool {
	_, ok := value.(noValueMarker)
	return ok
}

func nilValue() any {
	return nil
}

func validateDefinition(def *properties.CustomColumnConfig) error {
	if def.Name == "" {
		return fmt.Errorf("custom column name is required")
	}
	if def.Scope == "" {
		return fmt.Errorf("custom column scope is required")
	}
	if def.Type == "" {
		return fmt.Errorf("custom column type is required")
	}
	if def.DependsOn.Interface == "" {
		return fmt.Errorf("custom column depends_on.interface is required")
	}
	if def.Implementation.SourceField == "" {
		return fmt.Errorf("custom column implementation.source_field is required")
	}
	if def.Implementation.MatchField == "" {
		return fmt.Errorf("custom column implementation.match_field is required")
	}
	if def.Implementation.ValueField == "" {
		return fmt.Errorf("custom column implementation.value_field is required")
	}

	switch def.Scope {
	case properties.CustomColumnScopeEvent,
		properties.CustomColumnScopeSession:
	default:
		if def.Scope == properties.CustomColumnScopeSessionScopedEvent {
			return fmt.Errorf("custom column scope %q is not supported by nested lookup custom columns", def.Scope)
		}
		return fmt.Errorf("unsupported custom column scope %q", def.Scope)
	}

	switch def.Type {
	case properties.CustomColumnTypeString,
		properties.CustomColumnTypeInt64,
		properties.CustomColumnTypeFloat64,
		properties.CustomColumnTypeBool:
	default:
		return fmt.Errorf("unsupported custom column type %q", def.Type)
	}

	if def.Scope == properties.CustomColumnScopeEvent {
		switch def.Implementation.SourceScope {
		case "", properties.NestedLookupSourceScopeEvent:
		default:
			return fmt.Errorf("event custom columns cannot use source_scope=%q", def.Implementation.SourceScope)
		}

		if def.Implementation.Pick != "" {
			return fmt.Errorf("event custom columns cannot use implementation.pick")
		}
	}

	if def.Scope == properties.CustomColumnScopeSession {
		if _, err := resolveSessionSourceScope(def); err != nil {
			return err
		}
	}

	switch def.Implementation.Pick {
	case "", properties.NestedLookupPickStrategyLastNonNull:
		return nil
	default:
		return fmt.Errorf("unsupported pick strategy %q", def.Implementation.Pick)
	}
}

func resolveSessionSourceScope(def *properties.CustomColumnConfig) (sourceScope, error) {
	switch def.Implementation.SourceScope {
	case "", properties.NestedLookupSourceScopeEvent:
		return sourceScopeEvent, nil
	case properties.NestedLookupSourceScopeSession:
		return sourceScopeSession, nil
	default:
		return 0, fmt.Errorf(
			"unsupported custom column implementation.source_scope %q",
			def.Implementation.SourceScope,
		)
	}
}
