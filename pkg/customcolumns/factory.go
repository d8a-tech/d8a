package customcolumns

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/sirupsen/logrus"
)

// Factory builds runtime columns from one normalized definition.
type Factory interface {
	Build(def *properties.CustomColumnConfig) (BuiltColumn, error)
}

// BuiltColumn holds a single built column in one of the supported scopes.
type BuiltColumn struct {
	Event              schema.EventColumn
	Session            schema.SessionColumn
	SessionScopedEvent schema.SessionScopedEventColumn
}

type factory struct{}

type noValueMarker struct{}

type sourceScope int

const (
	sourceScopeEvent sourceScope = iota + 1
	sourceScopeSession
)

var noValue = noValueMarker{}

// NewFactory creates a factory for normalized nested-lookup custom columns.
func NewFactory() Factory {
	return &factory{}
}

func (f *factory) Build(def *properties.CustomColumnConfig) (BuiltColumn, error) {
	if err := validateDefinition(def); err != nil {
		return BuiltColumn{}, err
	}

	field, err := arrowField(def.Name, def.Type)
	if err != nil {
		return BuiltColumn{}, err
	}

	ifID := interfaceID(def)

	switch def.Scope {
	case properties.CustomColumnScopeEvent:
		return f.buildEventColumn(def, field, ifID)
	case properties.CustomColumnScopeSession:
		return f.buildSessionColumn(def, field, ifID)
	case properties.CustomColumnScopeSessionScopedEvent:
		return f.buildSessionScopedEventColumn(def, field, ifID)
	default:
		return BuiltColumn{}, fmt.Errorf("unsupported custom column scope %q", def.Scope)
	}
}

func (f *factory) buildEventColumn(
	def *properties.CustomColumnConfig,
	field *arrow.Field,
	ifID schema.InterfaceID,
) (BuiltColumn, error) {
	col := columns.NewSimpleEventColumn(
		ifID,
		field,
		func(event *schema.Event) (any, schema.D8AColumnWriteError) {
			value, ok := event.Values[def.Implementation.SourceField]
			if !ok {
				return noValue, nil
			}

			picked, pickErr := f.pickNested(def, value)
			if pickErr != nil {
				return nil, pickErr
			}

			return picked, nil
		},
		columns.WithEventColumnDependsOn(def.DependsOn),
		columns.WithEventColumnCast(func(value any) (any, schema.D8AColumnWriteError) {
			return f.cast(def.Type, value)
		}),
	)

	return BuiltColumn{Event: col}, nil
}

func (f *factory) buildSessionColumn(
	def *properties.CustomColumnConfig,
	field *arrow.Field,
	ifID schema.InterfaceID,
) (BuiltColumn, error) {
	col := columns.NewSimpleSessionColumn(
		ifID,
		field,
		func(session *schema.Session) (any, schema.D8AColumnWriteError) {
			values, getErr := f.collectSessionValues(def, session)
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
			return f.cast(def.Type, value)
		}),
	)

	return BuiltColumn{Session: col}, nil
}

func (f *factory) buildSessionScopedEventColumn(
	def *properties.CustomColumnConfig,
	field *arrow.Field,
	ifID schema.InterfaceID,
) (BuiltColumn, error) {
	col := columns.NewSimpleSessionScopedEventColumn(
		ifID,
		field,
		func(session *schema.Session, i int) (any, schema.D8AColumnWriteError) {
			if i < 0 || i >= len(session.Events) {
				return noValue, nil
			}

			value, ok := session.Events[i].Values[def.Implementation.SourceField]
			if !ok {
				return noValue, nil
			}

			picked, pickErr := f.pickNested(def, value)
			if pickErr != nil {
				return nil, pickErr
			}

			return picked, nil
		},
		columns.WithSessionScopedEventColumnDependsOn(def.DependsOn),
		columns.WithSessionScopedEventColumnCast(ifID, func(value any) (any, schema.D8AColumnWriteError) {
			return f.cast(def.Type, value)
		}),
	)

	return BuiltColumn{SessionScopedEvent: col}, nil
}

func (f *factory) collectSessionValues(
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

		picked, err := f.pickNested(def, value)
		if err != nil {
			return nil, err
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

		picked, err := f.pickNested(def, value)
		if err != nil {
			return nil, err
		}
		if isNoValue(picked) {
			continue
		}

		values = append(values, picked)
	}

	return values, nil
}

func (f *factory) pickNested(def *properties.CustomColumnConfig, value any) (any, schema.D8AColumnWriteError) {
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

		val, ok := row[def.Implementation.ValueField]
		if !ok {
			continue
		}

		candidates = append(candidates, val)
	}

	picked, ok := f.pickByConfig(def.Implementation.Pick, candidates)
	if !ok {
		return noValue, nil
	}

	return picked, nil
}

func (f *factory) cast(columnType properties.CustomColumnType, value any) (any, schema.D8AColumnWriteError) {
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

func (f *factory) pickByConfig(strategy properties.NestedLookupPickStrategy, values []any) (any, bool) {
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
		case int64:
			return typed, nil
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
		case int64:
			return float64(typed), nil
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
		properties.CustomColumnScopeSession,
		properties.CustomColumnScopeSessionScopedEvent:
	default:
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

	if def.Scope == properties.CustomColumnScopeSessionScopedEvent &&
		isSessionCustomColumnSource(def) {
		return fmt.Errorf(
			"session_scoped_event custom columns cannot use session custom columns as source",
		)
	}

	if def.Scope == properties.CustomColumnScopeSessionScopedEvent &&
		def.Implementation.SourceScope == properties.NestedLookupSourceScopeSession {
		return fmt.Errorf("session_scoped_event custom columns cannot use source_scope=session")
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
	if def.Implementation.SourceScope != "" {
		switch def.Implementation.SourceScope {
		case properties.NestedLookupSourceScopeEvent:
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

	sourceID := def.Implementation.SourceInterfaceID
	if sourceID == "" {
		sourceID = def.DependsOn.Interface
	}

	if sourceID == "" {
		return sourceScopeEvent, nil
	}

	logrus.Warnf(
		"custom column %q uses deprecated implementation.source_interface_id fallback; "+
			"set implementation.source_scope instead",
		def.Name,
	)

	source := string(sourceID)
	if strings.Contains(source, "/session/") {
		return sourceScopeSession, nil
	}
	if strings.Contains(source, "/event/") {
		return sourceScopeEvent, nil
	}

	return 0, fmt.Errorf("cannot infer source scope from implementation.source_interface_id %q", sourceID)
}

func isSessionCustomColumnSource(def *properties.CustomColumnConfig) bool {
	sourceID := def.Implementation.SourceInterfaceID
	if sourceID == "" {
		sourceID = def.DependsOn.Interface
	}

	return strings.HasPrefix(string(sourceID), "customcolumns.d8a.tech/session/")
}
