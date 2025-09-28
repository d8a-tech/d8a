package schema

// ColumnsRegistry is a registry of columns for different properties.
type ColumnsRegistry interface {
	Get(propertyID string) (Columns, error)
}

type staticColumnsRegistry struct {
	columns        map[string]Columns
	defaultColumns Columns
}

func (m *staticColumnsRegistry) Get(propertyID string) (Columns, error) {
	columns, ok := m.columns[propertyID]
	if !ok {
		return m.defaultColumns, nil
	}
	return columns, nil
}

// NewStaticColumnsRegistry creates a new static columns registry with the given columns.
func NewStaticColumnsRegistry(columns map[string]Columns, defaultColumns Columns) ColumnsRegistry {
	return &staticColumnsRegistry{
		columns:        columns,
		defaultColumns: defaultColumns,
	}
}
