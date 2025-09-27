package schema

// merger merges multiple columns registries into a single one.
type merger struct {
	registries []ColumnsRegistry
	sorter     *DependencySorter
}

func (m *merger) Get(propertyID string) (Columns, error) {
	allColumns := Columns{
		Session: make([]SessionColumn, 0),
		Event:   make([]EventColumn, 0),
	}
	for _, registry := range m.registries {
		columns, err := registry.Get(propertyID)
		if err != nil {
			return Columns{}, err
		}
		allColumns.Session = append(allColumns.Session, columns.Session...)
		allColumns.Event = append(allColumns.Event, columns.Event...)
	}
	sortedColumns, err := m.sorter.SortAllColumns(allColumns)
	if err != nil {
		return Columns{}, err
	}
	return sortedColumns, nil
}

// NewColumnsMerger creates a new columns merger.
func NewColumnsMerger(registries []ColumnsRegistry) ColumnsRegistry {
	return &merger{
		registries: registries,
		sorter:     NewDependencySorter(),
	}
}
