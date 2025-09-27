package sessions

import "github.com/d8a-tech/d8a/pkg/schema"

type batchingLayout struct {
	layout    schema.Layout
	batchSize int
}

func (l *batchingLayout) Tables(columns schema.Columns) []schema.WithName {
	return l.layout.Tables(columns)
}

func (l *batchingLayout) ToRows(columns schema.Columns, sessions ...*schema.Session) ([]schema.TableRows, error) {
	tableRows, err := l.layout.ToRows(columns, sessions...)
	if err != nil {
		return nil, err
	}

	tableRowsBlueprint := map[string][]map[string]any{}
	for _, tableRow := range tableRows {
		_, ok := tableRowsBlueprint[tableRow.Table]
		if !ok {
			tableRowsBlueprint[tableRow.Table] = []map[string]any{}
		}
		tableRowsBlueprint[tableRow.Table] = append(tableRowsBlueprint[tableRow.Table], tableRow.Rows...)
	}

	newTableRows := []schema.TableRows{}
	for table, rows := range tableRowsBlueprint {
		newTableRows = append(newTableRows, schema.TableRows{
			Table: table,
			Rows:  rows,
		})
	}

	return newTableRows, nil
}

// NewBatchingSchemaLayout creates a new layout that batches the sessions into
// smaller tables.
func NewBatchingSchemaLayout(layout schema.Layout, batchSize int) schema.Layout {
	return &batchingLayout{
		layout:    layout,
		batchSize: batchSize,
	}
}
