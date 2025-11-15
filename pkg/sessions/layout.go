package sessions

import "github.com/d8a-tech/d8a/pkg/schema"

type batchingLayout struct {
	layout schema.Layout
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

// NewBatchingSchemaLayout creates a new layout that makes sure  that all the rows
// from a single table will be written to the warehouse in a single call.
func NewBatchingSchemaLayout(layout schema.Layout) schema.Layout {
	return &batchingLayout{
		layout: layout,
	}
}
