package sessions

import (
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/sirupsen/logrus"
)

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

type brokenFilteringLayout struct {
	layout schema.Layout
}

func (l *brokenFilteringLayout) Tables(columns schema.Columns) []schema.WithName {
	return l.layout.Tables(columns)
}

func (l *brokenFilteringLayout) ToRows(
	columns schema.Columns,
	sessions ...*schema.Session,
) ([]schema.TableRows, error) {
	newSessions := make([]*schema.Session, 0, len(sessions))
	for _, session := range sessions {
		if session.IsBroken {
			logrus.Warnf("skipping write for broken session: %s", session.BrokenReason)
			continue
		}

		// Filter events in-place
		writeIndex := 0
		for _, event := range session.Events {
			if event.IsBroken {
				logrus.Warnf("skipping write for broken event: %s", event.BrokenReason)
				continue
			}
			session.Events[writeIndex] = event
			writeIndex++
		}
		session.Events = session.Events[:writeIndex]

		if len(session.Events) > 0 {
			newSessions = append(newSessions, session)
		}
	}
	return l.layout.ToRows(columns, newSessions...)
}

func NewBrokenFilteringSchemaLayout(layout schema.Layout) schema.Layout {
	return &brokenFilteringLayout{
		layout: layout,
	}
}
