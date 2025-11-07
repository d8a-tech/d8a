package schema

import (
	"errors"

	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/sirupsen/logrus"
)

// Guard ensures that warehouse tables have the correct schema and columns.
type Guard struct {
	warehouseRegistry warehouse.Registry

	columnsColumns ColumnsRegistry
	layout         LayoutRegistry
	ordering       *InterfaceOrdering
}

// NewGuard creates a new Guard
func NewGuard(
	warehouseRegistry warehouse.Registry,
	columnsColumns ColumnsRegistry,
	layout LayoutRegistry,
	ordering *InterfaceOrdering,
) *Guard {
	return &Guard{
		warehouseRegistry: warehouseRegistry,
		columnsColumns:    columnsColumns,
		layout:            layout,
		ordering:          ordering,
	}
}

// EnsureTables creates tables and adds missing columns for the specified property.
func (m *Guard) EnsureTables(
	propertyID string,
) error {
	columns, err := m.columnsColumns.Get(propertyID)
	if err != nil {
		return err
	}
	layout, err := m.layout.Get(propertyID)
	if err != nil {
		return err
	}
	tables := layout.Tables(Sorted(columns, m.ordering))
	for _, table := range tables {
		driver, err := m.warehouseRegistry.Get(propertyID)
		if err != nil {
			return err
		}

		// Create table if it doesn't exist
		if err := driver.CreateTable(table.Table, table.Schema); err != nil {
			var tableAlreadyExistsErr *warehouse.ErrTableAlreadyExists
			if !errors.As(err, &tableAlreadyExistsErr) {
				return err
			}
			logrus.Infof("table `%s` already exists", table.Table)
		} else {
			logrus.Infof("created table `%s`", table.Table)
		}

		// Add missing columns
		missingColumns, err := driver.MissingColumns(table.Table, table.Schema)
		if err != nil {
			return err
		}
		if len(missingColumns) > 0 {
			logrus.Infof("adding %d missing columns to table %s", len(missingColumns), table.Table)
			for _, column := range missingColumns {
				if err := driver.AddColumn(table.Table, column); err != nil {
					return err
				}
				logrus.Infof("added column `%s` to table `%s`", column.Name, table.Table)
			}
		}
	}
	return nil
}
