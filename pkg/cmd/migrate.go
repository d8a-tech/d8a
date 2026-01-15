package cmd

import (
	"context"
	"fmt"

	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"
)

func migrate(ctx context.Context, cmd *cli.Command, propertyID string) error {
	settings, err := propertySettings(cmd).GetByPropertyID(propertyID)
	if err != nil {
		return err
	}
	protocol := protocolByID(settings.ProtocolID, cmd)
	if protocol == nil {
		return fmt.Errorf("protocol %s not found", settings.ProtocolID)
	}
	columnData, err := columnsRegistry(cmd).Get(propertyID) // nolint:contextcheck // false positive
	if err != nil {
		return err
	}
	totalColumns := len(columnData.Event) + len(columnData.Session) +
		len(columnData.SessionScopedEvent)
	allColumns := make([]schema.Column, 0, totalColumns)
	allColumns = append(allColumns, schema.ToGenericColumns(columnData.Event)...)
	allColumns = append(allColumns, schema.ToGenericColumns(columnData.Session)...)
	allColumns = append(allColumns, schema.ToGenericColumns(columnData.SessionScopedEvent)...)
	err = schema.AssertAllDependenciesFulfilledWithCoreColumns(allColumns, columns.GetAllCoreColumns())
	if err != nil {
		return err
	}
	logrus.Infof("all dependencies fulfilled for property %s", propertyID)
	guard := schema.NewGuard(
		warehouseRegistry(ctx, cmd),
		schema.NewStaticColumnsRegistry(
			map[string]schema.Columns{},
			columnData,
		),
		schema.NewStaticLayoutRegistry(
			map[string]schema.Layout{},
			schema.NewEmbeddedSessionColumnsLayout(
				getTableNames(cmd).events,
				getTableNames(cmd).sessionsColumnPrefix,
			),
		),
		schema.NewInterfaceDefinitionOrderKeeper(
			columns.CoreInterfaces,
			protocol.Interfaces(),
		),
	)
	if err := guard.EnsureTables(propertyID); err != nil {
		return err
	}

	logrus.Infof("migrated property %s to the new schema", propertyID)
	return nil
}
