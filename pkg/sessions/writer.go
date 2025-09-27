package sessions

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/dgraph-io/ristretto/v2"
	"golang.org/x/sync/errgroup"
)

// sessionWriterImpl writes session data to a warehouse using the configured layout and column sources.
type sessionWriterImpl struct {
	writeTimeout time.Duration
	concurrency  int

	cacheTTL time.Duration

	layoutRegistry schema.LayoutRegistry
	layoutsCache   *ristretto.Cache[string, schema.Layout]
	layoutsLock    sync.Mutex

	warehouseCache    *ristretto.Cache[string, warehouse.Driver]
	warehouseRegistry warehouse.Registry
	warehouseLock     sync.Mutex

	columnsRegistry schema.ColumnsRegistry
	columnsCache    *ristretto.Cache[string, schema.Columns]
	columnsLock     sync.Mutex

	parentCtx context.Context
}

// Compile-time check to ensure SessionWriter implements SessionWriterInterface
var _ SessionWriter = (*sessionWriterImpl)(nil)

func (m *sessionWriterImpl) getSchema(propertyID, table string) (*arrow.Schema, error) {
	layout, err := m.getLayout(propertyID)
	if err != nil {
		return nil, err
	}
	columns, err := m.getColumns(propertyID)
	if err != nil {
		return nil, err
	}
	schemas := layout.Tables(columns)
	for _, schema := range schemas {
		if schema.Table == table {
			return schema.Schema, nil
		}
	}
	return nil, fmt.Errorf("table %s not found", table)
}

func (m *sessionWriterImpl) getWarehouse(propertyID string) (warehouse.Driver, error) {
	driver, err := getCached(m.warehouseCache, &m.warehouseLock, m.warehouseRegistry.Get, propertyID, m.cacheTTL)
	if err != nil {
		return nil, err
	}
	return driver, nil
}

func (m *sessionWriterImpl) getColumns(propertyID string) (schema.Columns, error) {
	columns, err := getCached(m.columnsCache, &m.columnsLock, m.columnsRegistry.Get, propertyID, m.cacheTTL)
	if err != nil {
		return schema.Columns{}, err
	}
	return columns, nil
}

func (m *sessionWriterImpl) getLayout(propertyID string) (schema.Layout, error) {
	layout, err := getCached(m.layoutsCache, &m.layoutsLock, m.layoutRegistry.Get, propertyID, m.cacheTTL)
	if err != nil {
		return nil, err
	}
	return layout, nil
}

// Write method writes the sessions to the warehouse. It first determines where the data
// should be written, then executes the writes in parallel for every table. It waits for
// all the writes to complete and returns an error if any of the writes fail.
func (m *sessionWriterImpl) Write(sessions ...*schema.Session) error {
	writeDeps, err := m.prepareDeps(sessions)
	if err != nil {
		return err
	}

	for _, session := range sessions {
		if err := m.writeColumns(writeDeps.columns, session); err != nil {
			return err
		}
	}

	perTableRows, err := NewBatchingSchemaLayout(writeDeps.layout, 1000).ToRows(writeDeps.columns, sessions...)
	if err != nil {
		return err
	}

	g, ctx := errgroup.WithContext(m.parentCtx)
	g.SetLimit(m.concurrency)
	for _, allRowsForGivenTable := range perTableRows {
		table, rows := allRowsForGivenTable.Table, allRowsForGivenTable.Rows
		g.Go(func() error {
			schema, err := m.getSchema(writeDeps.propertyID, allRowsForGivenTable.Table)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(ctx, m.writeTimeout)
			defer cancel()
			return writeDeps.warehouse.Write(ctx, table, schema, rows)
		})
	}
	return g.Wait()
}

type writeDeps struct {
	propertyID string
	warehouse  warehouse.Driver
	layout     schema.Layout
	columns    schema.Columns
}

func (m *sessionWriterImpl) prepareDeps(sessions []*schema.Session) (*writeDeps, error) {
	if len(sessions) == 0 {
		return nil, fmt.Errorf("no sessions provided")
	}
	uniquePropertyIDs := map[string]struct{}{}
	for _, session := range sessions {
		uniquePropertyIDs[session.PropertyID] = struct{}{}
	}
	if len(uniquePropertyIDs) > 1 {
		return nil, fmt.Errorf("all sessions must have the same property ID")
	}

	propertyID := sessions[0].PropertyID

	driver, err := m.getWarehouse(propertyID)
	if err != nil {
		return nil, fmt.Errorf("failed to get warehouse for property ID %s: %w", propertyID, err)
	}
	layout, err := m.getLayout(propertyID)
	if err != nil {
		return nil, fmt.Errorf("failed to get layout for property ID %s: %w", propertyID, err)
	}
	columns, err := m.getColumns(propertyID)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns for property ID %s: %w", propertyID, err)
	}

	return &writeDeps{
		propertyID: propertyID,
		warehouse:  driver,
		layout:     layout,
		columns:    columns,
	}, nil
}

func (m *sessionWriterImpl) writeColumns(columns schema.Columns, session *schema.Session) error {
	for _, column := range columns.Event {
		for _, event := range session.Events {
			if err := column.Write(event); err != nil {
				return err
			}
		}
	}
	for _, column := range columns.Session {
		if err := column.Write(session); err != nil {
			return err
		}
	}
	return nil
}

// NewSessionWriter creates a new SessionWriter with the provided warehouse, column sources, and layout sources.
// It caches each entity (warehouse, columns, layout) for 5 minutes.
func NewSessionWriter(
	parentCtx context.Context,
	whr warehouse.Registry,
	columnsRegistry schema.ColumnsRegistry,
	layouts schema.LayoutRegistry,
) SessionWriter {
	return &sessionWriterImpl{
		writeTimeout:      30 * time.Second,
		concurrency:       10,
		warehouseRegistry: whr,
		warehouseCache:    createDefaultCache[warehouse.Driver](),
		columnsRegistry:   columnsRegistry,
		columnsCache:      createDefaultCache[schema.Columns](),
		layoutRegistry:    layouts,
		layoutsCache:      createDefaultCache[schema.Layout](),
		cacheTTL:          5 * time.Minute,
		parentCtx:         parentCtx,
	}
}

func createDefaultCache[T any]() *ristretto.Cache[string, T] {
	cache, err := ristretto.NewCache(&ristretto.Config[string, T]{
		NumCounters: 8096,
		MaxCost:     8096,
		BufferItems: 64,
	})
	if err != nil {
		panic(err)
	}
	return cache
}

// getCached implements the double-checked locking pattern for caching registry lookups
func getCached[T any](
	cache *ristretto.Cache[string, T],
	lock *sync.Mutex,
	getter func(string) (T, error),
	propertyID string,
	cacheTTL time.Duration,
) (T, error) {
	// First check without lock
	item, ok := cache.Get(propertyID)
	if ok {
		return item, nil
	}

	// Acquire lock and double-check
	lock.Lock()
	defer lock.Unlock()
	item, ok = cache.Get(propertyID)
	if ok {
		return item, nil
	}

	// Get from registry
	item, err := getter(propertyID)
	if err != nil {
		var zero T
		return zero, err
	}

	// Cache the result
	cache.SetWithTTL(propertyID, item, 1, cacheTTL)
	return item, nil
}
