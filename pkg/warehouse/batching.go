package warehouse

import (
	"context"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/sirupsen/logrus"
)

type tableProps struct {
	schema  *arrow.Schema
	records []map[string]any
}

type batchingDriver struct {
	lock   sync.Mutex
	driver Driver
	ctx    context.Context

	maxBatchSize int
	interval     time.Duration
	tableProps   map[string]*tableProps
}

// NewBatchingDriver creates a batching driver that accumulates writes in memory
// and flushes them periodically or when the context is cancelled.
func NewBatchingDriver(
	ctx context.Context,
	driver Driver,
	maxBatchSize int,
	interval time.Duration,
) Driver {
	d := &batchingDriver{
		ctx:          ctx,
		driver:       driver,
		interval:     interval,
		maxBatchSize: maxBatchSize,
		tableProps:   make(map[string]*tableProps),
	}
	d.start()
	return d
}

func (d *batchingDriver) Write(
	ctx context.Context,
	table string,
	schema *arrow.Schema,
	records []map[string]any,
) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	if len(records) == 0 {
		return nil
	}

	props, ok := d.tableProps[table]
	if !ok {
		props = &tableProps{
			schema:  schema,
			records: nil,
		}
		d.tableProps[table] = props
	}

	props.records = append(props.records, records...)
	return nil
}

// AddColumn implements Driver.
func (d *batchingDriver) AddColumn(table string, column *arrow.Field) error {
	return d.driver.AddColumn(table, column)
}

// CreateTable implements Driver.
func (d *batchingDriver) CreateTable(table string, schema *arrow.Schema) error {
	return d.driver.CreateTable(table, schema)
}

// MissingColumns implements Driver.
func (d *batchingDriver) MissingColumns(table string, schema *arrow.Schema) ([]*arrow.Field, error) {
	return d.driver.MissingColumns(table, schema)
}

func (d *batchingDriver) flush() {
	d.lock.Lock()
	defer d.lock.Unlock()

	for table, props := range d.tableProps {
		if len(props.records) == 0 {
			continue
		}
		logrus.Infof("flushing batch of size %d for table %s", len(props.records), table)

		if err := d.driver.Write(d.ctx, table, props.schema, props.records); err != nil {
			logrus.WithError(err).Errorf("Failed to write batch for table %s", table)
			continue
		}
		props.records = nil
	}
}

func (d *batchingDriver) start() {
	go func() {
		ticker := time.NewTicker(d.interval)
		defer ticker.Stop()

		for {
			select {
			case <-d.ctx.Done():
				d.flush()
				return
			case <-ticker.C:
				d.flush()
			}
		}
	}()
}
