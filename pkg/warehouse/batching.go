package warehouse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/sirupsen/logrus"
)

type tableProps struct {
	schema           *arrow.Schema
	lastWriteTime    time.Time
	currentBatchSize int
}

type batchingDriver struct {
	// Persists all the data in set, so that at least once delivery is guaranteed
	set     storage.Set
	encoder encoding.EncoderFunc
	decoder encoding.DecoderFunc
	lock    sync.Mutex
	driver  Driver
	stop    chan struct{}
	lastCtx context.Context

	maxBatchSize int
	interval     time.Duration
	tableProps   map[string]*tableProps
}

// NewBatchingDriver creates a new batching driver that batches writes to improve performance.
func NewBatchingDriver(
	driver Driver,
	maxBatchSize int,
	interval time.Duration,
	set storage.Set,
	stop chan struct{},
) Driver {
	if stop == nil {
		stop = make(chan struct{})
	}

	d := &batchingDriver{
		driver:       driver,
		interval:     interval,
		maxBatchSize: maxBatchSize,
		stop:         stop,
		lock:         sync.Mutex{},
		encoder:      encoding.GobEncoder,
		decoder:      encoding.GobDecoder,
		set:          set,
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
	d.lastCtx = ctx
	if len(records) == 0 {
		return nil
	}

	props, ok := d.tableProps[table]
	if !ok {
		props = &tableProps{
			schema: schema,
		}
		d.tableProps[table] = props
	}

	buf := bytes.NewBuffer(nil)
	_, err := d.encoder(buf, records)
	if err != nil {
		return err
	}
	if err := d.set.Add([]byte(fmt.Sprintf("batch:%s", table)), buf.Bytes()); err != nil {
		logrus.WithError(err).Error("failed to add batch to set")
	}

	props.currentBatchSize += len(records)
	return nil
}

func (d *batchingDriver) AddColumn(table string, column *arrow.Field) error {
	return d.driver.AddColumn(table, column)
}

func (d *batchingDriver) CreateTable(table string, schema *arrow.Schema) error {
	return d.driver.CreateTable(table, schema)
}

func (d *batchingDriver) MissingColumns(table string, schema *arrow.Schema) ([]*arrow.Field, error) {
	return d.driver.MissingColumns(table, schema)
}

func (d *batchingDriver) flush() error {
	d.lock.Lock()
	defer d.lock.Unlock()

	for table, props := range d.tableProps {
		if props.currentBatchSize == 0 {
			continue
		}
		logrus.Infof("flushing batch of size %d for table %s", props.currentBatchSize, table)
		buf, err := d.set.All([]byte(fmt.Sprintf("batch:%s", table)))
		if err != nil {
			logrus.WithError(err).Errorf("Failed to get batch for table %s", table)
			continue
		}
		var records []map[string]any
		for _, b := range buf {
			var loopRecords []map[string]any
			err := d.decoder(bytes.NewReader(b), &loopRecords)
			if err != nil {
				// This is quite questionable, in case of decode error no future retry will help
				// We will clog the processing, but maybe it's the correct approach until we have
				// a secondary destionation to write "unretryable data" to
				logrus.WithError(err).Errorf("Failed to decode batch for table %s", table)
				continue
			}
			records = append(records, loopRecords...)
		}

		if err := d.driver.Write(d.lastCtx, table, props.schema, records); err != nil {
			logrus.WithError(err).Errorf("Failed to write batch for table %s", table)
			continue
		}
		if err := d.set.Delete([]byte(fmt.Sprintf("batch:%s", table))); err != nil {
			logrus.WithError(err).Error("failed to delete batch from set")
		}
		props.currentBatchSize = 0
		props.lastWriteTime = time.Now()
	}
	return nil
}

func (d *batchingDriver) start() {
	go func() {
		for {
			if err := d.flush(); err != nil {
				logrus.WithError(err).Error("error flushing batch")
			}

			select {
			case <-d.stop:
				return
			case <-time.After(d.interval):
				continue
			}
		}
	}()
}

// StopBatchingDriver lets you stop a running batching driver
func StopBatchingDriver(d Driver) error {
	batching, ok := d.(*batchingDriver)
	if !ok {
		return errors.New("passed driver instance is not a Batching driver")
	}
	batching.stop <- struct{}{}
	return nil
}
