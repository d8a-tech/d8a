package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/d8a-tech/d8a/pkg/bolt"
	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/dbip"
	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/protosessions"
	"github.com/d8a-tech/d8a/pkg/receiver"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/d8a-tech/d8a/pkg/sessions"
	"github.com/d8a-tech/d8a/pkg/splitter"
	"github.com/d8a-tech/d8a/pkg/spools"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/d8a-tech/d8a/pkg/worker"
	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
	"go.etcd.io/bbolt"
)

type WorkerRuntime struct {
	Worker  *worker.Worker
	Cleanup func()
}

//nolint:funlen,gocognit // wiring
func buildWorkerRuntime(
	ctx context.Context,
	cmd *cli.Command,
	serverStorage receiver.Storage,
	whr warehouse.Registry,
	converter currency.Converter,
	geoProvider dbip.LookupProvider,
) (*WorkerRuntime, error) {
	boltDBPath := filepath.Join(cmd.String(storageBoltDirectoryFlag.Name), "bolt.db")
	boltDB, err := bbolt.Open(boltDBPath, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("open bolt db: %w", err)
	}

	boltKVPath := filepath.Join(cmd.String(storageBoltDirectoryFlag.Name), "bolt_kv.db")
	kv, err := bolt.NewBoltKV(boltKVPath)
	if err != nil {
		_ = boltDB.Close()
		return nil, fmt.Errorf("open bolt kv: %w", err)
	}

	cr := columnsRegistry(cmd, converter, geoProvider) // nolint:contextcheck // false positive
	layoutRegistry := schema.NewStaticLayoutRegistry(
		map[string]schema.Layout{},
		schema.NewEmbeddedSessionColumnsLayout(
			getTableNames(cmd).events,
			getTableNames(cmd).sessionsColumnPrefix,
		),
	)
	splitterRegistry := splitter.NewFromPropertySettingsRegistry(
		propertySettings(cmd),
		splitter.WithCapacity(5),
		splitter.WithTTL(30*24*time.Hour), // Static config, so no need to invalidate
	)
	// Special case for OSS - on top of registry we validate if rules compile right away
	_, err = splitterRegistry.SessionModifier(cmd.String(propertyIDFlag.Name))
	if err != nil {
		logrus.Panicf("failed to create session modifier: %v", err)
	}

	var batchingCleanup func()
	cleanup := func() {
		if batchingCleanup != nil {
			batchingCleanup()
		}
		if closeErr := boltDB.Close(); closeErr != nil {
			logrus.Error("failed to close bolt db:", closeErr)
		}
		if c, ok := kv.(interface{ Close() error }); ok {
			if closeErr := c.Close(); closeErr != nil {
				logrus.Error("failed to close bolt kv:", closeErr)
			}
		}
	}

	sessionWriter := sessions.NewSessionWriter(
		ctx,
		whr,
		cr,
		layoutRegistry,
		splitterRegistry,
	)
	if cmd.Bool(storageSpoolEnabledFlag.Name) {
		spoolDir := filepath.Join(cmd.String(storageSpoolDirectoryFlag.Name), "warehouse", "generic")

		sp, err := spools.New(afero.NewOsFs(), spoolDir)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("create spool: %w", err)
		}

		mode := cmd.String(deliveryModeFlag.Name)
		var fs sessions.SpoolFailureStrategy
		if mode == "at_least_once" {
			fs = sessions.NewQuarantineSpoolStrategy()
		} else {
			fs = sessions.NewDeleteSpoolStrategy()
		}

		persistentWriter, persistentCleanup, err := sessions.NewPersistentSpoolWriter(
			ctx, sp, sessionWriter, fs,
		)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("create persistent spool writer: %w", err)
		}
		sessionWriter = persistentWriter
		batchingCleanup = persistentCleanup
	}

	w := worker.NewWorker(
		[]worker.TaskHandler{
			worker.NewGenericTaskHandler(
				hits.HitProcessingTaskName,
				encoding.GzipJSONDecoder,
				protosessions.Handler(
					ctx,
					protosessions.NewDeduplicatingBatchedIOBackend(func() protosessions.BatchedIOBackend {
						b, err := bolt.NewBatchedProtosessionsIOBackend(
							boltDB,
							encoding.GzipJSONEncoder,
							encoding.GzipJSONDecoder,
						)
						if err != nil {
							logrus.Panicf("failed to create bolt batched io backend: %v", err)
						}
						return b
					}()),
					protosessions.NewGenericKVTimingWheelBackend(
						"default",
						kv,
					),
					protosessions.NewShardingCloser(
						10,
						func(_ int) protosessions.Closer {
							return sessions.NewDirectCloser(
								sessionWriter,
								5*time.Second,
							)
						},
					),
					serverStorage,
					propertySettings(cmd),
					protosessions.WithSkipCatchUpOnStartup(cmd.Bool(skipCatchUpFlag.Name)),
				),
			),
		},
		[]worker.Middleware{},
	)

	return &WorkerRuntime{Worker: w, Cleanup: cleanup}, nil
}
