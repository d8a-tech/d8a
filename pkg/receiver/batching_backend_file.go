package receiver

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/sirupsen/logrus"
)

// FileBatchingBackendConfig configures a file-backed batching backend.
type FileBatchingBackendConfig struct {
	Dir           string
	FlushFileName string
}

// NewFileBatchingBackend creates a BatchingBackend that durably stages hits to disk
// using a single gzip-json encoded file.
func NewFileBatchingBackend(cfg FileBatchingBackendConfig) BatchingBackend {
	return &fileBatchingBackend{cfg: cfg}
}

type fileBatchingBackend struct {
	cfg FileBatchingBackendConfig
}

func (f *fileBatchingBackend) flushFilePath() string {
	return filepath.Join(f.cfg.Dir, f.cfg.FlushFileName)
}

// Append implements BatchingBackend.
func (f *fileBatchingBackend) Append(h []*hits.Hit) error {
	if err := os.MkdirAll(f.cfg.Dir, 0o750); err != nil {
		return fmt.Errorf("creating backend dir: %w", err)
	}

	existing, err := f.readFlushFile()
	if err != nil {
		return fmt.Errorf("reading existing flush file: %w", err)
	}

	existing = append(existing, h...)

	if err := f.writeAtomically(existing); err != nil {
		return fmt.Errorf("writing flush file: %w", err)
	}
	return nil
}

// Flush implements BatchingBackend.
func (f *fileBatchingBackend) Flush(cb func([]*hits.Hit) error) error {
	path := f.flushFilePath()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	}

	allHits, err := f.readFlushFile()
	if err != nil {
		return fmt.Errorf("reading flush file for flush: %w", err)
	}

	if len(allHits) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			logrus.Debugf("removing empty flush file %q: %v", path, err)
		}
		return nil
	}

	if err := cb(allHits); err != nil {
		return err
	}

	if err := os.Remove(path); err != nil {
		return fmt.Errorf("removing flush file after successful flush: %w", err)
	}
	return nil
}

// Close implements BatchingBackend.
func (f *fileBatchingBackend) Close() error {
	return nil
}

func (f *fileBatchingBackend) readFlushFile() ([]*hits.Hit, error) {
	path := f.flushFilePath()

	file, err := os.Open(path) //nolint:gosec // path derived from config, not user input
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("opening flush file: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			logrus.Errorf("closing flush file: %v", closeErr)
		}
	}()

	gz, err := gzip.NewReader(file)
	if err != nil {
		return nil, fmt.Errorf("creating gzip reader: %w", err)
	}
	defer func() {
		if closeErr := gz.Close(); closeErr != nil {
			logrus.Errorf("closing gzip reader: %v", closeErr)
		}
	}()

	var result []*hits.Hit
	if err := json.NewDecoder(gz).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding hits from flush file: %w", err)
	}
	return result, nil
}

func (f *fileBatchingBackend) writeAtomically(allHits []*hits.Hit) error {
	path := f.flushFilePath()

	tmp, err := os.CreateTemp(f.cfg.Dir, "pending_hits_*.tmp")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	tmpPath := tmp.Name()

	success := false
	defer func() {
		if !success {
			if closeErr := tmp.Close(); closeErr != nil {
				logrus.Debugf("closing temp flush file %q: %v", tmpPath, closeErr)
			}
			if removeErr := os.Remove(tmpPath); removeErr != nil && !os.IsNotExist(removeErr) {
				logrus.Debugf("removing temp flush file %q: %v", tmpPath, removeErr)
			}
		}
	}()

	gz := gzip.NewWriter(tmp)
	if err := json.NewEncoder(gz).Encode(allHits); err != nil {
		return fmt.Errorf("encoding hits: %w", err)
	}
	if err := gz.Close(); err != nil {
		return fmt.Errorf("closing gzip writer: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("syncing temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing temp file: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("renaming temp file to flush file: %w", err)
	}

	success = true
	return nil
}

// readFlushFileForTest is only accessible from same-package tests via an adapter.
// Exported for testing file backend recovery scenarios.
func readFlushFileForTest(b BatchingBackend) ([]*hits.Hit, error) {
	fb, ok := b.(*fileBatchingBackend)
	if !ok {
		return nil, fmt.Errorf("not a fileBatchingBackend")
	}
	return fb.readFlushFile()
}
