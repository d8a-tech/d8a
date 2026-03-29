package receiver

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/util"
	"github.com/sirupsen/logrus"
)

// FileBatchingBackendConfig configures a file-backed batching backend.
type FileBatchingBackendConfig struct {
	Dir           string
	FlushFileName string
}

// NewFileBatchingBackend creates a BatchingBackend that durably stages hits to disk
// using an append-only framed JSON file.
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

	path := f.flushFilePath()
	//nolint:gosec // path derived from config, not user input
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("opening flush file for append: %w", err)
	}

	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			logrus.Errorf("closing flush file after append: %v", closeErr)
		}
	}()

	for _, hit := range h {
		encodedHit, marshalErr := json.Marshal(hit)
		if marshalErr != nil {
			return fmt.Errorf("marshaling hit for append: %w", marshalErr)
		}

		if len(encodedHit) > int(^uint32(0)) {
			return fmt.Errorf("encoded hit too large: %d bytes", len(encodedHit))
		}

		var sizeHeader [4]byte
		binary.BigEndian.PutUint32(sizeHeader[:], util.SafeIntToUint32(len(encodedHit)))

		if _, writeErr := file.Write(sizeHeader[:]); writeErr != nil {
			return fmt.Errorf("writing framed hit size: %w", writeErr)
		}

		if _, writeErr := file.Write(encodedHit); writeErr != nil {
			return fmt.Errorf("writing framed hit payload: %w", writeErr)
		}
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("syncing appended flush file: %w", err)
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

	return f.readFramedFlushFile(file)
}

func (f *fileBatchingBackend) readFramedFlushFile(file *os.File) ([]*hits.Hit, error) {
	result := make([]*hits.Hit, 0)

	var sizeHeader [4]byte
	for {
		_, err := io.ReadFull(file, sizeHeader[:])
		if err == io.EOF {
			return result, nil
		}
		if err == io.ErrUnexpectedEOF {
			return nil, fmt.Errorf("reading framed hit size: truncated header")
		}
		if err != nil {
			return nil, fmt.Errorf("reading framed hit size: %w", err)
		}

		payloadSize := binary.BigEndian.Uint32(sizeHeader[:])
		if payloadSize == 0 {
			return nil, fmt.Errorf("reading framed hit payload: invalid zero size")
		}

		payload := make([]byte, payloadSize)
		if _, err := io.ReadFull(file, payload); err == io.ErrUnexpectedEOF || err == io.EOF {
			return nil, fmt.Errorf("reading framed hit payload: truncated payload")
		} else if err != nil {
			return nil, fmt.Errorf("reading framed hit payload: %w", err)
		}

		var hit hits.Hit
		if err := json.Unmarshal(payload, &hit); err != nil {
			return nil, fmt.Errorf("decoding framed hit payload: %w", err)
		}

		result = append(result, &hit)
	}
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
