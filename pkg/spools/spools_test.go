package spools

import (
	"encoding/binary"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadBatch_StopsAtFlushBatchMaxBytes(t *testing.T) {
	// given
	fs := afero.NewMemMapFs()
	path := "/spool/inflight.spool"
	require.NoError(t, fs.MkdirAll("/spool", 0o755))
	require.NoError(t, writeFramesFile(fs, path, [][]byte{
		make([]byte, 4),
		make([]byte, 4),
		make([]byte, 4),
	}))

	r, err := newFrameReader(fs, path)
	require.NoError(t, err)
	defer r.close()

	s := &fileSpool{flushBatchMaxBytes: 8}

	// when
	batch, err := s.readBatch(r)

	// then
	require.NoError(t, err)
	assert.Len(t, batch, 2)
	assert.Equal(t, int64(8), batchPayloadBytes(batch))

	remaining, err := s.readBatch(r)
	require.NoError(t, err)
	assert.Len(t, remaining, 1)
	assert.Equal(t, int64(4), batchPayloadBytes(remaining))
}

func TestReadBatch_ReturnsOversizedSingleFrame(t *testing.T) {
	// given
	fs := afero.NewMemMapFs()
	path := "/spool/inflight.spool"
	require.NoError(t, fs.MkdirAll("/spool", 0o755))
	require.NoError(t, writeFramesFile(fs, path, [][]byte{
		make([]byte, 10),
		make([]byte, 2),
	}))

	r, err := newFrameReader(fs, path)
	require.NoError(t, err)
	defer r.close()

	s := &fileSpool{flushBatchMaxBytes: 8}

	// when
	batch, err := s.readBatch(r)

	// then
	require.NoError(t, err)
	assert.Len(t, batch, 1)
	assert.Equal(t, int64(10), batchPayloadBytes(batch))

	remaining, err := s.readBatch(r)
	require.NoError(t, err)
	assert.Len(t, remaining, 1)
	assert.Equal(t, int64(2), batchPayloadBytes(remaining))
}

func TestNewFileFactory_DefaultFlushBatchMaxBytes(t *testing.T) {
	// given
	fs := afero.NewMemMapFs()

	// when
	factory, err := NewFileFactory(fs, "/spool")

	// then
	require.NoError(t, err)
	ff, ok := factory.(*fileFactory)
	require.True(t, ok)
	assert.Equal(t, defaultFlushBatchMaxBytes, ff.flushBatchMaxBytes)
}

func writeFramesFile(fs afero.Fs, path string, frames [][]byte) error {
	f, err := fs.Create(path)
	if err != nil {
		return err
	}
	defer f.Close() //nolint:errcheck // test helper

	header := make([]byte, headerSize)
	for _, frame := range frames {
		binary.LittleEndian.PutUint32(header, uint32(len(frame)))
		if _, err := f.Write(header); err != nil {
			return err
		}
		if _, err := f.Write(frame); err != nil {
			return err
		}
	}

	return nil
}
