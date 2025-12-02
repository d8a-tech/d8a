package protosessionsv3

import (
	"context"
	"testing"

	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/stretchr/testify/assert"
)

func TestGenericStorageTickerBackend_FirstRun(t *testing.T) {
	// given
	kv := &storage.InMemoryKV{KV: make(map[string][]byte)}
	backend := NewGenericStorageTimingWheelBackend("test", kv)
	ctx := context.Background()

	// when
	nextBucket, err := backend.GetNextBucket(ctx)

	// then
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), nextBucket)
}

func TestGenericStorageTickerBackend_SaveAndGet(t *testing.T) {
	// given
	kv := &storage.InMemoryKV{KV: make(map[string][]byte)}
	backend := NewGenericStorageTimingWheelBackend("test", kv)
	ctx := context.Background()

	// when
	err := backend.SaveNextBucket(ctx, 42)
	assert.NoError(t, err)

	nextBucket, err := backend.GetNextBucket(ctx)

	// then
	assert.NoError(t, err)
	assert.Equal(t, int64(42), nextBucket)
}

func TestGenericStorageTickerBackend_NamedVsUnnamed(t *testing.T) {
	// given
	kv := &storage.InMemoryKV{KV: make(map[string][]byte)}

	namedBackend := NewGenericStorageTimingWheelBackend("wheel1", kv)
	unnamedBackend := NewGenericStorageTimingWheelBackend("", kv)
	ctx := context.Background()

	// when
	err := namedBackend.SaveNextBucket(ctx, 10)
	assert.NoError(t, err)

	err = unnamedBackend.SaveNextBucket(ctx, 20)
	assert.NoError(t, err)

	namedNext, err := namedBackend.GetNextBucket(ctx)
	assert.NoError(t, err)

	unnamedNext, err := unnamedBackend.GetNextBucket(ctx)
	assert.NoError(t, err)

	// then
	assert.Equal(t, int64(10), namedNext)
	assert.Equal(t, int64(20), unnamedNext)
}
