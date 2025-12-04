package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCachedKV(t *testing.T) {
	// given
	underlying := NewInMemoryKV()
	cachedKV, err := NewCachedKV(
		underlying,
		WithMaxCacheBytes(1024*1024),
		WithCacheTTL(time.Minute),
	)
	if err != nil {
		t.Fatalf("Failed to create CachedKV: %v", err)
	}

	// when / then
	KVTestSuite(t, cachedKV)
}

func TestCachedKVWithDefaults(t *testing.T) {
	// given
	underlying := NewInMemoryKV()
	cachedKV, err := NewCachedKV(underlying)
	if err != nil {
		t.Fatalf("Failed to create CachedKV: %v", err)
	}

	// when / then
	KVTestSuite(t, cachedKV)
}

func TestCachedKVSkipWriteIfCachedValueSuite(t *testing.T) {
	// given
	underlying := NewInMemoryKV()
	cachedKV, err := NewCachedKV(underlying, WithSkipWriteIfCachedValueMatches(true))
	if err != nil {
		t.Fatalf("Failed to create CachedKV: %v", err)
	}

	// when / then
	KVTestSuite(t, cachedKV)
}

func TestCachedKVSkipWriteIfCachedValueMatches(t *testing.T) {
	t.Run("Skip write when cached value matches", func(t *testing.T) {
		// given
		underlying := NewInMemoryKV()
		cachedKV, err := NewCachedKV(
			underlying,
			WithMaxCacheBytes(1024*1024),
			WithCacheTTL(time.Minute),
			WithSkipWriteIfCachedValueMatches(true),
		)
		assert.NoError(t, err)

		key := []byte("test-key")
		value := []byte("test-value")

		// when - first set (cache miss)
		_, err = cachedKV.Set(key, value)
		assert.NoError(t, err)

		// when - get to populate cache
		got, err := cachedKV.Get(key)
		assert.NoError(t, err)
		assert.Equal(t, value, got)

		// when - set with same value (cache hit)
		prev, err := cachedKV.Set(key, value)
		assert.NoError(t, err)

		// then - should return nil (skipped write)
		assert.Nil(t, prev)

		// then - value should still be retrievable
		got, err = cachedKV.Get(key)
		assert.NoError(t, err)
		assert.Equal(t, value, got)
	})

	t.Run("Do not skip write when cached value differs", func(t *testing.T) {
		// given
		underlying := NewInMemoryKV()
		cachedKV, err := NewCachedKV(
			underlying,
			WithMaxCacheBytes(1024*1024),
			WithCacheTTL(time.Minute),
			WithSkipWriteIfCachedValueMatches(true),
		)
		assert.NoError(t, err)

		key := []byte("test-key")
		value1 := []byte("value1")
		value2 := []byte("value2")

		// when - first set
		_, err = cachedKV.Set(key, value1)
		assert.NoError(t, err)

		// when - get to populate cache
		got, err := cachedKV.Get(key)
		assert.NoError(t, err)
		assert.Equal(t, value1, got)

		// when - set with different value
		_, err = cachedKV.Set(key, value2)
		assert.NoError(t, err)

		// then - new value should be set
		got, err = cachedKV.Get(key)
		assert.NoError(t, err)
		assert.Equal(t, value2, got)
	})

	t.Run("Default behavior does not skip write", func(t *testing.T) {
		// given
		underlying := NewInMemoryKV()
		cachedKV, err := NewCachedKV(underlying)
		assert.NoError(t, err)

		key := []byte("test-key")
		value := []byte("test-value")

		// when - first set
		_, err = cachedKV.Set(key, value)
		assert.NoError(t, err)

		// when - get to populate cache
		got, err := cachedKV.Get(key)
		assert.NoError(t, err)
		assert.Equal(t, value, got)

		// when - set with same value (should write through)
		_, err = cachedKV.Set(key, value, WithReturnPreviousValue(true))
		assert.NoError(t, err)

		// then - value should still work correctly
		got, err = cachedKV.Get(key)
		assert.NoError(t, err)
		assert.Equal(t, value, got)
	})
}
