package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTTLCache_SetAndGet(t *testing.T) {
	tests := []struct {
		name     string
		ttl      time.Duration
		key      string
		value    string
		waitTime time.Duration
		wantOk   bool
	}{
		{
			name:     "get existing non-expired value",
			ttl:      100 * time.Millisecond,
			key:      "key1",
			value:    "value1",
			waitTime: 0,
			wantOk:   true,
		},
		{
			name:     "get expired value returns false",
			ttl:      50 * time.Millisecond,
			key:      "key2",
			value:    "value2",
			waitTime: 60 * time.Millisecond,
			wantOk:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			cache := NewTTLCache[string](tt.ttl)

			// when
			cache.Set(tt.key, tt.value)
			if tt.waitTime > 0 {
				time.Sleep(tt.waitTime)
			}
			got, ok := cache.Get(tt.key)

			// then
			assert.Equal(t, tt.wantOk, ok)
			if tt.wantOk {
				assert.Equal(t, tt.value, got)
			}
		})
	}
}

func TestTTLCache_GetNonExistent(t *testing.T) {
	// given
	cache := NewTTLCache[int](time.Minute)

	// when
	val, ok := cache.Get("nonexistent")

	// then
	assert.False(t, ok)
	assert.Equal(t, 0, val)
}

func TestTTLCache_Delete(t *testing.T) {
	// given
	cache := NewTTLCache[string](time.Minute)
	cache.Set("key1", "value1")

	// when
	cache.Delete("key1")
	_, ok := cache.Get("key1")

	// then
	assert.False(t, ok)
}

func TestTTLCache_Clear(t *testing.T) {
	// given
	cache := NewTTLCache[int](time.Minute)
	cache.Set("key1", 1)
	cache.Set("key2", 2)
	cache.Set("key3", 3)

	// when
	cache.Clear()

	// then
	assert.Equal(t, 0, cache.Len())
	_, ok := cache.Get("key1")
	assert.False(t, ok)
}

func TestTTLCache_Len(t *testing.T) {
	// given
	cache := NewTTLCache[string](time.Minute)

	// when
	cache.Set("key1", "value1")
	cache.Set("key2", "value2")

	// then
	assert.Equal(t, 2, cache.Len())
}

func TestTTLCache_GenericTypes(t *testing.T) {
	t.Run("struct type", func(t *testing.T) {
		// given
		type testStruct struct {
			ID   int
			Name string
		}
		cache := NewTTLCache[testStruct](time.Minute)
		expected := testStruct{ID: 1, Name: "test"}

		// when
		cache.Set("key", expected)
		got, ok := cache.Get("key")

		// then
		assert.True(t, ok)
		assert.Equal(t, expected, got)
	})

	t.Run("pointer type", func(t *testing.T) {
		// given
		cache := NewTTLCache[*int](time.Minute)
		val := 42
		expected := &val

		// when
		cache.Set("key", expected)
		got, ok := cache.Get("key")

		// then
		assert.True(t, ok)
		assert.Equal(t, expected, got)
	})
}

func TestTTLCache_ExpiredEntriesRemovedOnGet(t *testing.T) {
	// given
	cache := NewTTLCache[string](50 * time.Millisecond)
	cache.Set("key1", "value1")

	// when
	time.Sleep(60 * time.Millisecond)
	_, ok := cache.Get("key1")
	lenAfterGet := cache.Len()

	// then
	assert.False(t, ok)
	assert.Equal(t, 0, lenAfterGet)
}
