package storage

import (
	"bytes"
	"time"

	"github.com/dgraph-io/ristretto/v2"
)

// CachedKVConfig holds configuration for CachedKV.
type CachedKVConfig struct {
	// MaxCacheBytes is the maximum size of the cache in bytes.
	MaxCacheBytes int64
	// TTL is the time-to-live for cached entries.
	TTL time.Duration
	// SkipWriteIfCachedValueMatches skips writing to underlying storage if cached value equals new value.
	// This is disabled by default to maintain strong consistency.
	// Enable only if you control all writes and accept eventual consistency risks.
	SkipWriteIfCachedValueMatches bool
}

// CachedKVConfigFunc is a function that modifies CachedKVConfig.
type CachedKVConfigFunc func(config *CachedKVConfig)

// DefaultCachedKVConfig returns default configuration.
func DefaultCachedKVConfig() *CachedKVConfig {
	return &CachedKVConfig{
		MaxCacheBytes:                 10 * 1024 * 1024, // 10MB
		TTL:                           5 * time.Minute,
		SkipWriteIfCachedValueMatches: false,
	}
}

// WithMaxCacheBytes sets the maximum cache size in bytes.
func WithMaxCacheBytes(maxBytes int64) CachedKVConfigFunc {
	return func(config *CachedKVConfig) {
		config.MaxCacheBytes = maxBytes
	}
}

// WithCacheTTL sets the TTL for cached entries.
func WithCacheTTL(ttl time.Duration) CachedKVConfigFunc {
	return func(config *CachedKVConfig) {
		config.TTL = ttl
	}
}

// WithSkipWriteIfCachedValueMatches enables skipping writes when cached value matches.
func WithSkipWriteIfCachedValueMatches(skip bool) CachedKVConfigFunc {
	return func(config *CachedKVConfig) {
		config.SkipWriteIfCachedValueMatches = skip
	}
}

// CachedKV wraps a KV implementation with a ristretto cache.
type CachedKV struct {
	underlying                    KV
	cache                         *ristretto.Cache[string, []byte]
	ttl                           time.Duration
	skipWriteIfCachedValueMatches bool
}

// NewCachedKV creates a new cached KV implementation.
func NewCachedKV(underlying KV, opts ...CachedKVConfigFunc) (KV, error) {
	config := DefaultCachedKVConfig()
	for _, opt := range opts {
		opt(config)
	}

	cache, err := ristretto.NewCache(&ristretto.Config[string, []byte]{
		NumCounters: 1e7, // number of keys to track frequency
		MaxCost:     config.MaxCacheBytes,
		BufferItems: 64,
	})
	if err != nil {
		return nil, err
	}

	return &CachedKV{
		underlying:                    underlying,
		cache:                         cache,
		ttl:                           config.TTL,
		skipWriteIfCachedValueMatches: config.SkipWriteIfCachedValueMatches,
	}, nil
}

// Get implements KV.
func (c *CachedKV) Get(key []byte) ([]byte, error) {
	if key == nil {
		return nil, ErrNilKey
	}
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}

	keyStr := string(key)

	// Check cache first
	if value, found := c.cache.Get(keyStr); found {
		return value, nil
	}

	// Get from underlying storage
	value, err := c.underlying.Get(key)
	if err != nil {
		return nil, err
	}

	// Cache the result if value is not nil
	if value != nil {
		cost := int64(len(value))
		c.cache.SetWithTTL(keyStr, value, cost, c.ttl)
	}

	return value, nil
}

// Set implements KV.
func (c *CachedKV) Set(key, value []byte, opts ...SetOptionsFunc) ([]byte, error) {
	if key == nil {
		return nil, ErrNilKey
	}
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}
	if value == nil {
		return nil, ErrNilValue
	}

	keyStr := string(key)

	// Check if we should skip write based on cached value
	if c.skipWriteIfCachedValueMatches {
		options := DefaultSetOptions()
		for _, opt := range opts {
			opt(options)
		}
		if cachedValue, found := c.cache.Get(keyStr); found {
			if bytes.Equal(cachedValue, value) {
				// Value matches cache, skip underlying write
				if options.ReturnPreviousValue {
					return cachedValue, nil
				}
				return nil, nil
			}
		}
	}

	// Perform the set operation on the underlying storage
	previousValue, err := c.underlying.Set(key, value, opts...)
	if err != nil {
		return nil, err
	}

	// Invalidate cache entry
	c.cache.Del(keyStr)

	return previousValue, nil
}

// Delete implements KV.
func (c *CachedKV) Delete(key []byte) error {
	if key == nil {
		return ErrNilKey
	}
	if len(key) == 0 {
		return ErrEmptyKey
	}

	keyStr := string(key)

	// Delete from underlying storage
	err := c.underlying.Delete(key)
	if err != nil {
		return err
	}

	// Invalidate cache entry
	c.cache.Del(keyStr)

	return nil
}
