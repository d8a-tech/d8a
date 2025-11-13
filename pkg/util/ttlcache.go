package util

import (
	"sync"
	"time"
)

// TTLCache is a simple in-memory cache with time-to-live expiration.
// Expired entries are removed lazily during Get and Set operations.
type TTLCache[V any] struct {
	mu    sync.RWMutex
	items map[string]*cacheItem[V]
	ttl   time.Duration
}

type cacheItem[V any] struct {
	value     V
	expiresAt time.Time
}

// NewTTLCache creates a cache with the specified TTL for all entries.
func NewTTLCache[V any](ttl time.Duration) *TTLCache[V] {
	return &TTLCache[V]{
		items: make(map[string]*cacheItem[V]),
		ttl:   ttl,
	}
}

// Set stores a value with the cache's default TTL.
func (c *TTLCache[V]) Set(key string, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items[key] = &cacheItem[V]{
		value:     value,
		expiresAt: time.Now().Add(c.ttl),
	}
}

// Get retrieves a value if it exists and hasn't expired.
// Returns the value and true if found and valid, zero value and false otherwise.
func (c *TTLCache[V]) Get(key string) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, exists := c.items[key]
	if !exists {
		var zero V
		return zero, false
	}

	if time.Now().After(item.expiresAt) {
		delete(c.items, key)
		var zero V
		return zero, false
	}

	return item.value, true
}

// Delete removes an entry from the cache.
func (c *TTLCache[V]) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.items, key)
}

// Clear removes all entries from the cache.
func (c *TTLCache[V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*cacheItem[V])
}

// Len returns the number of entries in the cache (including expired ones).
func (c *TTLCache[V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.items)
}
