package storage

import (
	"sort"
	"sync"

	"github.com/d8a-tech/d8a/pkg/util"
)

// InMemoryKV is an in-memory implementation of the KV interface.
type InMemoryKV struct {
	mu sync.RWMutex
	KV map[string][]byte
}

// Get retrieves a value from the in-memory storage by key.
func (k *InMemoryKV) Get(key []byte) ([]byte, error) {
	if key == nil {
		return nil, ErrNilKey
	}
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}

	k.mu.RLock()
	defer k.mu.RUnlock()
	val, exists := k.KV[string(key)]
	if !exists {
		return nil, nil
	}
	return val, nil
}

// Set stores a value in the in-memory storage with the given key.
func (k *InMemoryKV) Set(key, value []byte, opts ...SetOptionsFunc) ([]byte, error) {
	if key == nil {
		return nil, ErrNilKey
	}
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}
	if value == nil {
		return nil, ErrNilValue
	}

	options := DefaultSetOptions()
	for _, opt := range opts {
		opt(options)
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	// Store the old value if needed for either SkipIfKeyAlreadyExists or Get
	var oldValue []byte
	var exists bool
	var keyStr = string(key)

	if options.ReturnPreviousValue || options.SkipIfKeyAlreadyExists {
		val, keyExists := k.KV[keyStr]
		exists = keyExists

		if exists && options.ReturnPreviousValue {
			oldValue = make([]byte, len(val))
			copy(oldValue, val)
		}
	}

	// Skip the set operation if key exists and SkipIfKeyAlreadyExists is true
	if options.SkipIfKeyAlreadyExists && exists {
		return oldValue, nil
	}

	// Special case for handling combined options: if both options are true and we're setting a new key
	if options.ReturnPreviousValue && options.SkipIfKeyAlreadyExists && !exists {
		k.KV[keyStr] = value
		return nil, nil
	}

	k.KV[keyStr] = value
	return oldValue, nil
}

// Delete removes a value from the in-memory storage by key.
func (k *InMemoryKV) Delete(key []byte) error {
	if key == nil {
		return ErrNilKey
	}
	if len(key) == 0 {
		return ErrEmptyKey
	}

	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.KV, string(key))
	return nil
}

// Keys returns all keys in the in-memory storage that match the given prefix.
func (k *InMemoryKV) Keys(prefix []byte, opts ...KeysOptionsFunc) ([][]byte, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	options := DefaultKeysOptions()
	for _, opt := range opts {
		opt(options)
	}

	prefixStr := string(prefix)
	var matchingKeys []string

	for key := range k.KV {
		if prefixStr == "" || len(key) >= len(prefixStr) && key[:len(prefixStr)] == prefixStr {
			matchingKeys = append(matchingKeys, key)
		}
	}

	sort.Strings(matchingKeys)

	keys := make([][]byte, 0, len(k.KV))

	for _, key := range matchingKeys {
		keyBytes := []byte(key)
		keys = append(keys, keyBytes)

		if options.MaxKeys > 0 && util.SafeIntToUint32(len(keys)) >= options.MaxKeys {
			break
		}
	}
	return keys, nil
}

// NewInMemoryKV creates a new in-memory key-value store.
func NewInMemoryKV() KV {
	return &InMemoryKV{
		KV: make(map[string][]byte),
	}
}

// InMemorySet represents an in-memory set storage implementation.
type InMemorySet struct {
	mu sync.RWMutex
	HM map[string]map[string]struct{}
}

// Add adds a value to the in-memory set for the given key.
func (h *InMemorySet) Add(key, value []byte) error {
	if key == nil {
		return ErrNilKey
	}
	if value == nil {
		return ErrNilValue
	}
	if len(key) == 0 {
		return ErrEmptyKey
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	keyStr := string(key)
	if h.HM[keyStr] == nil {
		h.HM[keyStr] = make(map[string]struct{})
	}
	h.HM[keyStr][string(value)] = struct{}{}
	return nil
}

// All returns all values in the in-memory set for the given key.
func (h *InMemorySet) All(key []byte) ([][]byte, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	values := make([][]byte, 0, len(h.HM[string(key)]))
	for val := range h.HM[string(key)] {
		values = append(values, []byte(val))
	}
	return values, nil
}

// Delete removes a value from the in-memory set for the given key.
func (h *InMemorySet) Delete(key []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.HM, string(key))
	return nil
}

// NewInMemorySet creates a new in-memory set.
func NewInMemorySet() Set {
	return &InMemorySet{
		HM: make(map[string]map[string]struct{}),
	}
}
