// Package storage provides interfaces and implementations for key-value and set storage.
package storage

// KeysOptions holds options for key retrieval.
type KeysOptions struct {
	MaxKeys uint32
}

// KeysOptionsFunc is a function that modifies KeysOptions.
type KeysOptionsFunc func(opts *KeysOptions)

// WithMaxKeys returns a KeysOptionsFunc that sets the maximum number of keys.
func WithMaxKeys(maxKeys uint32) KeysOptionsFunc {
	return func(opts *KeysOptions) {
		opts.MaxKeys = maxKeys
	}
}

// DefaultKeysOptions returns the default KeysOptions.
func DefaultKeysOptions() *KeysOptions {
	return &KeysOptions{
		MaxKeys: 0,
	}
}

// SetOptions holds options for the Set operation.
type SetOptions struct {
	SkipIfKeyAlreadyExists bool
	ReturnPreviousValue    bool
}

// SetOptionsFunc is a function that modifies SetOptions.
type SetOptionsFunc func(opts *SetOptions)

// DefaultSetOptions returns the default SetOptions.
func DefaultSetOptions() *SetOptions {
	return &SetOptions{
		SkipIfKeyAlreadyExists: false,
		ReturnPreviousValue:    false,
	}
}

// WithSkipIfKeyAlreadyExists returns a SetOptionsFunc that configures whether to skip
// setting a value if the key already exists.
func WithSkipIfKeyAlreadyExists(skipIfKeyAlreadyExists bool) SetOptionsFunc {
	return func(opts *SetOptions) {
		opts.SkipIfKeyAlreadyExists = skipIfKeyAlreadyExists
	}
}

// WithReturnPreviousValue returns a SetOptionsFunc that configures whether to return the previous
// value when setting a new one.
func WithReturnPreviousValue(get bool) SetOptionsFunc {
	return func(opts *SetOptions) {
		opts.ReturnPreviousValue = get
	}
}

// KV defines the interface for key-value storage.
type KV interface {
	Get(key []byte) ([]byte, error)
	Set(key []byte, value []byte, opts ...SetOptionsFunc) ([]byte, error)
	Delete(key []byte) error
}
