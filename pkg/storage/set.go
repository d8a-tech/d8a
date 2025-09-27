package storage

// Set defines the interface for set storage.
type Set interface {
	Add(key []byte, value []byte) error
	All(key []byte) ([][]byte, error)
	Delete(key []byte) error
}
