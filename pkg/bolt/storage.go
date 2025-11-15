package bolt

import (
	"bytes"

	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/d8a-tech/d8a/pkg/util"
	bolt "go.etcd.io/bbolt"
)

const (
	kvBucketName     = "kv"
	setBucketName    = "set"
	emptyValueMarker = "__EMPTY_VALUE__"
)

type boltKV struct {
	db *bolt.DB
}

func (b *boltKV) Get(key []byte) ([]byte, error) {
	if key == nil {
		return nil, storage.ErrNilKey
	}
	if len(key) == 0 {
		return nil, storage.ErrEmptyKey
	}

	isNil := true
	var result []byte
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(kvBucketName))
		if bucket == nil {
			return nil
		}
		value := bucket.Get(key)
		if value == nil {
			isNil = true
		} else {
			result = make([]byte, len(value))
			copy(result, value)
			isNil = false
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if isNil {
		return nil, nil
	}
	return result, nil
}

func (b *boltKV) Set(key, value []byte, opts ...storage.SetOptionsFunc) ([]byte, error) {
	if key == nil {
		return nil, storage.ErrNilKey
	}
	if len(key) == 0 {
		return nil, storage.ErrEmptyKey
	}
	if value == nil {
		return nil, storage.ErrNilValue
	}

	options := storage.DefaultSetOptions()
	for _, opt := range opts {
		opt(options)
	}

	var oldValue []byte
	var skipPut bool
	var keyExists bool

	err := b.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(kvBucketName))
		if err != nil {
			return err
		}

		// Get old value if requested or if we need to check for key existence
		existingValue := bucket.Get(key)
		if existingValue != nil {
			keyExists = true
			// Check if key exists and handle SkipIfKeyAlreadyExists
			if options.SkipIfKeyAlreadyExists {
				skipPut = true
			}

			// Get old value only if specifically requested with Get option
			if options.ReturnPreviousValue {
				oldValue = make([]byte, len(existingValue))
				copy(oldValue, existingValue)
			}
		}

		if !skipPut {
			return bucket.Put(key, value)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Special case for handling combined options: if both options are true and it's a new key
	if options.ReturnPreviousValue && options.SkipIfKeyAlreadyExists && !keyExists {
		return nil, nil
	}

	return oldValue, nil
}

func (b *boltKV) Delete(key []byte) error {
	if key == nil {
		return storage.ErrNilKey
	}
	if len(key) == 0 {
		return storage.ErrEmptyKey
	}

	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(kvBucketName))
		if bucket == nil {
			return nil
		}
		return bucket.Delete(key)
	})
}

func (b *boltKV) Keys(prefix []byte, opts ...storage.KeysOptionsFunc) ([][]byte, error) {
	options := storage.DefaultKeysOptions()
	for _, opt := range opts {
		opt(options)
	}

	var keys [][]byte

	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(kvBucketName))
		if bucket == nil {
			return nil
		}
		c := bucket.Cursor()
		for k, _ := c.Seek(prefix); k != nil && (len(
			prefix,
		) == 0 || len(k) >= len(prefix) && bytes.Equal(k[:len(prefix)], prefix)); k, _ = c.Next() {
			// Make a copy of the key since it's only valid during the transaction
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)

			keys = append(keys, keyCopy)

			// Check if we've reached the MaxKeys limit
			if options.MaxKeys > 0 && util.SafeIntToUint32(len(keys)) >= options.MaxKeys {
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}

type boltSet struct {
	db *bolt.DB
}

func (b *boltSet) Add(key, value []byte) error {
	if key == nil {
		return storage.ErrNilKey
	}
	if value == nil {
		return storage.ErrNilValue
	}
	if len(key) == 0 {
		return storage.ErrEmptyKey
	}

	return b.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(setBucketName))
		if err != nil {
			return err
		}
		keyBucket, err := bucket.CreateBucketIfNotExists(key)
		if err != nil {
			return err
		}

		valueKey := value
		if len(value) == 0 {
			valueKey = []byte(emptyValueMarker)
		}

		return keyBucket.Put(valueKey, []byte{1})
	})
}

func (b *boltSet) All(key []byte) ([][]byte, error) {
	var values [][]byte
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(setBucketName))
		if bucket == nil {
			return nil
		}
		keyBucket := bucket.Bucket(key)
		if keyBucket == nil {
			return nil
		}
		return keyBucket.ForEach(func(k, _ []byte) error {
			// Check if it's our special empty value marker
			if string(k) == emptyValueMarker {
				values = append(values, []byte{})
				return nil
			}

			// Make a copy of the key since it's only valid during the transaction
			value := make([]byte, len(k))
			copy(value, k)
			values = append(values, value)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return values, nil
}

func (b *boltSet) Drop(key []byte) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(setBucketName))
		if bucket == nil {
			return nil
		}
		keyBucket := bucket.Bucket(key)
		if keyBucket == nil {
			return nil
		}
		return bucket.DeleteBucket(key)
	})
}

func (b *boltSet) Delete(key, value []byte) error {
	if key == nil {
		return storage.ErrNilKey
	}
	if len(key) == 0 {
		return storage.ErrEmptyKey
	}
	if value == nil {
		return storage.ErrNilValue
	}

	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(setBucketName))
		if bucket == nil {
			return nil
		}
		keyBucket := bucket.Bucket(key)
		if keyBucket == nil {
			return nil
		}

		valueKey := value
		if len(value) == 0 {
			valueKey = []byte(emptyValueMarker)
		}

		if err := keyBucket.Delete(valueKey); err != nil {
			return err
		}

		if keyBucket.Stats().KeyN == 0 {
			return bucket.DeleteBucket(key)
		}

		return nil
	})
}

// NewBoltKV creates a new KV implementation using BoltDB
func NewBoltKV(dbPath string) (storage.KV, error) {
	db, err := bolt.Open(dbPath, 0o600, nil)
	if err != nil {
		return nil, err
	}
	return &boltKV{db: db}, nil
}

// NewBoltSet creates a new Set implementation using BoltDB
func NewBoltSet(dbPath string) (storage.Set, error) {
	db, err := bolt.Open(dbPath, 0o600, nil)
	if err != nil {
		return nil, err
	}
	return &boltSet{db: db}, nil
}
