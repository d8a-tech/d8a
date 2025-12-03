package protosessionsv3

import (
	"context"
	"fmt"
	"strconv"

	"github.com/d8a-tech/d8a/pkg/storage"
)

const (
	// NextBucketKeyPrefix is the key prefix for storing the next bucket to process.
	NextBucketKeyPrefix = "timingwheel.next"
)

type genericKVTimingWheelBackend struct {
	kv   storage.KV
	name string
}

// GetNextBucket retrieves the next bucket number to process from storage.
func (b *genericKVTimingWheelBackend) GetNextBucket(_ context.Context) (int64, error) {
	key := b.nextBucketKey()
	value, err := b.kv.Get([]byte(key))
	if err != nil {
		return 0, fmt.Errorf("failed to get next bucket: %w", err)
	}

	// First run - no bucket has been stored yet
	if len(value) == 0 {
		return -1, nil
	}

	bucketNumber, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse bucket number: %w", err)
	}

	return bucketNumber, nil
}

// SaveNextBucket persists the next bucket number to process.
func (b *genericKVTimingWheelBackend) SaveNextBucket(_ context.Context, bucketNumber int64) error {
	key := b.nextBucketKey()
	_, err := b.kv.Set([]byte(key), []byte(strconv.FormatInt(bucketNumber, 10)))
	if err != nil {
		return fmt.Errorf("failed to save next bucket: %w", err)
	}
	return nil
}

func (b *genericKVTimingWheelBackend) nextBucketKey() string {
	if b.name != "" {
		return fmt.Sprintf("%s.%s", NextBucketKeyPrefix, b.name)
	}
	return NextBucketKeyPrefix
}

// NewGenericKVTimingWheelBackend creates a TickerStateBackend using generic storage interfaces.
func NewGenericKVTimingWheelBackend(
	name string,
	kv storage.KV,
) TimingWheelStateBackend {
	return &genericKVTimingWheelBackend{
		kv:   kv,
		name: name,
	}
}
