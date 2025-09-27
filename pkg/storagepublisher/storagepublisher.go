package storagepublisher

import (
	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/worker"
)

// StoragePublisherAdapter translates hits to a task and publishes it to the storage
type StoragePublisherAdapter struct {
	encoder   encoding.EncoderFunc
	publisher worker.Publisher
}

// Push distributes hits to the storage
func (p *StoragePublisherAdapter) Push(theHits []*hits.Hit) error {
	taskData, err := worker.SerializeTaskData(p.encoder, hits.HitProcessingTask{
		Hits: theHits,
	})
	if err != nil {
		return err
	}
	return p.publisher.Publish(
		worker.NewTask(
			hits.HitProcessingTaskName,
			map[string]string{},
			taskData,
		),
	)
}

// NewStoragePublisherAdapter creates a new StoragePublisherAdapter instance
func NewStoragePublisherAdapter(encoder encoding.EncoderFunc, publisher worker.Publisher) *StoragePublisherAdapter {
	return &StoragePublisherAdapter{
		encoder:   encoder,
		publisher: publisher,
	}
}
