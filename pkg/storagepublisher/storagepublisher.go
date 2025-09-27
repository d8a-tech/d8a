// Package storagepublisher provides an adapter to publish hits to a worker-backed storage.
package storagepublisher

import (
	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/worker"
)

// Adapter translates hits to a task and publishes it to the storage
type Adapter struct {
	encoder   encoding.EncoderFunc
	publisher worker.Publisher
}

// Push distributes hits to the storage
func (p *Adapter) Push(theHits []*hits.Hit) error {
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

// NewAdapter creates a new Adapter instance
func NewAdapter(encoder encoding.EncoderFunc, publisher worker.Publisher) *Adapter {
	return &Adapter{
		encoder:   encoder,
		publisher: publisher,
	}
}
