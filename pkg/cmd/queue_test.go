package cmd

import (
	"context"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/worker"
	"github.com/stretchr/testify/assert"
	"gocloud.dev/blob/memblob"
)

func TestBuildObjectStorageQueue_PrefixIsolation(t *testing.T) {
	// given
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	bkt := memblob.OpenBucket(nil)

	qA, err := buildObjectStorageQueue(ctx, bkt, "envA/queue")
	assert.NoError(t, err)
	qB, err := buildObjectStorageQueue(ctx, bkt, "envB/queue")
	assert.NoError(t, err)

	assert.NoError(t, qA.Publisher.Publish(&worker.Task{Type: "t", Headers: map[string]string{}, Body: []byte("a1")}))
	assert.NoError(t, qB.Publisher.Publish(&worker.Task{Type: "t", Headers: map[string]string{}, Body: []byte("b1")}))

	processed := make(chan []byte, 2)
	go func() {
		_ = qA.Consumer.Consume(func(task *worker.Task) error {
			processed <- task.Body
			cancel()
			return nil
		})
	}()

	select {
	case body := <-processed:
		assert.Equal(t, []byte("a1"), body)
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for consumption")
	}

	// then: ensure the other prefix is still present
	ctxB, cancelB := context.WithCancel(context.Background())
	t.Cleanup(cancelB)
	qB2, err := buildObjectStorageQueue(ctxB, bkt, "envB/queue")
	assert.NoError(t, err)

	processedB := make(chan []byte, 1)
	go func() {
		_ = qB2.Consumer.Consume(func(task *worker.Task) error {
			processedB <- task.Body
			cancelB()
			return nil
		})
	}()

	select {
	case body := <-processedB:
		assert.Equal(t, []byte("b1"), body)
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for envB consumption")
	}
}
