package worker

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFilesystemDirectoryPublisher_Publish(t *testing.T) {
	tests := []struct {
		name    string
		task    *Task
		wantErr bool
	}{
		{
			name: "publishes task to file",
			task: &Task{
				Type:    "test-type",
				Headers: map[string]string{"key": "value"},
				Body:    []byte("test body"),
			},
			wantErr: false,
		},
		{
			name: "publishes task with empty body",
			task: &Task{
				Type:    "empty",
				Headers: map[string]string{},
				Body:    nil,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			dir := t.TempDir()
			format := NewBinaryMessageFormat()
			publisher, err := NewFilesystemDirectoryPublisher(dir, format)
			assert.NoError(t, err)

			// when
			err = publisher.Publish(tt.task)

			// then
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			files, err := os.ReadDir(dir)
			assert.NoError(t, err)
			assert.Len(t, files, 1)
			assert.Equal(t, ".task", filepath.Ext(files[0].Name()))
			assert.NotEqual(t, ".tmp", filepath.Ext(files[0].Name()))
		})
	}
}

func TestFilesystemDirectoryConsumer_Consume(t *testing.T) {
	tests := []struct {
		name      string
		tasks     []*Task
		wantCount int
	}{
		{
			name: "consumes single task",
			tasks: []*Task{
				{
					Type:    "single",
					Headers: map[string]string{"h": "v"},
					Body:    []byte("data"),
				},
			},
			wantCount: 1,
		},
		{
			name: "consumes multiple tasks in order",
			tasks: []*Task{
				{Type: "first", Headers: map[string]string{}, Body: []byte("1")},
				{Type: "second", Headers: map[string]string{}, Body: []byte("2")},
				{Type: "third", Headers: map[string]string{}, Body: []byte("3")},
			},
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			dir := t.TempDir()
			format := NewBinaryMessageFormat()
			ctx, cancel := context.WithCancel(context.Background())

			publisher, err := NewFilesystemDirectoryPublisher(dir, format)
			assert.NoError(t, err)

			for _, task := range tt.tasks {
				err := publisher.Publish(task)
				assert.NoError(t, err)
				time.Sleep(time.Microsecond)
			}

			consumer, err := NewFilesystemDirectoryConsumer(ctx, dir, format)
			assert.NoError(t, err)

			var received []*Task
			var mu sync.Mutex

			// when
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = consumer.Consume(func(task *Task) error {
					mu.Lock()
					received = append(received, task)
					if len(received) >= tt.wantCount {
						cancel()
					}
					mu.Unlock()
					return nil
				})
			}()
			wg.Wait()

			// then
			assert.Len(t, received, tt.wantCount)

			files, err := os.ReadDir(dir)
			assert.NoError(t, err)
			assert.Empty(t, files)
		})
	}
}

func TestFilesystemDirectoryConsumer_ProcessNextBatchIgnoresTmpFiles(t *testing.T) {
	// given
	dir := t.TempDir()
	format := NewBinaryMessageFormat()
	ctx := context.Background()

	consumer, err := NewFilesystemDirectoryConsumer(ctx, dir, format)
	assert.NoError(t, err)

	tmpPath := filepath.Join(dir, "123.tmp")
	err = os.WriteFile(tmpPath, []byte("orphan"), 0o600)
	assert.NoError(t, err)

	handlerCalled := false

	// when
	processed, err := consumer.processNextBatch(func(_ *Task) error {
		handlerCalled = true
		return nil
	})

	// then
	assert.NoError(t, err)
	assert.False(t, processed)
	assert.False(t, handlerCalled)

	_, err = os.Stat(tmpPath)
	assert.NoError(t, err)
}

func TestFilesystemDirectoryConsumer_ProcessNextBatchDiscardsEmptyTaskFile(t *testing.T) {
	// given
	dir := t.TempDir()
	format := NewBinaryMessageFormat()
	ctx := context.Background()

	consumer, err := NewFilesystemDirectoryConsumer(ctx, dir, format)
	assert.NoError(t, err)

	emptyTaskPath := filepath.Join(dir, "123.task")
	err = os.WriteFile(emptyTaskPath, nil, 0o600)
	assert.NoError(t, err)

	handlerCalled := false

	// when
	processed, err := consumer.processNextBatch(func(_ *Task) error {
		handlerCalled = true
		return nil
	})

	// then
	assert.NoError(t, err)
	assert.True(t, processed)
	assert.False(t, handlerCalled)

	_, err = os.Stat(emptyTaskPath)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestFilesystemDirectoryConsumer_ProcessNextBatchDoesNotDiscardValidTask(t *testing.T) {
	// given
	dir := t.TempDir()
	format := NewBinaryMessageFormat()
	ctx := context.Background()

	consumer, err := NewFilesystemDirectoryConsumer(ctx, dir, format)
	assert.NoError(t, err)

	emptyTaskPath := filepath.Join(dir, "100.task")
	err = os.WriteFile(emptyTaskPath, nil, 0o600)
	assert.NoError(t, err)

	validTask := &Task{Type: "valid", Headers: map[string]string{"k": "v"}, Body: []byte("body")}
	data, err := format.Serialize(validTask)
	assert.NoError(t, err)

	validTaskPath := filepath.Join(dir, "101.task")
	err = os.WriteFile(validTaskPath, data, 0o600)
	assert.NoError(t, err)

	var received []*Task

	// when
	processed, err := consumer.processNextBatch(func(task *Task) error {
		received = append(received, task)
		return nil
	})

	// then
	assert.NoError(t, err)
	assert.True(t, processed)
	assert.Len(t, received, 1)
	assert.Equal(t, validTask.Type, received[0].Type)
	assert.Equal(t, validTask.Headers, received[0].Headers)
	assert.Equal(t, validTask.Body, received[0].Body)

	_, err = os.Stat(emptyTaskPath)
	assert.ErrorIs(t, err, os.ErrNotExist)

	_, err = os.Stat(validTaskPath)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestFilesystemDirectoryConsumer_ConsumeOrder(t *testing.T) {
	// given
	dir := t.TempDir()
	format := NewBinaryMessageFormat()
	ctx, cancel := context.WithCancel(context.Background())

	publisher, err := NewFilesystemDirectoryPublisher(dir, format)
	assert.NoError(t, err)

	tasks := []string{"first", "second", "third"}
	for _, taskType := range tasks {
		err := publisher.Publish(&Task{
			Type:    taskType,
			Headers: map[string]string{},
			Body:    []byte(taskType),
		})
		assert.NoError(t, err)
		time.Sleep(time.Microsecond)
	}

	consumer, err := NewFilesystemDirectoryConsumer(ctx, dir, format)
	assert.NoError(t, err)

	var order []string
	var mu sync.Mutex

	// when
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = consumer.Consume(func(task *Task) error {
			mu.Lock()
			order = append(order, task.Type)
			if len(order) >= len(tasks) {
				cancel()
			}
			mu.Unlock()
			return nil
		})
	}()
	wg.Wait()

	// then
	assert.Equal(t, tasks, order)
}

func TestFilesystemDirectoryConsumer_ContextCancellation(t *testing.T) {
	// given
	dir := t.TempDir()
	format := NewBinaryMessageFormat()
	ctx, cancel := context.WithCancel(context.Background())

	consumer, err := NewFilesystemDirectoryConsumer(ctx, dir, format)
	assert.NoError(t, err)

	done := make(chan struct{})

	// when
	go func() {
		_ = consumer.Consume(func(_ *Task) error {
			return nil
		})
		close(done)
	}()

	cancel()

	// then
	select {
	case <-done:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Fatal("consumer did not stop after context cancellation")
	}
}

func TestNewFilesystemDirectoryPublisher_CreatesDirectory(t *testing.T) {
	// given
	dir := filepath.Join(t.TempDir(), "nested", "path")
	format := NewBinaryMessageFormat()

	// when
	publisher, err := NewFilesystemDirectoryPublisher(dir, format)

	// then
	assert.NoError(t, err)
	assert.NotNil(t, publisher)

	info, err := os.Stat(dir)
	assert.NoError(t, err)
	assert.True(t, info.IsDir())
}

func TestNewFilesystemDirectoryConsumer_NonexistentDirectory(t *testing.T) {
	// given
	dir := filepath.Join(t.TempDir(), "nonexistent")
	format := NewBinaryMessageFormat()
	ctx := context.Background()

	// when
	consumer, err := NewFilesystemDirectoryConsumer(ctx, dir, format)

	// then
	assert.Error(t, err)
	assert.Nil(t, consumer)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestFilesystemDirectoryConsumer_HandlerErrorPreservesTaskFile(t *testing.T) {
	// given
	dir := t.TempDir()
	format := NewBinaryMessageFormat()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	publisher, err := NewFilesystemDirectoryPublisher(dir, format)
	assert.NoError(t, err)

	task := &Task{Type: "test", Headers: map[string]string{"k": "v"}, Body: []byte("data")}
	err = publisher.Publish(task)
	assert.NoError(t, err)

	consumer, err := NewFilesystemDirectoryConsumer(ctx, dir, format)
	assert.NoError(t, err)

	// when — handler returns context.Canceled (simulating shutdown)
	processed, batchErr := consumer.processNextBatch(func(_ *Task) error {
		return context.Canceled
	})

	// then — task file must still exist on disk for retry
	assert.False(t, processed)
	assert.ErrorIs(t, batchErr, context.Canceled)

	files, err := os.ReadDir(dir)
	assert.NoError(t, err)
	assert.Len(t, files, 1, "task file must be preserved when handler returns an error")
}

func TestExtractTimestamp(t *testing.T) {
	tests := []struct {
		filename string
		want     int64
	}{
		{"1234567890.task", 1234567890},
		{"0.task", 0},
		{"invalid.task", 0},
	}

	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			// when
			got := extractTimestamp(tt.filename)

			// then
			assert.Equal(t, tt.want, got)
		})
	}
}
