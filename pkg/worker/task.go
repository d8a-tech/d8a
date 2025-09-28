package worker

import (
	"bytes"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/google/uuid"
)

// TaskHandler is an interface for processing tasks
type TaskHandler interface {
	TaskType() string
	Process(map[string]string, []byte) *Error
}

// Task represents a unit of work with type, headers and data
type Task struct {
	Type    string
	Headers map[string]string
	Body    []byte
}

// ID returns the ID of the task
func (t *Task) ID() string {
	return t.Headers[TaskIDHeaderKey]
}

// TaskIDHeaderKey is the key for the task ID in the headers
const TaskIDHeaderKey = "task_id"

// NewTask creates a new task with a random ID
func NewTask(taskType string, headers map[string]string, data []byte) *Task {
	_, hasTaskID := headers[TaskIDHeaderKey]
	if !hasTaskID {
		headers[TaskIDHeaderKey] = uuid.New().String()
	}
	return &Task{
		Type:    taskType,
		Headers: headers,
		Body:    data,
	}
}

type genericTaskHandler[T any] struct {
	theType   string
	decoder   encoding.DecoderFunc
	processor func(headers map[string]string, data *T) *Error
}

func (g *genericTaskHandler[T]) TaskType() string {
	return g.theType
}

func (g *genericTaskHandler[T]) Process(headers map[string]string, data []byte) *Error {
	var task T
	// Decoder error is fatal, so we drop the task
	if err := g.decoder(bytes.NewReader(data), &task); err != nil {
		return NewError(ErrTypeDroppable, err)
	}
	return g.processor(headers, &task)
}

// NewGenericTaskHandler creates a new generic task handler for a specific task type
func NewGenericTaskHandler[T any](
	theType string,
	decoder encoding.DecoderFunc,
	processor func(headers map[string]string, data *T) *Error,
) TaskHandler {
	return &genericTaskHandler[T]{
		theType:   theType,
		decoder:   decoder,
		processor: processor,
	}
}
