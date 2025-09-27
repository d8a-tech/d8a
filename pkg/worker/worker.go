package worker

import (
	"bytes"
	"fmt"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/sirupsen/logrus"
)

// TaskHandlerFunc is a function that handles a task
type TaskHandlerFunc func(task *Task) error

// Consumer defines an interface for task consumers that provide a channel of tasks
type Consumer interface {
	Consume(handler TaskHandlerFunc) error
}

// Publisher defines an interface for task publishers that can publish tasks
type Publisher interface {
	Publish(task *Task) error
}

// Middleware defines an interface for task processing middleware
type Middleware interface {
	Handle(task *Task, next func() *Error) *Error
}

// SerializeTaskData serializes task data using the provided encoder
func SerializeTaskData[T any](
	encoder encoding.EncoderFunc,
	task T,
) ([]byte, error) {
	var buf bytes.Buffer

	// Write task data
	_, err := encoder(&buf, task)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Worker represents a task processing worker that can handle multiple task types
type Worker struct {
	handlers   map[string][]TaskHandler
	middleware []Middleware
}

// Process handles a task by applying middleware and passing it to appropriate handlers
func (w *Worker) Process(task *Task) error {
	handlers, ok := w.handlers[task.Type]
	if !ok {
		return fmt.Errorf("no handler for task type %s, registered handlers: %v", task.Type, w.handlers)
	}
	// If there are no middlewares, process the handlers directly
	if len(w.middleware) == 0 {
		for _, handler := range handlers {
			if err := handler.Process(task.Headers, task.Body); err != nil {
				if err.Type == ErrTypeDroppable {
					return nil
				}
				return err
			}
		}
		logrus.Debugf("Processed task of type `%s` with %d handlers", task.Type, len(handlers))
		return nil
	}
	for i, mw := range w.middleware {
		var next func() *Error
		if i == len(w.middleware)-1 {
			next = func() *Error {
				for _, handler := range handlers {
					if err := handler.Process(task.Headers, task.Body); err != nil {
						if err.Type == ErrTypeDroppable {
							return nil
						}
						return err
					}
				}
				return nil
			}
		} else {
			next = func() *Error {
				return w.middleware[i+1].Handle(task, next)
			}
		}
		if err := mw.Handle(task, next); err != nil {
			return err
		}
	}
	logrus.Debugf("Processed task of type `%s` with %d handlers", task.Type, len(handlers))
	return nil
}

// NewWorker creates a new worker instance with the specified handlers and middleware
func NewWorker(handlers []TaskHandler, middleware []Middleware) *Worker {
	handlersM := make(map[string][]TaskHandler)
	for _, handler := range handlers {
		handlersM[handler.TaskType()] = append(handlersM[handler.TaskType()], handler)
	}
	return &Worker{handlers: handlersM, middleware: middleware}
}
