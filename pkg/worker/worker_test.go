package worker

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// mockMiddleware is a test middleware implementation
type mockMiddleware struct {
	handleFunc func(task *Task, next func() *Error) *Error
}

func (m *mockMiddleware) Handle(task *Task, next func() *Error) *Error {
	return m.handleFunc(task, next)
}

// mockTaskHandler is a test task handler implementation
type mockTaskHandler struct {
	taskType    string
	processFunc func(headers map[string]string, data []byte) *Error
}

func (h *mockTaskHandler) TaskType() string {
	return h.taskType
}

func (h *mockTaskHandler) Process(headers map[string]string, data []byte) *Error {
	return h.processFunc(headers, data)
}

func TestWorker_Process(t *testing.T) {
	tests := []struct {
		name        string
		task        *Task
		handlers    []TaskHandler
		middleware  []Middleware
		wantErr     bool
		errContains string
	}{
		{
			name: "successful task processing with no middleware",
			task: &Task{
				Type:    "test",
				Headers: map[string]string{},
				Body:    []byte("test data"),
			},
			handlers: []TaskHandler{
				&mockTaskHandler{
					taskType: "test",
					processFunc: func(_ map[string]string, _ []byte) *Error {
						return nil
					},
				},
			},
			middleware: nil,
			wantErr:    false,
		},
		{
			name: "successful task processing with single handler",
			task: &Task{
				Type:    "test",
				Headers: map[string]string{},
				Body:    []byte("test data"),
			},
			handlers: []TaskHandler{
				&mockTaskHandler{
					taskType: "test",
					processFunc: func(_ map[string]string, _ []byte) *Error {
						return nil
					},
				},
			},
			middleware: []Middleware{
				&mockMiddleware{
					handleFunc: func(_ *Task, next func() *Error) *Error {
						return next()
					},
				},
			},
			wantErr: false,
		},
		{
			name: "task type not found",
			task: &Task{
				Type: "unknown",
			},
			handlers: []TaskHandler{
				&mockTaskHandler{
					taskType: "test",
					processFunc: func(_ map[string]string, _ []byte) *Error {
						return nil
					},
				},
			},
			wantErr:     true,
			errContains: "no handler for task type",
		},
		{
			name: "handler returns error (retryable)",
			task: &Task{
				Type: "test",
			},
			handlers: []TaskHandler{
				&mockTaskHandler{
					taskType: "test",
					processFunc: func(_ map[string]string, _ []byte) *Error {
						return NewError(ErrTypeRetryable, errors.New("handler error"))
					},
				},
			},
			middleware: []Middleware{
				&mockMiddleware{
					handleFunc: func(_ *Task, next func() *Error) *Error {
						return next()
					},
				},
			},
			wantErr:     true,
			errContains: "handler error",
		},
		{
			name: "middleware returns error",
			task: &Task{
				Type: "test",
			},
			handlers: []TaskHandler{
				&mockTaskHandler{
					taskType: "test",
					processFunc: func(_ map[string]string, _ []byte) *Error {
						return nil
					},
				},
			},
			middleware: []Middleware{
				&mockMiddleware{
					handleFunc: func(_ *Task, _ func() *Error) *Error {
						return NewError(ErrTypeRetryable, errors.New("middleware error"))
					},
				},
			},
			wantErr:     true,
			errContains: "middleware error",
		},
		{
			name: "droppable error is not propagated",
			task: &Task{
				Type: "test",
			},
			handlers: []TaskHandler{
				&mockTaskHandler{
					taskType: "test",
					processFunc: func(_ map[string]string, _ []byte) *Error {
						return NewError(ErrTypeDroppable, errors.New("droppable error"))
					},
				},
			},
			middleware: []Middleware{
				&mockMiddleware{
					handleFunc: func(_ *Task, next func() *Error) *Error {
						return next()
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			worker := NewWorker(tt.handlers, tt.middleware)

			// when
			err := worker.Process(tt.task)

			// then
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errContains != "" {
					assert.ErrorContains(t, err, tt.errContains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
