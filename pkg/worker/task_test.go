package worker

import (
	"errors"
	"testing"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/stretchr/testify/assert"
)

type testTaskData struct {
	Field string `json:"field"`
}

func TestGenericTaskHandler(t *testing.T) {
	tests := []struct {
		name        string
		taskType    string
		data        []byte
		headers     map[string]string
		expectedErr *Error
	}{
		{
			name:     "should process valid task data",
			taskType: "test-type",
			data:     []byte(`{"field": "test"}`),
			headers:  map[string]string{"key": "value"},
		},
		{
			name:        "should return error for invalid json",
			taskType:    "test-type",
			data:        []byte(`invalid json`),
			headers:     map[string]string{"key": "value"},
			expectedErr: NewError(ErrTypeDroppable, errors.New("invalid character 'i' looking for beginning of value")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			processor := func(_ map[string]string, _ *testTaskData) *Error {
				if tt.expectedErr != nil {
					return tt.expectedErr
				}
				return nil
			}

			handler := NewGenericTaskHandler(
				tt.taskType,
				encoding.JSONDecoder,
				processor,
			)

			// when
			err := handler.Process(tt.headers, tt.data)

			// then
			if tt.expectedErr != nil {
				assert.NotNil(t, err)
				assert.Equal(t, tt.expectedErr.Type, err.Type)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestGenericTaskHandler_TaskType(t *testing.T) {
	tests := []struct {
		name     string
		taskType string
	}{
		{
			name:     "should return correct task type",
			taskType: "test-type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			handler := NewGenericTaskHandler(
				tt.taskType,
				encoding.JSONDecoder,
				func(_ map[string]string, _ *testTaskData) *Error { return nil },
			)

			// when
			result := handler.TaskType()

			// then
			assert.Equal(t, tt.taskType, result)
		})
	}
}
