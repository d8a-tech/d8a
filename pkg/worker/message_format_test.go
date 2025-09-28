package worker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBinaryMessageFormat_Serialize(t *testing.T) {
	tests := []struct {
		name    string
		task    *Task
		wantErr bool
	}{
		{
			name: "valid task with headers and data",
			task: &Task{
				Type:    "test_type",
				Headers: map[string]string{"key": "value"},
				Body:    []byte("test data"),
			},
			wantErr: false,
		},
		{
			name: "task with empty headers",
			task: &Task{
				Type:    "test_type",
				Headers: map[string]string{},
				Body:    []byte("test data"),
			},
			wantErr: false,
		},
		{
			name: "task with empty data",
			task: &Task{
				Type:    "test_type",
				Headers: map[string]string{"key": "value"},
				Body:    []byte{},
			},
			wantErr: false,
		},
		{
			name: "task with type too long",
			task: &Task{
				Type:    string(make([]byte, 256)), // 256 bytes is too long
				Headers: map[string]string{"key": "value"},
				Body:    []byte("test data"),
			},
			wantErr: true,
		},
		{
			name: "task with headers too long",
			task: &Task{
				Type: "test_type",
				Headers: map[string]string{
					"key": string(make([]byte, 65536)), // 65,536 bytes is too long
				},
				Body: []byte("test data"),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			format := NewBinaryMessageFormat()

			// when
			got, err := format.Serialize(tt.task)

			// then
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.NotNil(t, got)

			// verify we can deserialize back to the same task
			deserialized, err := format.Deserialize(got)
			assert.NoError(t, err)
			assert.Equal(t, tt.task.Type, deserialized.Type)
			assert.Equal(t, tt.task.Headers, deserialized.Headers)
			assert.Equal(t, tt.task.Body, deserialized.Body)
		})
	}
}

func TestBinaryMessageFormat_Deserialize(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{
			name:    "empty data",
			data:    []byte{},
			wantErr: true,
		},
		{
			name:    "invalid type length",
			data:    []byte{10}, // only type length byte, missing type string
			wantErr: true,
		},
		{
			name:    "invalid header length",
			data:    []byte{1, 'a', 0}, // type length=1, type='a', missing second header length byte
			wantErr: true,
		},
		{
			name:    "invalid headers",
			data:    []byte{1, 'a', 0, 5, '{', 'i', 'n', 'v', 'a'}, // invalid JSON
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			format := NewBinaryMessageFormat()

			// when
			got, err := format.Deserialize(tt.data)

			// then
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, got)
				return
			}
			assert.NoError(t, err)
			assert.NotNil(t, got)
		})
	}
}
