package objectstorage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseTimestampFromKey(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		expectErr bool
	}{
		{
			name:      "valid key with prefix",
			key:       "1234567890123456789_task-id-123",
			expectErr: false,
		},
		{
			name:      "valid key without prefix",
			key:       "1234567890123456789_task-id-456",
			expectErr: false,
		},
		{
			name:      "invalid key format - no underscore",
			key:       "1234567890123456789",
			expectErr: true,
		},
		{
			name:      "invalid key format - empty",
			key:       "",
			expectErr: true,
		},
		{
			name:      "invalid timestamp - not numeric",
			key:       "notanumber_task-id",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// when
			timestamp, err := ParseTimestampFromKey(tt.key)

			// then
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Greater(t, timestamp, int64(0))
			}
		})
	}
}

func TestGenerateTaskID(t *testing.T) {
	// when
	id1 := GenerateTaskID()
	id2 := GenerateTaskID()

	// then
	assert.NotEmpty(t, id1)
	assert.NotEmpty(t, id2)
	assert.NotEqual(t, id1, id2) // Should generate unique IDs

	// UUID format validation (basic check)
	parts := strings.Split(id1, "-")
	assert.Equal(t, 5, len(parts), "UUID should have 5 parts separated by hyphens")
}
