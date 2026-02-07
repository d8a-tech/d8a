package warehouse

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/warehouse/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetArrowMetadataValue(t *testing.T) {
	tests := []struct {
		name     string
		metadata arrow.Metadata
		key      string
		wantVal  string
		wantOk   bool
	}{
		{
			name:     "empty metadata",
			metadata: arrow.Metadata{},
			key:      "test",
			wantVal:  "",
			wantOk:   false,
		},
		{
			name:     "key exists",
			metadata: arrow.NewMetadata([]string{"key1", "key2"}, []string{"value1", "value2"}),
			key:      "key1",
			wantVal:  "value1",
			wantOk:   true,
		},
		{
			name:     "key does not exist",
			metadata: arrow.NewMetadata([]string{"key1", "key2"}, []string{"value1", "value2"}),
			key:      "key3",
			wantVal:  "",
			wantOk:   false,
		},
		{
			name:     "description key exists",
			metadata: arrow.NewMetadata([]string{meta.ColumnDescriptionMetadataKey}, []string{"test description"}),
			key:      meta.ColumnDescriptionMetadataKey,
			wantVal:  "test description",
			wantOk:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, ok := GetArrowMetadataValue(tt.metadata, tt.key)
			assert.Equal(t, tt.wantVal, val)
			assert.Equal(t, tt.wantOk, ok)
		})
	}
}

func TestMergeArrowMetadata(t *testing.T) {
	tests := []struct {
		name     string
		existing arrow.Metadata
		key      string
		value    string
		wantVal  string
		wantOk   bool
	}{
		{
			name:     "empty metadata, add new key",
			existing: arrow.Metadata{},
			key:      "newkey",
			value:    "newvalue",
			wantVal:  "newvalue",
			wantOk:   true,
		},
		{
			name:     "existing metadata, add new key",
			existing: arrow.NewMetadata([]string{"key1"}, []string{"value1"}),
			key:      "key2",
			value:    "value2",
			wantVal:  "value2",
			wantOk:   true,
		},
		{
			name:     "existing metadata, overwrite existing key",
			existing: arrow.NewMetadata([]string{"key1", "key2"}, []string{"value1", "value2"}),
			key:      "key1",
			value:    "newvalue1",
			wantVal:  "newvalue1",
			wantOk:   true,
		},
		{
			name:     "merge description into existing metadata",
			existing: arrow.NewMetadata([]string{"other"}, []string{"othervalue"}),
			key:      meta.ColumnDescriptionMetadataKey,
			value:    "column description",
			wantVal:  "column description",
			wantOk:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged := MergeArrowMetadata(tt.existing, tt.key, tt.value)
			val, ok := GetArrowMetadataValue(merged, tt.key)
			assert.Equal(t, tt.wantVal, val)
			assert.Equal(t, tt.wantOk, ok)

			// Verify existing keys are preserved (except the one we merged)
			if tt.existing.Len() > 0 {
				existingKeys := tt.existing.Keys()
				existingValues := tt.existing.Values()
				for i, k := range existingKeys {
					if k != tt.key {
						val, ok := GetArrowMetadataValue(merged, k)
						require.True(t, ok, "existing key %s should be preserved", k)
						assert.Equal(t, existingValues[i], val)
					}
				}
			}
		})
	}
}
