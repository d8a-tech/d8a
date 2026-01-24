package schema

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testColumn is a test implementation of Column interface
type testColumn struct {
	docs       Documentation
	implements Interface
}

func (m *testColumn) Docs() Documentation {
	return m.docs
}

func (m *testColumn) Implements() Interface {
	return m.implements
}

func (m *testColumn) DependsOn() []DependsOnEntry {
	return nil
}

func TestWithExtraFields_DescriptionMetadata(t *testing.T) {
	tests := []struct {
		name        string
		description string
		wantPresent bool
	}{
		{
			name:        "description present",
			description: "Test column description",
			wantPresent: true,
		},
		{
			name:        "empty description",
			description: "",
			wantPresent: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := &testColumn{
				docs: Documentation{
					ColumnName:  "test_field",
					Description: tt.description,
					Type:        &arrow.Field{Name: "test_field", Type: arrow.BinaryTypes.String},
				},
				implements: Interface{
					ID:    "test.id",
					Field: &arrow.Field{Name: "test_field", Type: arrow.BinaryTypes.String},
				},
			}

			schema := WithExtraFields([]*testColumn{col})
			require.Len(t, schema.Fields(), 1)

			field := schema.Field(0)
			desc, ok := warehouse.GetArrowMetadataValue(field.Metadata, warehouse.ColumnDescriptionMetadataKey)

			if tt.wantPresent {
				assert.True(t, ok, "description metadata should be present")
				assert.Equal(t, tt.description, desc)
			} else {
				assert.False(t, ok, "description metadata should not be present")
			}
		})
	}
}

func TestWithExtraFields_PreservesExistingMetadata(t *testing.T) {
	existingMetadata := arrow.NewMetadata([]string{"existing_key"}, []string{"existing_value"})
	col := &testColumn{
		docs: Documentation{
			ColumnName:  "test_field",
			Description: "test description",
			Type:        &arrow.Field{Name: "test_field", Type: arrow.BinaryTypes.String},
		},
		implements: Interface{
			ID: "test.id",
			Field: &arrow.Field{
				Name:     "test_field",
				Type:     arrow.BinaryTypes.String,
				Metadata: existingMetadata,
			},
		},
	}

	schema := WithExtraFields([]*testColumn{col})
	field := schema.Field(0)

	// Verify description was added
	desc, ok := warehouse.GetArrowMetadataValue(field.Metadata, warehouse.ColumnDescriptionMetadataKey)
	assert.True(t, ok)
	assert.Equal(t, "test description", desc)

	// Verify existing metadata was preserved
	existingVal, ok := warehouse.GetArrowMetadataValue(field.Metadata, "existing_key")
	assert.True(t, ok)
	assert.Equal(t, "existing_value", existingVal)
}

func TestWithExtraFields_DoesNotMutateOriginalField(t *testing.T) {
	originalField := &arrow.Field{
		Name:     "test_field",
		Type:     arrow.BinaryTypes.String,
		Metadata: arrow.Metadata{},
	}

	col := &testColumn{
		docs: Documentation{
			ColumnName:  "test_field",
			Description: "test description",
			Type:        originalField,
		},
		implements: Interface{
			ID:    "test.id",
			Field: originalField,
		},
	}

	schema := WithExtraFields([]*testColumn{col})
	field := schema.Field(0)

	// Verify description was added to schema field
	_, ok := warehouse.GetArrowMetadataValue(field.Metadata, warehouse.ColumnDescriptionMetadataKey)
	assert.True(t, ok)

	// Verify original field was not mutated
	_, ok = warehouse.GetArrowMetadataValue(originalField.Metadata, warehouse.ColumnDescriptionMetadataKey)
	assert.False(t, ok, "original field should not be mutated")
}
