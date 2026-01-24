package schema

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
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
	}{
		{
			name:        "description present",
			description: "Test column description",
		},
		{
			name:        "empty description",
			description: "",
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
			// WithExtraFields does not inject documentation into metadata.
			assert.Equal(t, arrow.Metadata{}, field.Metadata)
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

	// Verify existing metadata was preserved
	assert.Equal(t, existingMetadata, field.Metadata)
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

	// Verify original field was not mutated
	assert.Equal(t, arrow.Metadata{}, originalField.Metadata)
	assert.Equal(t, arrow.Metadata{}, field.Metadata)
}
