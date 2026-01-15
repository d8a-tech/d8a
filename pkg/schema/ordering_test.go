package schema

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
)

func TestInterfaceOrdering_RegisterInterfaceStruct(t *testing.T) {
	// given
	testInterfaces := struct {
		FirstInterface  Interface
		SecondInterface Interface
		ThirdInterface  Interface
	}{
		FirstInterface: Interface{
			ID:    "test/first",
			Field: &arrow.Field{Name: "first", Type: arrow.BinaryTypes.String},
		},
		SecondInterface: Interface{
			ID:    "test/second",
			Field: &arrow.Field{Name: "second", Type: arrow.BinaryTypes.String},
		},
		ThirdInterface: Interface{
			ID:    "test/third",
			Field: &arrow.Field{Name: "third", Type: arrow.BinaryTypes.String},
		},
	}

	// when
	ordering := NewInterfaceDefinitionOrderKeeper(testInterfaces)

	// then
	assert.Equal(t, 0, ordering.GetOrder("test/first"))
	assert.Equal(t, 1, ordering.GetOrder("test/second"))
	assert.Equal(t, 2, ordering.GetOrder("test/third"))
}

func TestInterfaceOrdering_GetOrder_UnknownInterface(t *testing.T) {
	// given
	ordering := NewInterfaceDefinitionOrderKeeper()

	// when
	order := ordering.GetOrder("unknown/interface")

	// then
	assert.Equal(t, 1<<63-1, order) // math.MaxInt
}

func TestInterfaceOrdering_RegisterInterfaceStruct_FirstRegistrationWins(t *testing.T) {
	// given
	firstStruct := struct {
		Interface Interface
	}{
		Interface: Interface{
			ID:    "test/duplicate",
			Field: &arrow.Field{Name: "first", Type: arrow.BinaryTypes.String},
		},
	}

	secondStruct := struct {
		Interface Interface
	}{
		Interface: Interface{
			ID:    "test/duplicate",
			Field: &arrow.Field{Name: "second", Type: arrow.BinaryTypes.String},
		},
	}

	// when
	ordering := NewInterfaceDefinitionOrderKeeper(firstStruct, secondStruct)

	// then
	// First registration should win, so order is 0
	assert.Equal(t, 0, ordering.GetOrder("test/duplicate"))
}

func TestSortByOrdering(t *testing.T) {
	tests := []struct {
		name     string
		columns  []mockOrderColumn
		ordering OrderKeeper
		expected []string
	}{
		{
			name: "sorts columns by ordering",
			columns: []mockOrderColumn{
				{id: "test/third", name: "third"},
				{id: "test/first", name: "first"},
				{id: "test/second", name: "second"},
			},
			ordering: func() *InterfaceOrdering {
				o := NewInterfaceDefinitionOrderKeeper()
				o.order["test/first"] = 0
				o.order["test/second"] = 1
				o.order["test/third"] = 2
				return o
			}(),
			expected: []string{"first", "second", "third"},
		},
		{
			name: "unknown interfaces sort last",
			columns: []mockOrderColumn{
				{id: "test/unknown", name: "unknown"},
				{id: "test/first", name: "first"},
			},
			ordering: func() *InterfaceOrdering {
				o := NewInterfaceDefinitionOrderKeeper()
				o.order["test/first"] = 0
				return o
			}(),
			expected: []string{"first", "unknown"},
		},
		{
			name: "nil ordering returns copy",
			columns: []mockOrderColumn{
				{id: "test/a", name: "a"},
				{id: "test/b", name: "b"},
			},
			ordering: nil,
			expected: []string{"a", "b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given + when
			sorted := SortByOrdering(tt.columns, tt.ordering)

			// then
			assert.Equal(t, len(tt.expected), len(sorted))
			for i, col := range sorted {
				assert.Equal(t, tt.expected[i], col.name)
			}
		})
	}
}

// mockOrderColumn is a test implementation of Column
type mockOrderColumn struct {
	id   InterfaceID
	name string
}

func (m mockOrderColumn) Docs() Documentation {
	return Documentation{}
}

func (m mockOrderColumn) Implements() Interface {
	return Interface{
		ID:    m.id,
		Field: &arrow.Field{Name: m.name, Type: arrow.BinaryTypes.String},
	}
}

func (m mockOrderColumn) DependsOn() []DependsOnEntry {
	return nil
}
