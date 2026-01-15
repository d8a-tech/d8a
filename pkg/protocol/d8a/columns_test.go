package d8a

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventColumns(t *testing.T) {
	// Test cases for event columns with different values
	var eventColumnTestCases = []struct {
		name        string
		param       string
		value       string
		expected    any
		expectedErr bool
		fieldName   string
		description string
		hits        columntests.TestHits
	}{

		{
			name:        "TrackingProtocol_D8A",
			expected:    "d8a",
			fieldName:   "tracking_protocol",
			description: "Tracking protocol should be d8a",
		},
	}

	for _, tc := range eventColumnTestCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			hits := tc.hits
			if hits == nil {
				hits = columntests.TestHits{columntests.TestHitOne()}
			}
			columntests.ColumnTestCase(
				t,
				hits,
				func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
					// when + then
					if tc.expectedErr {
						assert.Error(t, closeErr)
					} else {
						require.NoError(t, closeErr)
						record := whd.WriteCalls[0].Records[0]
						assert.Equal(t, tc.expected, record[tc.fieldName], tc.description)
					}
				},
				NewD8AProtocol(currency.NewDummyConverter(1), properties.NewTestSettingRegistry()),
				columntests.EnsureQueryParam(0, tc.param, tc.value))
		})
	}
}

func TestSessionColumns(t *testing.T) {
	// Test cases for session columns
	var sessionColumnTestCases = []struct {
		name            string
		expected        any
		expectedErr     bool
		fieldName       string
		description     string
		hits            columntests.TestHits
		caseConfigFuncs []columntests.CaseConfigFunc
	}{}

	for _, tc := range sessionColumnTestCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			hits := tc.hits
			columntests.ColumnTestCase(
				t,
				hits,
				func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
					// when + then
					if tc.expectedErr {
						assert.Error(t, closeErr)
					} else {
						require.NoError(t, closeErr)
						record := whd.WriteCalls[0].Records[0]
						assert.Equal(t, tc.expected, record[tc.fieldName], tc.description)
					}
				},
				NewD8AProtocol(currency.NewDummyConverter(1), properties.NewTestSettingRegistry()),
				tc.caseConfigFuncs...)
		})
	}
}

type mockDependsOnSessionColumn struct {
	id   schema.InterfaceID
	deps []schema.DependsOnEntry
}

func (c *mockDependsOnSessionColumn) Docs() schema.Documentation { return schema.Documentation{} }
func (c *mockDependsOnSessionColumn) Implements() schema.Interface {
	return schema.Interface{ID: c.id}
}
func (c *mockDependsOnSessionColumn) DependsOn() []schema.DependsOnEntry { return c.deps }
func (c *mockDependsOnSessionColumn) Write(*schema.Session) schema.D8AColumnWriteError {
	return nil
}

func TestWrapColumns_PatchesDependsOnInterfaceIDs(t *testing.T) {
	// given
	child := &mockDependsOnSessionColumn{
		id: "ga4.example/col_a",
		deps: []schema.DependsOnEntry{
			{Interface: "ga4.example/col_b"},
		},
	}
	wrapped := (&d8aSessionColumnWrapper{column: child})

	// when
	gotImplements := wrapped.Implements()
	gotDeps := wrapped.DependsOn()

	// then
	assert.Equal(t, schema.InterfaceID("d8a.example/col_a"), gotImplements.ID)
	require.Len(t, gotDeps, 1)
	assert.Equal(t, schema.InterfaceID("d8a.example/col_b"), gotDeps[0].Interface)
}
