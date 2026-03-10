package matomo

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatomoSessionReturningUserColumn(t *testing.T) {
	proto := NewMatomoProtocol(&staticPropertyIDExtractor{propertyID: "test_property_id"})

	type testCase struct {
		name        string
		hits        columntests.TestHits
		cfg         []columntests.CaseConfigFunc
		expected    any
		description string
	}

	testCases := []testCase{
		{
			name:        "FirstEventNewUser",
			hits:        columntests.TestHits{testHitOne()},
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "_idn", "1")},
			expected:    int64(0),
			description: "_idn=1 on the first event means new user and session_returning_user=0",
		},
		{
			name:        "FirstEventReturningUser",
			hits:        columntests.TestHits{testHitOne()},
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "_idn", "0")},
			expected:    int64(1),
			description: "_idn=0 on the first event means returning user and session_returning_user=1",
		},
		{
			name:        "MissingIdnDefaultsToNewUser",
			hits:        columntests.TestHits{testHitOne()},
			cfg:         nil,
			expected:    int64(0),
			description: "missing _idn defaults to session_returning_user=0",
		},
		{
			name: "FirstEventWinsWhenConflictingAcrossEvents",
			hits: columntests.TestHits{testHitOne(), testHitOne()},
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "_idn", "1"),
				columntests.EnsureQueryParam(1, "_idn", "0"),
			},
			expected:    int64(0),
			description: "session_returning_user is derived from first event when later events differ",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfgs := append(tc.cfg, columntests.EnsureQueryParam(0, "v", "2"))
			if len(tc.hits) > 1 {
				cfgs = append(cfgs, columntests.EnsureQueryParam(1, "v", "2"))
			}

			columntests.ColumnTestCase(
				t,
				tc.hits,
				func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
					// given + when + then
					require.NoError(t, closeErr)
					require.NotEmpty(t, whd.WriteCalls, "expected at least one warehouse write call")
					require.NotEmpty(t, whd.WriteCalls[0].Records, "expected at least one record")
					record := whd.WriteCalls[0].Records[0]
					assert.Equal(t, tc.expected, record["session_returning_user"], tc.description)
				},
				proto,
				cfgs...,
			)
		})
	}
}
