package matomo

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatomoEventParamsColumns(t *testing.T) {
	proto := NewMatomoProtocol(&staticPropertyIDExtractor{propertyID: "test_property_id"})

	buildPageViewHit := func(_ *testing.T) *hits.Hit {
		hit := testHitOne()
		hit.EventName = protocol.PageViewEventType
		return hit
	}

	type testCase struct {
		name      string
		cfg       []columntests.CaseConfigFunc
		fieldName string
		expected  any
	}

	testCases := []testCase{
		{
			name:      "EventSiteID_Valid",
			cfg:       []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "idsite", "42")},
			fieldName: "params_site_id",
			expected:  "42",
		},
		{
			name:      "EventSiteID_Empty",
			cfg:       []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "idsite", "")},
			fieldName: "params_site_id",
			expected:  nil,
		},
		{
			name:      "EventSiteID_Absent",
			cfg:       []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "idsite", "")},
			fieldName: "params_site_id",
			expected:  nil,
		},
		{
			name:      "EventParamsPageViewID_Valid",
			cfg:       []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "pv_id", "abc123")},
			fieldName: "params_page_view_id",
			expected:  "abc123",
		},
		{
			name:      "EventParamsPageViewID_Empty",
			cfg:       []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "pv_id", "")},
			fieldName: "params_page_view_id",
			expected:  nil,
		},
		{name: "EventParamsPageViewID_Absent", fieldName: "params_page_view_id", expected: nil},
		{
			name:      "EventParamsGoalID_Valid",
			cfg:       []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "idgoal", "7")},
			fieldName: "params_goal_id",
			expected:  "7",
		},
		{
			name:      "EventParamsGoalID_Empty",
			cfg:       []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "idgoal", "")},
			fieldName: "params_goal_id",
			expected:  nil,
		},
		{name: "EventParamsGoalID_Absent", fieldName: "params_goal_id", expected: nil},
		{
			name:      "EventParamsCategory_Valid",
			cfg:       []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "e_c", "checkout")},
			fieldName: "params_category",
			expected:  "checkout",
		},
		{name: "EventParamsCategory_Absent", fieldName: "params_category", expected: nil},
		{
			name:      "EventParamsAction_Valid",
			cfg:       []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "e_a", "add_to_cart")},
			fieldName: "params_action",
			expected:  "add_to_cart",
		},
		{name: "EventParamsAction_Absent", fieldName: "params_action", expected: nil},
		{
			name:      "EventParamsValue_ValidNumeric",
			cfg:       []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "e_v", "99.95")},
			fieldName: "params_value",
			expected:  99.95,
		},
		{
			name:      "EventParamsValue_ValidInteger",
			cfg:       []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "e_v", "42")},
			fieldName: "params_value",
			expected:  42.0,
		},
		{
			name:      "EventParamsValue_NonNumeric",
			cfg:       []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "e_v", "not_a_number")},
			fieldName: "params_value",
			expected:  nil,
		},
		{
			name:      "EventParamsValue_Empty",
			cfg:       []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "e_v", "")},
			fieldName: "params_value",
			expected:  nil,
		},
		{name: "EventParamsValue_Absent", fieldName: "params_value", expected: nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			columntests.ColumnTestCase(
				t,
				columntests.TestHits{buildPageViewHit(t)},
				func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
					require.NoError(t, closeErr)
					require.NotEmpty(t, whd.WriteCalls)
					record := whd.WriteCalls[0].Records[0]
					assert.Equal(t, tc.expected, record[tc.fieldName])
				},
				proto,
				append(tc.cfg, columntests.EnsureQueryParam(0, "v", "2"))...,
			)
		})
	}
}
