package matomo

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatomoNavigationColumns(t *testing.T) {
	proto := NewMatomoProtocol(&staticPropertyIDExtractor{propertyID: "test_property_id"})

	buildPageViewHit := func(_ *testing.T) *hits.Hit {
		hit := testHitOne()
		hit.EventName = pageViewEventType
		return hit
	}

	single := func(build func(*testing.T) *hits.Hit) func(*testing.T) columntests.TestHits {
		return func(t *testing.T) columntests.TestHits {
			return columntests.TestHits{build(t)}
		}
	}

	type testCase struct {
		name        string
		cfg         []columntests.CaseConfigFunc
		fieldName   string
		expected    any
		description string
	}

	testCases := []testCase{
		{
			name:        "EventLinkURL_Valid",
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "link", "https://example.com/page")},
			fieldName:   "params_link_url",
			expected:    "https://example.com/page",
			description: "Valid link URL via link query parameter",
		},
		{
			name:        "EventLinkURL_Empty",
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "link", "")},
			fieldName:   "params_link_url",
			expected:    nil,
			description: "Returns nil when link parameter is empty",
		},
		{
			name:        "EventLinkURL_Absent",
			fieldName:   "params_link_url",
			expected:    nil,
			description: "Returns nil when link parameter is absent",
		},
		{
			name: "EventDownloadURL_Valid",
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "download", "https://example.com/file.zip"),
			},
			fieldName:   "params_download_url",
			expected:    "https://example.com/file.zip",
			description: "Valid download URL via download query parameter",
		},
		{
			name:        "EventDownloadURL_Empty",
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "download", "")},
			fieldName:   "params_download_url",
			expected:    nil,
			description: "Returns nil when download parameter is empty",
		},
		{
			name:        "EventDownloadURL_Absent",
			fieldName:   "params_download_url",
			expected:    nil,
			description: "Returns nil when download parameter is absent",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			columntests.ColumnTestCase(
				t,
				single(buildPageViewHit)(t),
				func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
					require.NoError(t, closeErr)
					require.NotEmpty(t, whd.WriteCalls)
					record := whd.WriteCalls[0].Records[0]
					assert.Equal(t, tc.expected, record[tc.fieldName], tc.description)
				},
				proto,
				append(tc.cfg, columntests.EnsureQueryParam(0, "v", "2"))...,
			)
		})
	}
}
