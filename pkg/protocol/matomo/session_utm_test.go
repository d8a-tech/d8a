//nolint:funlen,lll // test code
package matomo

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatomoSessionUTMColumns(t *testing.T) {
	proto := NewMatomoProtocol(&staticPropertyIDExtractor{propertyID: "test_property_id"})

	buildPageViewHit := func(_ *testing.T) *hits.Hit {
		hit := columntests.TestHitOne()
		hit.EventName = "page_view"
		return hit
	}

	buildNonPageViewHit := func(_ *testing.T) *hits.Hit {
		hit := columntests.TestHitOne()
		hit.EventName = "scroll"
		return hit
	}

	type testCase struct {
		name        string
		buildHits   func(t *testing.T) columntests.TestHits
		cfg         []columntests.CaseConfigFunc
		fieldName   string
		expected    any
		description string
	}

	utmCases := func(fieldName, utmParam, utmValue string) []testCase {
		return []testCase{
			{
				name: fieldName + "_PageViewWithValue",
				buildHits: func(t *testing.T) columntests.TestHits {
					return columntests.TestHits{buildPageViewHit(t)}
				},
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "url", "https://example.com/?"+utmParam+"="+utmValue)},
				fieldName:   fieldName,
				expected:    utmValue,
				description: "Returns UTM value from the first page view event",
			},
			{
				name: fieldName + "_SecondHitIsPageViewWithValue",
				buildHits: func(t *testing.T) columntests.TestHits {
					return columntests.TestHits{buildNonPageViewHit(t), buildPageViewHit(t)}
				},
				cfg: []columntests.CaseConfigFunc{
					columntests.EnsureQueryParam(1, "url", "https://example.com/?"+utmParam+"="+utmValue),
					columntests.EnsureQueryParam(1, "v", "2"),
				},
				fieldName:   fieldName,
				expected:    utmValue,
				description: "Returns UTM value from the first page view even when preceded by a non-page-view event",
			},
			{
				name: fieldName + "_NoPageViewEvents",
				buildHits: func(t *testing.T) columntests.TestHits {
					return columntests.TestHits{buildNonPageViewHit(t)}
				},
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "url", "https://example.com/?"+utmParam+"="+utmValue)},
				fieldName:   fieldName,
				expected:    nil,
				description: "Returns nil when there are no page view events in the session",
			},
			{
				name: fieldName + "_ParamAbsent",
				buildHits: func(t *testing.T) columntests.TestHits {
					return columntests.TestHits{buildPageViewHit(t)}
				},
				cfg:         nil,
				fieldName:   fieldName,
				expected:    nil,
				description: "Returns nil when the UTM param is absent from the page URL",
			},
		}
	}

	mergeCases := func(groups ...[]testCase) []testCase {
		total := 0
		for _, g := range groups {
			total += len(g)
		}
		out := make([]testCase, 0, total)
		for _, g := range groups {
			out = append(out, g...)
		}
		return out
	}

	testCases := mergeCases(
		utmCases("session_utm_campaign", "utm_campaign", "summer_sale"),
		utmCases("session_utm_source", "utm_source", "google"),
		utmCases("session_utm_medium", "utm_medium", "cpc"),
		utmCases("session_utm_content", "utm_content", "banner_v2"),
		utmCases("session_utm_term", "utm_term", "running_shoes"),
		utmCases("session_utm_id", "utm_id", "abc123"),
		utmCases("session_utm_source_platform", "utm_source_platform", "Search Ads 360"),
		utmCases("session_utm_creative_format", "utm_creative_format", "display"),
		utmCases("session_utm_marketing_tactic", "utm_marketing_tactic", "remarketing"),
	)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			thits := tc.buildHits(t)

			columntests.ColumnTestCase(t, thits,
				func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
					// when + then
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
