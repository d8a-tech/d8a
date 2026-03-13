package matomo

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nolint:funlen,lll // test code
func TestMatomoMediaColumns(t *testing.T) {
	proto := NewMatomoProtocol(&staticPropertyIDExtractor{propertyID: "test_property_id"})

	buildPageViewHit := func(_ *testing.T) *hits.Hit {
		hit := columntests.TestHitOne()
		hit.EventName = "page_view"
		return hit
	}

	single := func(build func(*testing.T) *hits.Hit) func(*testing.T) columntests.TestHits {
		return func(t *testing.T) columntests.TestHits {
			return columntests.TestHits{build(t)}
		}
	}

	type testCase struct {
		name        string
		buildHits   func(t *testing.T) columntests.TestHits
		cfg         []columntests.CaseConfigFunc
		fieldName   string
		expected    any
		expectNoIO  bool
		description string
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

		[]testCase{
			{
				name:        "EventParamsMediaAssetID_Valid",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ma_id", "video_123")},
				fieldName:   "params_media_asset_id",
				expected:    "video_123",
				description: "Valid media asset ID via ma_id query parameter",
			},
			{
				name:        "EventParamsMediaAssetID_Empty",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ma_id", "")},
				fieldName:   "params_media_asset_id",
				expected:    nil,
				description: "Returns nil when ma_id parameter is empty",
			},
			{
				name:        "EventParamsMediaAssetID_Absent",
				buildHits:   single(buildPageViewHit),
				fieldName:   "params_media_asset_id",
				expected:    nil,
				description: "Returns nil when ma_id parameter is absent",
			},
			{
				name:        "EventParamsMediaType_Valid",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ma_mt", "video")},
				fieldName:   "params_media_type",
				expected:    "video",
				description: "Valid media type via ma_mt query parameter",
			},
			{
				name:        "EventParamsMediaType_Empty",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ma_mt", "")},
				fieldName:   "params_media_type",
				expected:    nil,
				description: "Returns nil when ma_mt parameter is empty",
			},
			{
				name:        "EventParamsMediaType_Absent",
				buildHits:   single(buildPageViewHit),
				fieldName:   "params_media_type",
				expected:    nil,
				description: "Returns nil when ma_mt parameter is absent",
			},
		},
	)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			columntests.ColumnTestCase(
				t,
				tc.buildHits(t),
				func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
					// when + then
					require.NoError(t, closeErr)
					if tc.expectNoIO {
						require.Empty(t, whd.WriteCalls, "expected no warehouse write calls")
						return
					}
					require.NotEmpty(t, whd.WriteCalls, "expected at least one warehouse write call")
					require.NotEmpty(t, whd.WriteCalls[0].Records, "expected at least one record written")
					record := whd.WriteCalls[0].Records[0]
					assert.Equal(t, tc.expected, record[tc.fieldName], tc.description)
				},
				proto,
				append(tc.cfg, columntests.EnsureQueryParam(0, "v", "2"))...,
			)
		})
	}
}
