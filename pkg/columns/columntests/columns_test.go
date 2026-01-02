// package columntests contains tests for the columns package
package columntests

import (
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol/ga4"
	"github.com/d8a-tech/d8a/pkg/splitter"
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
	}{
		// Required fields
		{
			name:        "EventName_Valid",
			param:       "en",
			value:       "page_view",
			expected:    "page_view",
			fieldName:   "name",
			description: "Required event name field",
		},
		{
			name:        "EventUtmMarketingTactic_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_marketing_tactic=1337",
			expected:    "1337",
			fieldName:   "utm_marketing_tactic",
			description: "Valid UTM marketing tactic",
		},
		{
			name:        "EventUtmMarketingTactic_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_marketing_tactic",
			description: "Empty UTM marketing tactic",
		},
		{
			name:        "EventUtmSourcePlatform_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_source_platform=1337",
			expected:    "1337",
			fieldName:   "utm_source_platform",
			description: "Valid UTM source platform",
		},
		{
			name:        "EventUtmSourcePlatform_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_source_platform",
			description: "Empty UTM source platform",
		},
		{
			name:        "EventUtmTerm_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_term=1337",
			expected:    "1337",
			fieldName:   "utm_term",
			description: "Valid UTM term",
		},
		{
			name:        "EventUtmTerm_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_term",
			description: "Empty UTM term",
		},
		{
			name:        "EventUtmContent_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_content=1337",
			expected:    "1337",
			fieldName:   "utm_content",
			description: "Valid UTM content",
		},
		{
			name:        "EventUtmContent_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_content",
			description: "Empty UTM content",
		},
		{
			name:        "EventUtmSource_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_source=1337",
			expected:    "1337",
			fieldName:   "utm_source",
			description: "Valid UTM source",
		},
		{
			name:        "EventUtmSource_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_source",
			description: "Empty UTM source",
		},
		{
			name:        "EventUtmMedium_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_medium=1337",
			expected:    "1337",
			fieldName:   "utm_medium",
			description: "Valid UTM medium",
		},
		{
			name:        "EventUtmMedium_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_medium",
			description: "Empty UTM medium",
		},
		{
			name:        "EventUtmCampaign_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_campaign=1337",
			expected:    "1337",
			fieldName:   "utm_campaign",
			description: "Valid UTM campaign",
		},
		{
			name:        "EventUtmCampaign_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_campaign",
			description: "Empty UTM campaign",
		},
		{
			name:        "EventUtmId_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_id=1337",
			expected:    "1337",
			fieldName:   "utm_id",
			description: "Valid UTM ID",
		},
		{
			name:        "EventUtmId_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_id",
			description: "Empty UTM ID",
		},
		{
			name:        "EventUtmCreativeFormat_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_creative_format=1337",
			expected:    "1337",
			fieldName:   "utm_creative_format",
			description: "Valid UTM creative format",
		},
		{
			name:        "EventUtmCreativeFormat_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_creative_format",
			description: "Empty UTM creative format",
		},

		{
			name:        "ClickIDsGclid_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&gclid=1337",
			expected:    "1337",
			fieldName:   "click_id_gclid",
			description: "Valid click id gclid",
		},
		{
			name:        "ClickIDsGclid_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "click_id_gclid",
			description: "Empty click id gclid should be nil",
		},
		{
			name:        "ClickIDsDclid_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&dclid=1337",
			expected:    "1337",
			fieldName:   "click_id_dclid",
			description: "Valid click id dclid",
		},
		{
			name:        "ClickIDsDclid_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "click_id_dclid",
			description: "Empty click id dclid should be nil",
		},
		{
			name:        "ClickIDsSrsltid_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&srsltid=1337",
			expected:    "1337",
			fieldName:   "click_id_srsltid",
			description: "Valid click id srsltid",
		},
		{
			name:        "ClickIDsSrsltid_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "click_id_srsltid",
			description: "Empty click id srsltid should be nil",
		},
		{
			name:        "ClickIDsGbraid_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&gbraid=1337",
			expected:    "1337",
			fieldName:   "click_id_gbraid",
			description: "Valid click id gbraid",
		},
		{
			name:        "ClickIDsGbraid_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "click_id_gbraid",
			description: "Empty click id gbraid should be nil",
		},
		{
			name:        "ClickIDsWbraid_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&wbraid=1337",
			expected:    "1337",
			fieldName:   "click_id_wbraid",
			description: "Valid click id wbraid",
		},
		{
			name:        "ClickIDsWbraid_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "click_id_wbraid",
			description: "Empty click id wbraid should be nil",
		},
		{
			name:        "ClickIDsFbclid_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&fbclid=1337",
			expected:    "1337",
			fieldName:   "click_id_fbclid",
			description: "Valid click id fbclid",
		},
		{
			name:        "ClickIDsFbclid_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "click_id_fbclid",
			description: "Empty click id fbclid should be nil",
		},
		{
			name:        "ClickIDsMsclkid_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&msclkid=1337",
			expected:    "1337",
			fieldName:   "click_id_msclkid",
			description: "Valid click id msclkid",
		},
		{
			name:        "ClickIDsMsclkid_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "click_id_msclkid",
			description: "Empty click id msclkid should be nil",
		},
	}

	for _, tc := range eventColumnTestCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			ColumnTestCase(
				t,
				TestHits{TestHitOne()},
				func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
					// when + then
					if tc.expectedErr {
						assert.Error(t, closeErr)
					} else {
						require.NoError(t, closeErr)
						require.Len(t, whd.WriteCalls, 1)
						require.Len(t, whd.WriteCalls[0].Records, 1)
						record := whd.WriteCalls[0].Records[0]
						assert.Equal(t, tc.expected, record[tc.fieldName], tc.description)
					}
				},
				ga4.NewGA4Protocol(
					currency.NewDummyConverter(1),
					properties.NewTestSettingRegistry()),
				EnsureQueryParam(0, tc.param, tc.value))
		})
	}
}

func TestSessionHitNumber(t *testing.T) {
	ColumnTestCase(
		t,
		TestHits{TestHitOne(), TestHitTwo(), TestHitThree()},
		func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
			// when + then
			require.NoError(t, closeErr)
			for i, record := range whd.WriteCalls[0].Records {
				assert.Equal(t, int64(i), record["session_hit_number"])
			}
		},
		ga4.NewGA4Protocol(currency.NewDummyConverter(1), properties.NewTestSettingRegistry()),
	)
}

func TestSessionPageNumber(t *testing.T) {
	thThree := TestHitThree()
	thThree.MustParsedRequest().QueryParams.Set("dl",
		"https%3A%2F%2Fd8a-tech.github.io%2Fanalytics-playground%2Ffoobar.html")
	ColumnTestCase(
		t,
		TestHits{TestHitOne(), TestHitTwo(), thThree},
		func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
			// when + then
			require.NoError(t, closeErr)
			assert.Equal(t, int64(0), whd.WriteCalls[0].Records[0]["session_page_number"])
			assert.Equal(t, int64(0), whd.WriteCalls[0].Records[1]["session_page_number"])
			assert.Equal(t, int64(1), whd.WriteCalls[0].Records[2]["session_page_number"])
		},
		ga4.NewGA4Protocol(currency.NewDummyConverter(1), properties.NewTestSettingRegistry()),
	)
}

func TestSessionIsEntry(t *testing.T) {
	// given
	ColumnTestCase(
		t,
		TestHits{TestHitOne(), TestHitTwo(), TestHitThree()},
		func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
			// when + then
			require.NoError(t, closeErr)
			assert.Equal(t, int64(1), whd.WriteCalls[0].Records[0]["session_is_entry"])
			assert.Equal(t, int64(0), whd.WriteCalls[0].Records[1]["session_is_entry"])
			assert.Equal(t, int64(0), whd.WriteCalls[0].Records[2]["session_is_entry"])
		},
		ga4.NewGA4Protocol(currency.NewDummyConverter(1), properties.NewTestSettingRegistry()),
	)
}

func TestSessionFirstEventTime(t *testing.T) {
	// given
	th1 := TestHitOne()
	th2 := TestHitTwo()
	th3 := TestHitThree()
	baseTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	th1.MustParsedRequest().ServerReceivedTime = baseTime
	th2.MustParsedRequest().ServerReceivedTime = baseTime.Add(5 * time.Second)
	th3.MustParsedRequest().ServerReceivedTime = baseTime.Add(10 * time.Second)

	ColumnTestCase(
		t,
		TestHits{th1, th2, th3},
		func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
			// when + then
			require.NoError(t, closeErr)
			expectedFirstTime := baseTime.Unix()
			assert.Equal(t, expectedFirstTime, whd.WriteCalls[0].Records[0]["session_first_event_time"])
			assert.Equal(t, expectedFirstTime, whd.WriteCalls[0].Records[1]["session_first_event_time"])
			assert.Equal(t, expectedFirstTime, whd.WriteCalls[0].Records[2]["session_first_event_time"])
		},
		ga4.NewGA4Protocol(currency.NewDummyConverter(1), properties.NewTestSettingRegistry()),
	)
}

func TestSessionLastEventTime(t *testing.T) {
	// given
	th1 := TestHitOne()
	th2 := TestHitTwo()
	th3 := TestHitThree()
	baseTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	th1.MustParsedRequest().ServerReceivedTime = baseTime
	th2.MustParsedRequest().ServerReceivedTime = baseTime.Add(5 * time.Second)
	th3.MustParsedRequest().ServerReceivedTime = baseTime.Add(10 * time.Second)

	ColumnTestCase(
		t,
		TestHits{th1, th2, th3},
		func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
			// when + then
			require.NoError(t, closeErr)
			expectedLastTime := baseTime.Add(10 * time.Second).Unix()
			assert.Equal(t, expectedLastTime, whd.WriteCalls[0].Records[0]["session_last_event_time"])
			assert.Equal(t, expectedLastTime, whd.WriteCalls[0].Records[1]["session_last_event_time"])
			assert.Equal(t, expectedLastTime, whd.WriteCalls[0].Records[2]["session_last_event_time"])
		},
		ga4.NewGA4Protocol(currency.NewDummyConverter(1), properties.NewTestSettingRegistry()),
	)
}

func TestSessionTotalEvents(t *testing.T) {
	ColumnTestCase(
		t,
		TestHits{TestHitOne(), TestHitTwo(), TestHitThree()},
		func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
			// when + then
			require.NoError(t, closeErr)
			assert.Equal(t, 3, whd.WriteCalls[0].Records[0]["session_total_events"])
			assert.Equal(t, 3, whd.WriteCalls[0].Records[1]["session_total_events"])
			assert.Equal(t, 3, whd.WriteCalls[0].Records[2]["session_total_events"])
		},
		ga4.NewGA4Protocol(currency.NewDummyConverter(1), properties.NewTestSettingRegistry()),
	)
}

func TestSessionReferrer(t *testing.T) {
	ColumnTestCase(
		t,
		TestHits{TestHitOne(), TestHitTwo(), TestHitThree()},
		func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
			// when + then
			require.NoError(t, closeErr)

			assert.Equal(t, "https://example.com", whd.WriteCalls[0].Records[0]["session_referrer"])
			assert.Equal(t, "https://example.com", whd.WriteCalls[0].Records[1]["session_referrer"])
			assert.Equal(t, "https://example.com", whd.WriteCalls[0].Records[2]["session_referrer"])
		},
		ga4.NewGA4Protocol(currency.NewDummyConverter(1), properties.NewTestSettingRegistry()),
	)
}

func TestSessionSplitCause(t *testing.T) {
	ColumnTestCase(
		t,
		TestHits{TestHitOne(), TestHitTwo(), TestHitThree(), TestHitFour()},
		func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
			// when + then
			require.NoError(t, closeErr)

			// First and second events are in first session, no split cause
			assert.Equal(t, TestHitOne().ID, whd.WriteCalls[0].Records[0]["session_id"])
			assert.Equal(t, nil, whd.WriteCalls[0].Records[0]["session_split_cause"])
			assert.Equal(t, TestHitOne().ID, whd.WriteCalls[0].Records[1]["session_id"])
			assert.Equal(t, nil, whd.WriteCalls[0].Records[1]["session_split_cause"])

			// Third and fourth event is in second session, max_events_reached split cause
			assert.Equal(t, TestHitThree().ID, whd.WriteCalls[0].Records[2]["session_id"])
			assert.Equal(t, "max_events_reached", whd.WriteCalls[0].Records[2]["session_split_cause"])
			assert.Equal(t, TestHitThree().ID, whd.WriteCalls[0].Records[3]["session_id"])
			assert.Equal(t, "max_events_reached", whd.WriteCalls[0].Records[3]["session_split_cause"])
		},
		ga4.NewGA4Protocol(currency.NewDummyConverter(1), properties.NewTestSettingRegistry()),
		SetSplitterRegistry(splitter.NewStaticRegistry(
			splitter.New(
				splitter.NewMaxXEventsCondition(2),
			),
		)),
	)
}

func TestSessionSourceMediumTerm(t *testing.T) {
	// syntax sugar for creating a pointer to a string
	var s = func(s string) *string {
		return &s
	}

	// given
	var testCases = []struct {
		name            string
		hits            TestHits
		caseConfigFuncs []CaseConfigFunc
		expected        map[string][]*string
	}{
		{
			name: "SessionSourceMediumTerm_PipeUtmTags",
			hits: TestHits{TestHitOne()},
			caseConfigFuncs: []CaseConfigFunc{
				EnsureQueryParam(0, "dl", "https://example.com/page?utm_source=google&utm_medium=cpc&utm_term=keyword"),
			},
			expected: map[string][]*string{
				TestHitOne().ID: {
					s("google"),
					s("cpc"),
					s("keyword"),
				},
			},
		},
		{
			name: "SessionSourceMediumTerm_PageLocationParamsGclid",
			hits: TestHits{TestHitOne()},
			caseConfigFuncs: []CaseConfigFunc{
				EnsureQueryParam(0, "dl", "https://example.com/page?gclid=1234567890"),
			},
			expected: map[string][]*string{
				TestHitOne().ID: {
					s("google"),
					s("cpc"),
					s(""),
				},
			},
		},
		{
			name: "SessionSourceMediumTerm_SearchEngine",
			hits: TestHits{TestHitOne()},
			caseConfigFuncs: []CaseConfigFunc{
				EnsureHeader(0, "Referer", "https://google.com/search?q=keyword"),
			},
			expected: map[string][]*string{
				TestHitOne().ID: {
					s("google"),
					s("organic"),
					s("keyword"),
				},
			},
		},
		{
			name: "SessionSourceMediumTerm_GoogleWWW",
			hits: TestHits{TestHitOne()},
			caseConfigFuncs: []CaseConfigFunc{
				EnsureHeader(0, "Referer", "https://www.google.com/search?q=keyword"),
			},
			expected: map[string][]*string{
				TestHitOne().ID: {
					s("google"),
					s("organic"),
					s("keyword"),
				},
			},
		},
		{
			name: "SessionSourceMediumTerm_IlseNL",
			hits: TestHits{TestHitOne()},
			caseConfigFuncs: []CaseConfigFunc{
				EnsureHeader(0, "Referer", "https://www.ilse.nl/search?search_for=keyword"),
			},
			expected: map[string][]*string{
				TestHitOne().ID: {
					s("ilse-nl"),
					s("organic"),
					s("keyword"),
				},
			},
		},
		{
			name: "SessionSourceMediumTerm_SearchEngine_RegexMatcher",
			hits: TestHits{TestHitOne()},
			caseConfigFuncs: []CaseConfigFunc{
				EnsureHeader(0, "Referer", "https://google.gr/search?q=keyword"),
			},
			expected: map[string][]*string{
				TestHitOne().ID: {
					s("google"),
					s("organic"),
					s("keyword"),
				},
			},
		},
		{
			name: "SessionSourceMediumTerm_NonGoogle",
			hits: TestHits{TestHitOne()},
			caseConfigFuncs: []CaseConfigFunc{
				EnsureHeader(0, "Referer", "https://www.baidu.com/s?wd=keyword"),
			},
			expected: map[string][]*string{
				TestHitOne().ID: {
					s("baidu"),
					s("organic"),
					s("keyword"),
				},
			},
		},
		{
			name: "SessionSourceMediumTerm_Facebook",
			hits: TestHits{TestHitOne()},
			caseConfigFuncs: []CaseConfigFunc{
				EnsureHeader(0, "Referer", "https://facebook.com/post/123"),
			},
			expected: map[string][]*string{
				TestHitOne().ID: {
					s("facebook"),
					s("social"),
					s(""),
				},
			},
		},
		{
			name: "SessionSourceMediumTerm_Twitter",
			hits: TestHits{TestHitOne()},
			caseConfigFuncs: []CaseConfigFunc{
				EnsureHeader(0, "Referer", "https://twitter.com/user/status/123"),
			},
			expected: map[string][]*string{
				TestHitOne().ID: {
					s("twitter"),
					s("social"),
					s(""),
				},
			},
		},
		{
			name: "SessionSourceMediumTerm_ChatGPT",
			hits: TestHits{TestHitOne()},
			caseConfigFuncs: []CaseConfigFunc{
				EnsureHeader(0, "Referer", "https://chatgpt.com/chat"),
			},
			expected: map[string][]*string{
				TestHitOne().ID: {
					s("chatgpt"),
					s("ai"),
					s(""),
				},
			},
		},
		{
			name: "SessionSourceMediumTerm_Gemini",
			hits: TestHits{TestHitOne()},
			caseConfigFuncs: []CaseConfigFunc{
				EnsureHeader(0, "Referer", "https://gemini.google.com/app"),
			},
			expected: map[string][]*string{
				TestHitOne().ID: {
					s("gemini"),
					s("ai"),
					s(""),
				},
			},
		},
		{
			name: "SessionSourceMediumTerm_YouTube",
			hits: TestHits{TestHitOne()},
			caseConfigFuncs: []CaseConfigFunc{
				EnsureHeader(0, "Referer", "https://youtube.com/watch?v=123"),
			},
			expected: map[string][]*string{
				TestHitOne().ID: {
					s("youtube"),
					s("video"),
					s(""),
				},
			},
		},
		{
			name: "SessionSourceMediumTerm_MessyPlatformNamNormalized",
			hits: TestHits{TestHitOne()},
			caseConfigFuncs: []CaseConfigFunc{
				EnsureHeader(0, "Referer", "https://videa.seznam.cz/foobar"),
			},
			expected: map[string][]*string{
				TestHitOne().ID: {
					s("seznam-videa"),
					s("organic"),
					s(""),
				},
			},
		},
		{
			name: "SessionSourceMediumTerm_YouTubeOverridenByUtmTags",
			hits: TestHits{TestHitOne()},
			caseConfigFuncs: []CaseConfigFunc{
				// UTM tags always have precedence over other detections
				EnsureQueryParam(0, "dl", "https://example.com/page?utm_source=foobar&utm_medium=bar&utm_term=keyword"),
				EnsureHeader(0, "Referer", "https://youtube.com/watch?v=123"),
			},
			expected: map[string][]*string{
				TestHitOne().ID: {
					s("foobar"),
					s("bar"),
					s("keyword"),
				},
			},
		},
		{
			name: "SessionSourceMediumTerm_Vimeo",
			hits: TestHits{TestHitOne()},
			caseConfigFuncs: []CaseConfigFunc{
				EnsureHeader(0, "Referer", "https://vimeo.com/123456"),
			},
			expected: map[string][]*string{
				TestHitOne().ID: {
					s("vimeo"),
					s("video"),
					s(""),
				},
			},
		},
		{
			name: "SessionSourceMediumTerm_Gmail",
			hits: TestHits{TestHitOne()},
			caseConfigFuncs: []CaseConfigFunc{
				EnsureHeader(0, "Referer", "https://gmail.com"),
			},
			expected: map[string][]*string{
				TestHitOne().ID: {
					s("gmail"),
					s("email"),
					s(""),
				},
			},
		},
		{
			name: "SessionSourceMediumTerm_MailReferer",
			hits: TestHits{TestHitOne()},
			caseConfigFuncs: []CaseConfigFunc{
				EnsureHeader(0, "Referer", "https://www.mail.example.com/path?query=value"),
			},
			expected: map[string][]*string{
				TestHitOne().ID: {
					s("mail.example.com"),
					s("email"),
					s(""),
				},
			},
		},
		{
			name: "SessionSourceMediumTerm_GenericReferral",
			hits: TestHits{TestHitOne()},
			caseConfigFuncs: []CaseConfigFunc{
				EnsureQueryParam(0, "dl", "https://www.example.com/page"),
				EnsureHeader(0, "Referer", "https://www.other-site.com/blog/article?id=123"),
			},
			expected: map[string][]*string{
				TestHitOne().ID: {
					s("other-site.com"),
					s("referral"),
					s(""),
				},
			},
		},
		{
			name: "SessionSourceMediumTerm_GenericReferral_MatchesPageLocation",
			hits: TestHits{TestHitOne()},
			caseConfigFuncs: []CaseConfigFunc{
				EnsureQueryParam(0, "dl", "https://www.example.com/page"),
				EnsureHeader(0, "Referer", "https://www.example.com/blog/article?id=123"),
			},
			expected: map[string][]*string{
				TestHitOne().ID: {
					s("direct"),
					s("none"),
					s(""),
				},
			},
		},
		{
			name: "SessionSourceMediumTerm_Direct",
			hits: TestHits{TestHitOne()},
			caseConfigFuncs: []CaseConfigFunc{
				EnsureQueryParam(0, "dl", "https://example.com/page"),
			},
			expected: map[string][]*string{
				TestHitOne().ID: {
					s("direct"),
					s("none"),
					s(""),
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			ColumnTestCase(
				t,
				tc.hits,
				func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
					// when + then
					require.NoError(t, closeErr)
					require.Len(t, whd.WriteCalls, 1)

					actual := make(map[string][]*string)
					for _, record := range whd.WriteCalls[0].Records {
						sessionID, ok := record["session_id"].(string)
						require.True(t, ok, "session_id must be string")
						source := record["session_source"]
						medium := record["session_medium"]
						term := record["session_term"]

						var sourcePtr, mediumPtr, termPtr *string
						if source != nil {
							s, ok := source.(string)
							if ok {
								sourcePtr = &s
							}
						}
						if medium != nil {
							m, ok := medium.(string)
							if ok {
								mediumPtr = &m
							}
						}
						if term != nil {
							termStr, ok := term.(string)
							if ok {
								termPtr = &termStr
							}
						}

						actual[sessionID] = []*string{sourcePtr, mediumPtr, termPtr}
					}

					for sessionID, expectedValues := range tc.expected {
						actualValues, ok := actual[sessionID]
						require.True(t, ok, "session_id %s not found", sessionID)

						expectedSource := expectedValues[0]
						actualSource := actualValues[0]
						if expectedSource == nil {
							assert.Nil(t, actualSource, "session_source should be nil")
						} else {
							require.NotNil(t, actualSource, "session_source should not be nil")
							assert.Equal(t, *expectedSource, *actualSource, "session_source should match utm_source")
						}

						expectedMedium := expectedValues[1]
						actualMedium := actualValues[1]
						if expectedMedium == nil {
							assert.Nil(t, actualMedium, "session_medium should be nil")
						} else {
							require.NotNil(t, actualMedium, "session_medium should not be nil")
							assert.Equal(t, *expectedMedium, *actualMedium, "session_medium should match utm_medium")
						}
						expectedTerm := expectedValues[2]
						actualTerm := actualValues[2]
						if expectedTerm == nil {
							assert.Nil(t, actualTerm, "session_term should be nil")
						} else {
							require.NotNil(t, actualTerm, "session_term should not be nil")
							assert.Equal(t, *expectedTerm, *actualTerm, "session_term should match utm_term")
						}
					}
				},
				ga4.NewGA4Protocol(currency.NewDummyConverter(1), properties.NewTestSettingRegistry()),
				tc.caseConfigFuncs...,
			)
		})
	}
}
