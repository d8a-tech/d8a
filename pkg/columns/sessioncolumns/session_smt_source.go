package sessioncolumns

import (
	_ "embed"
	"net/url"
	"regexp"
	"strings"

	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert/yaml"
)

var sessionSourceMediumTermDetector = NewCompositeSourceMediumTermDetector(
	NewPageLocationParamsSourceMediumTermDetector(
		IfHasQueryParam("gclid", SessionSourceMediumTerm{Source: "google", Medium: "cpc", Term: ""}),
		IfHasQueryParam("gbraid", SessionSourceMediumTerm{Source: "google", Medium: "cpc", Term: ""}),
		IfHasQueryParam("wbraid", SessionSourceMediumTerm{Source: "google", Medium: "cpc", Term: ""}),
		IfHasQueryParam("msclkid", SessionSourceMediumTerm{Source: "bing", Medium: "cpc", Term: ""}),
		IfHasQueryParam("fbclid", SessionSourceMediumTerm{Source: "facebook", Medium: "cpc", Term: ""}),
		IfHasQueryParam("twclid", SessionSourceMediumTerm{Source: "twitter", Medium: "cpc", Term: ""}),
		IfHasQueryParam("li_fat_id", SessionSourceMediumTerm{Source: "linkedin", Medium: "cpc", Term: ""}),
		IfHasQueryParam("ttclid", SessionSourceMediumTerm{Source: "tiktok", Medium: "cpc", Term: ""}),
		IfHasQueryParam("ScCid", SessionSourceMediumTerm{Source: "snapchat", Medium: "cpc", Term: ""}),
		IfHasQueryParam("irclickid", SessionSourceMediumTerm{Source: "impact", Medium: "affiliate", Term: ""}),
		IfQueryParamEquals("gclsrc", "aw.ds", SessionSourceMediumTerm{Source: "google", Medium: "cpc", Term: ""}),
		IfQueryParamEquals("gclsrc", "3p.ds", SessionSourceMediumTerm{Source: "google", Medium: "display", Term: ""}),
	),
	must(NewVideoSourceMediumTermDetector()),
	must(NewEmailSourceMediumTermDetector()),
	NewMailRefererSourceMediumTermDetector(),
	must(NewSocialsSourceMediumTermDetector()),
	must(NewAISourceMediumTermDetector()),
	must(NewSearchEngineSourceMediumTermDetector()),
	NewGenericReferralSourceMediumTermDetector(),
	NewDirectSourceMediumTermDetector(),
)

// SessionSourceColumn is our guess on the source of the session.
var SessionSourceColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionSource.ID,
	columns.CoreInterfaces.SessionSource.Field,
	0,
	func(e *schema.Event) (any, error) {
		sourceMediumTerm, ok := sessionSourceMediumTermDetector.Detect(e)
		if !ok {
			return nil, nil // nolint:nilnil // nil is a valid value for this column
		}
		WriteSessionSourceMediumTerm(e, sourceMediumTerm)
		return sourceMediumTerm.Source, nil
	},
	func(e *schema.Event) bool { return true }, // first event is fine
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventPageLocation.ID,
			GreaterOrEqualTo: "1.0.0",
		},
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventUtmSource.ID,
			GreaterOrEqualTo: "1.0.0",
		},
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventUtmMedium.ID,
			GreaterOrEqualTo: "1.0.0",
		},
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventUtmTerm.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session Source",
		"Traffic origin (e.g., google, facebook, direct) detected from referrer, click IDs, or UTM parameters. For details, see the D8A documentation on traffic attribution.", // nolint:lll // it's a description
	),
)

func must(d SourceMediumTermDetector, err error) SourceMediumTermDetector {
	if err != nil {
		logrus.Panicf("failed to create source medium term detector: %v", err)
	}
	return d
}

//go:embed smt/searchengines.yaml
var searchEnginesYAML []byte

//go:embed smt/socials.yaml
var socialsYAML []byte

//go:embed smt/ai.yaml
var aiYAML []byte

//go:embed smt/video.yaml
var videoYAML []byte

//go:embed smt/emails.yaml
var emailsYAML []byte

// SourceMediumTermDetector is an interface for detecting the source, medium, and term of an event.
type SourceMediumTermDetector interface {
	Detect(event *schema.Event) (SessionSourceMediumTerm, bool)
}

type compositeSourceMediumTermDetector struct {
	detectors []SourceMediumTermDetector
}

func (d *compositeSourceMediumTermDetector) Detect(event *schema.Event) (SessionSourceMediumTerm, bool) {
	var result SessionSourceMediumTerm
	var found bool

	parsed := ensureParsedURLs(event)

	// Run all detectors first
	for _, detector := range d.detectors {
		sourceMediumTerm, ok := detector.Detect(event)
		if ok {
			result = sourceMediumTerm
			found = true
			break
		}
	}

	if !found {
		return SessionSourceMediumTerm{}, false
	}

	// Apply UTM parameter overrides from page location if they exist
	if parsed.pageQP != nil {
		if utmSource := parsed.pageQP.Get("utm_source"); utmSource != "" {
			result.Source = utmSource
		}
		if utmMedium := parsed.pageQP.Get("utm_medium"); utmMedium != "" {
			result.Medium = utmMedium
		}
		if utmTerm := parsed.pageQP.Get("utm_term"); utmTerm != "" {
			result.Term = utmTerm
		}
	}

	return normalizeSMT(result), true
}

func normalizeSMT(smt SessionSourceMediumTerm) SessionSourceMediumTerm {
	return SessionSourceMediumTerm{
		Source: normalizeSMTValue(smt.Source),
		Medium: normalizeSMTValue(smt.Medium),
		Term:   normalizeSMTValue(smt.Term),
	}
}

func normalizeSMTValue(value string) string {
	// Convert to lowercase
	value = strings.ToLower(value)

	// Replace spaces with hyphens
	value = strings.ReplaceAll(value, " ", "-")

	// Remove special characters: /, \, ?, &, =, #
	value = strings.ReplaceAll(value, "/", "")
	value = strings.ReplaceAll(value, "\\", "")
	value = strings.ReplaceAll(value, "?", "")
	value = strings.ReplaceAll(value, "&", "")
	value = strings.ReplaceAll(value, "=", "")
	value = strings.ReplaceAll(value, "#", "")

	// Remove leading/trailing whitespace
	value = strings.TrimSpace(value)

	return value
}

func NewCompositeSourceMediumTermDetector(detectors ...SourceMediumTermDetector) SourceMediumTermDetector {
	return &compositeSourceMediumTermDetector{detectors: detectors}
}

const parsedURLsMetadataKey = "session_smt_parsed_urls"

type parsedURLs struct {
	pageURL      *url.URL
	pageQP       url.Values
	refURL       *url.URL
	refQP        url.Values
	refHost      string
	refHostNoWWW string
	refRaw       string
}

func ensureParsedURLs(event *schema.Event) *parsedURLs {
	if event.Metadata == nil {
		event.Metadata = make(map[string]any)
	}
	if cached, ok := event.Metadata[parsedURLsMetadataKey]; ok {
		if parsed, ok := cached.(*parsedURLs); ok && parsed != nil {
			return parsed
		}
	}

	result := &parsedURLs{}

	pageRaw := columns.ReadOriginalPageLocation(event)
	if pageRaw != "" {
		if parsed, err := url.Parse(pageRaw); err == nil {
			result.pageURL = parsed
			result.pageQP = parsed.Query()
		}
	}

	refRaw := event.BoundHit.MustServerAttributes().Headers.Get("Referer")
	if refRaw != "" {
		if parsed, err := url.Parse(refRaw); err == nil {
			result.refRaw = refRaw
			result.refURL = parsed
			result.refQP = parsed.Query()
			result.refHost = strings.ReplaceAll(
				strings.ToLower(parsed.Hostname()),
				" ",
				"-",
			)
			result.refHostNoWWW = trimWWW(result.refHost)
		}
	}

	event.Metadata[parsedURLsMetadataKey] = result
	return result
}

func trimWWW(host string) string {
	host = strings.ToLower(host)
	return strings.TrimPrefix(host, "www.")
}

type directSourceMediumTermDetector struct {
}

func (d *directSourceMediumTermDetector) Detect(event *schema.Event) (SessionSourceMediumTerm, bool) {
	return SessionSourceMediumTerm{Source: "direct", Medium: "none", Term: ""}, true
}

func NewDirectSourceMediumTermDetector() SourceMediumTermDetector {
	return &directSourceMediumTermDetector{}
}

type pageLocationParamsSourceMediumTermDetector struct {
	conditions []func(qp url.Values) (SessionSourceMediumTerm, bool)
}

func (d *pageLocationParamsSourceMediumTermDetector) Detect(event *schema.Event) (SessionSourceMediumTerm, bool) {
	parsed := ensureParsedURLs(event)
	if parsed.pageQP == nil {
		return SessionSourceMediumTerm{}, false
	}
	for _, condition := range d.conditions {
		sourceMediumTerm, ok := condition(parsed.pageQP)
		if ok {
			return sourceMediumTerm, true
		}
	}
	return SessionSourceMediumTerm{}, false
}

func NewPageLocationParamsSourceMediumTermDetector(
	conditions ...func(qp url.Values) (SessionSourceMediumTerm, bool),
) SourceMediumTermDetector {
	return &pageLocationParamsSourceMediumTermDetector{conditions: conditions}
}

// IfHasQueryParam returns a function that checks if a query parameter is present.
func IfHasQueryParam(param string, smt SessionSourceMediumTerm) func(qp url.Values) (SessionSourceMediumTerm, bool) {
	return func(qp url.Values) (SessionSourceMediumTerm, bool) {
		if qp.Get(param) != "" {
			return smt, true
		}
		return SessionSourceMediumTerm{}, false
	}
}

// IfQueryParamEquals returns a function that checks if a query parameter equals a given value.
func IfQueryParamEquals(
	param, value string,
	smt SessionSourceMediumTerm,
) func(qp url.Values) (SessionSourceMediumTerm, bool) {
	return func(qp url.Values) (SessionSourceMediumTerm, bool) {
		if qp.Get(param) == value {
			return smt, true
		}
		return SessionSourceMediumTerm{}, false
	}
}

type refererCondition func(cleanedReferer string, qp url.Values) (SessionSourceMediumTerm, bool)
type fromRefererSourceMediumTermDetector struct {
	conditions []refererCondition
}

func (d *fromRefererSourceMediumTermDetector) Detect(event *schema.Event) (SessionSourceMediumTerm, bool) {
	parsedURLs := ensureParsedURLs(event)
	if parsedURLs.refHost == "" {
		return SessionSourceMediumTerm{}, false
	}
	for _, condition := range d.conditions {
		sourceMediumTerm, ok := condition(parsedURLs.refHost, parsedURLs.refQP)
		if ok {
			return sourceMediumTerm, true
		}
		sourceMediumTerm, ok = condition(parsedURLs.refHostNoWWW, parsedURLs.refQP)
		if ok {
			return sourceMediumTerm, true
		}
	}
	return SessionSourceMediumTerm{}, false
}

type searchEngineEntry struct {
	URLs     []string `yaml:"urls"`
	Params   []string `yaml:"params"`
	Charsets []string `yaml:"charsets,omitempty"`
}

// NewFromRefererRegexCondition returns a function that checks if a referer matches a given regex.
func NewFromRefererRegexCondition(
	regex *regexp.Regexp, smt func(qp url.Values) SessionSourceMediumTerm,
) refererCondition {
	return func(cleanedReferer string, qp url.Values) (SessionSourceMediumTerm, bool) {
		if regex.MatchString(cleanedReferer) {
			return smt(qp), true
		}
		return SessionSourceMediumTerm{}, false
	}
}

// NewFromRefererExactMatchCondition returns a function that checks if a referer matches a given exact match.
func NewFromRefererExactMatchCondition(
	exactMatch string, smt func(qp url.Values) SessionSourceMediumTerm,
) refererCondition {
	return func(cleanedReferer string, qp url.Values) (SessionSourceMediumTerm, bool) {
		if cleanedReferer == exactMatch {
			return smt(qp), true
		}
		return SessionSourceMediumTerm{}, false
	}
}

// NewSearchEngineSourceMediumTermDetector returns a new source medium term detector for search engines.
func NewSearchEngineSourceMediumTermDetector() (SourceMediumTermDetector, error) {
	searchEngines := make(map[string][]searchEngineEntry)
	err := yaml.Unmarshal(searchEnginesYAML, &searchEngines)
	if err != nil {
		return nil, err
	}
	conditions := []refererCondition{}
	for searchEngineName, searchEngine := range searchEngines {
		for _, entry := range searchEngine {
			for _, urlPattern := range entry.URLs {
				appendSearchEngineCondition(&conditions, searchEngineName, entry, urlPattern)
			}
		}
	}
	return &fromRefererSourceMediumTermDetector{
		conditions: conditions,
	}, nil
}

func tryMatchTerm(qp url.Values, searchEngineEntry searchEngineEntry) string {
	// This does only match for non-regex params, should be fine for now,
	// the engines using path params are rare, regexes add too much performance overhead
	// and almost all browser strip referer query params anyway
	for _, param := range searchEngineEntry.Params {
		if qp.Get(param) != "" {
			return qp.Get(param)
		}
	}
	return ""
}

func appendSearchEngineCondition(
	conditions *[]refererCondition,
	searchEngineName string,
	entry searchEngineEntry,
	urlPattern string,
) {
	if strings.Contains(urlPattern, "{}") {
		if strings.HasPrefix(urlPattern, "{}") && !strings.Contains(urlPattern[2:], "{}") {
			suffix := strings.TrimPrefix(urlPattern, "{}")
			*conditions = append(*conditions, searchEngineSuffixCondition(searchEngineName, entry, suffix))
			return
		}
		if strings.HasSuffix(urlPattern, "{}") && !strings.Contains(urlPattern[:len(urlPattern)-2], "{}") {
			prefix := strings.TrimSuffix(urlPattern, "{}")
			*conditions = append(*conditions, searchEnginePrefixCondition(searchEngineName, entry, prefix))
			return
		}
		*conditions = append(*conditions, searchEngineRegexCondition(searchEngineName, entry, urlPattern))
		return
	}

	*conditions = append(*conditions, searchEngineExactCondition(searchEngineName, entry, urlPattern))
}

// Prefer edge-only wildcard string checks to dodge regex work in the hot path.
func searchEngineSuffixCondition(searchEngineName string, entry searchEngineEntry, suffix string) refererCondition {
	return func(cleanedReferer string, qp url.Values) (SessionSourceMediumTerm, bool) {
		if strings.HasSuffix(cleanedReferer, suffix) {
			return searchEngineResult(searchEngineName, entry, qp), true
		}
		return SessionSourceMediumTerm{}, false
	}
}

// Prefer edge-only wildcard string checks to dodge regex work in the hot path.
func searchEnginePrefixCondition(searchEngineName string, entry searchEngineEntry, prefix string) refererCondition {
	return func(cleanedReferer string, qp url.Values) (SessionSourceMediumTerm, bool) {
		if strings.HasPrefix(cleanedReferer, prefix) {
			return searchEngineResult(searchEngineName, entry, qp), true
		}
		return SessionSourceMediumTerm{}, false
	}
}

func searchEngineRegexCondition(searchEngineName string, entry searchEngineEntry, urlPattern string) refererCondition {
	theRegex := regexp.MustCompile(strings.ReplaceAll(
		regexp.QuoteMeta(urlPattern), "\\{\\}", ".*"))
	return NewFromRefererRegexCondition(
		theRegex,
		func(qp url.Values) SessionSourceMediumTerm {
			return searchEngineResult(searchEngineName, entry, qp)
		},
	)
}

func searchEngineExactCondition(searchEngineName string, entry searchEngineEntry, urlPattern string) refererCondition {
	return NewFromRefererExactMatchCondition(urlPattern, func(qp url.Values) SessionSourceMediumTerm {
		return searchEngineResult(searchEngineName, entry, qp)
	})
}

func searchEngineResult(searchEngineName string, entry searchEngineEntry, qp url.Values) SessionSourceMediumTerm {
	return SessionSourceMediumTerm{
		Source: strings.ToLower(searchEngineName),
		Medium: "organic",
		Term:   tryMatchTerm(qp, entry),
	}
}

// NewSocialsSourceMediumTermDetector returns a new source medium term detector for socials.yaml
func NewSocialsSourceMediumTermDetector() (SourceMediumTermDetector, error) {
	socials := make(map[string][]string)
	err := yaml.Unmarshal(socialsYAML, &socials)
	if err != nil {
		return nil, err
	}
	conditions := []refererCondition{}
	for socialName, urls := range socials {
		for _, urlPattern := range urls {
			conditions = append(
				conditions,
				NewFromRefererExactMatchCondition(urlPattern, func(qp url.Values) SessionSourceMediumTerm {
					return SessionSourceMediumTerm{
						Source: strings.ToLower(socialName),
						Medium: "social",
						Term:   "",
					}
				}),
			)
		}
	}
	return &fromRefererSourceMediumTermDetector{
		conditions: conditions,
	}, nil
}

// NewAISourceMediumTermDetector returns a new source medium term detector for ai.yaml
func NewAISourceMediumTermDetector() (SourceMediumTermDetector, error) {
	ais := make(map[string][]string)
	err := yaml.Unmarshal(aiYAML, &ais)
	if err != nil {
		return nil, err
	}
	conditions := []refererCondition{}
	for aiName, urls := range ais {
		for _, urlPattern := range urls {
			conditions = append(
				conditions,
				NewFromRefererExactMatchCondition(urlPattern, func(qp url.Values) SessionSourceMediumTerm {
					return SessionSourceMediumTerm{
						Source: strings.ToLower(aiName),
						Medium: "ai",
						Term:   "",
					}
				}),
			)
		}
	}
	return &fromRefererSourceMediumTermDetector{
		conditions: conditions,
	}, nil
}

// NewVideoSourceMediumTermDetector returns a new source medium term detector for video.yaml
func NewVideoSourceMediumTermDetector() (SourceMediumTermDetector, error) {
	videos := make(map[string][]string)
	err := yaml.Unmarshal(videoYAML, &videos)
	if err != nil {
		return nil, err
	}
	conditions := []refererCondition{}
	for videoName, urls := range videos {
		for _, urlPattern := range urls {
			conditions = append(
				conditions,
				NewFromRefererExactMatchCondition(urlPattern, func(qp url.Values) SessionSourceMediumTerm {
					return SessionSourceMediumTerm{
						Source: strings.ToLower(videoName),
						Medium: "video",
						Term:   "",
					}
				}),
			)
		}
	}
	return &fromRefererSourceMediumTermDetector{
		conditions: conditions,
	}, nil
}

// NewEmailSourceMediumTermDetector returns a new source medium term detector for emails.yaml
func NewEmailSourceMediumTermDetector() (SourceMediumTermDetector, error) {
	emails := make(map[string][]string)
	err := yaml.Unmarshal(emailsYAML, &emails)
	if err != nil {
		return nil, err
	}
	conditions := []refererCondition{}
	for emailName, urls := range emails {
		for _, urlPattern := range urls {
			conditions = append(
				conditions,
				NewFromRefererExactMatchCondition(urlPattern, func(qp url.Values) SessionSourceMediumTerm {
					return SessionSourceMediumTerm{
						Source: strings.ToLower(emailName),
						Medium: "email",
						Term:   "",
					}
				}),
			)
		}
	}
	return &fromRefererSourceMediumTermDetector{
		conditions: conditions,
	}, nil
}

type mailRefererSourceMediumTermDetector struct {
}

func (d *mailRefererSourceMediumTermDetector) Detect(event *schema.Event) (SessionSourceMediumTerm, bool) {
	parsed := ensureParsedURLs(event)
	if parsed.refRaw == "" {
		return SessionSourceMediumTerm{}, false
	}
	if !strings.Contains(parsed.refRaw, "mail.") {
		return SessionSourceMediumTerm{}, false
	}
	if parsed.refHostNoWWW == "" {
		return SessionSourceMediumTerm{}, false
	}
	return SessionSourceMediumTerm{
		Source: parsed.refHostNoWWW,
		Medium: "email",
		Term:   "",
	}, true
}

// NewMailRefererSourceMediumTermDetector returns a new source medium term detector for mail referer.
func NewMailRefererSourceMediumTermDetector() SourceMediumTermDetector {
	return &mailRefererSourceMediumTermDetector{}
}

type genericReferralSourceMediumTermDetector struct {
}

func (d *genericReferralSourceMediumTermDetector) Detect(event *schema.Event) (SessionSourceMediumTerm, bool) {
	parsed := ensureParsedURLs(event)
	if parsed.refHostNoWWW == "" {
		return SessionSourceMediumTerm{}, false
	}
	if parsed.pageURL == nil {
		return SessionSourceMediumTerm{}, false
	}
	pageDomain := trimWWW(parsed.pageURL.Hostname())
	if pageDomain == "" {
		return SessionSourceMediumTerm{}, false
	}
	if parsed.refHostNoWWW == pageDomain {
		return SessionSourceMediumTerm{}, false
	}
	return SessionSourceMediumTerm{
		Source: parsed.refHostNoWWW,
		Medium: "referral",
		Term:   "",
	}, true
}

// NewGenericReferralSourceMediumTermDetector returns a new source medium term detector for generic referral.
func NewGenericReferralSourceMediumTermDetector() SourceMediumTermDetector {
	return &genericReferralSourceMediumTermDetector{}
}

// SessionSourceMediumTerm is a struct that contains the source, medium, and term of a session.
type SessionSourceMediumTerm struct {
	Source string
	Medium string
	Term   string
}

// WriteSessionSourceMediumTerm stores the session source, medium, and term in the event metadata.
func WriteSessionSourceMediumTerm(event *schema.Event, sourceMediumTerm SessionSourceMediumTerm) {
	if event.Metadata == nil {
		event.Metadata = make(map[string]any)
	}
	event.Metadata[columns.MetadataKeySessionSourceMediumTerm] = sourceMediumTerm
}

// ReadSessionSourceMediumTerm retrieves the session source, medium, and term from event metadata.
func ReadSessionSourceMediumTerm(event *schema.Event) SessionSourceMediumTerm {
	if event.Metadata == nil {
		return SessionSourceMediumTerm{}
	}
	sourceMediumTerm, ok := event.Metadata[columns.MetadataKeySessionSourceMediumTerm]
	if !ok {
		return SessionSourceMediumTerm{}
	}
	sourceMediumTermObj, ok := sourceMediumTerm.(SessionSourceMediumTerm)
	if !ok {
		return SessionSourceMediumTerm{}
	}
	return sourceMediumTermObj
}
