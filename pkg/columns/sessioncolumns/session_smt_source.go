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
	NewFromUTMParamsSourceMediumTermDetector(),
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
	must(NewSearchEngineSsourceMediumTermDetector()),
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
		"The source of the session.",
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
	for _, detector := range d.detectors {
		sourceMediumTerm, ok := detector.Detect(event)
		if ok {
			return sourceMediumTerm, true
		}
	}
	return SessionSourceMediumTerm{}, false
}

func NewCompositeSourceMediumTermDetector(detectors ...SourceMediumTermDetector) SourceMediumTermDetector {
	return &compositeSourceMediumTermDetector{detectors: detectors}
}

type fromUTMParamsSourceMediumTermDetector struct {
}

func (d *fromUTMParamsSourceMediumTermDetector) Detect(event *schema.Event) (SessionSourceMediumTerm, bool) {
	source, ok := event.Values[columns.CoreInterfaces.EventUtmSource.Field.Name]
	if !ok {
		return SessionSourceMediumTerm{}, false
	}
	sourceStr, ok := source.(string)
	if !ok {
		return SessionSourceMediumTerm{}, false
	}
	medium, ok := event.Values[columns.CoreInterfaces.EventUtmMedium.Field.Name]
	if !ok {
		return SessionSourceMediumTerm{}, false
	}
	mediumStr, ok := medium.(string)
	if !ok {
		return SessionSourceMediumTerm{}, false
	}
	termStr := ""
	term, ok := event.Values[columns.CoreInterfaces.EventUtmTerm.Field.Name]
	if ok {
		if termStrVal, ok := term.(string); ok {
			termStr = termStrVal
		}
	}
	return SessionSourceMediumTerm{Source: sourceStr, Medium: mediumStr, Term: termStr}, true
}

func NewFromUTMParamsSourceMediumTermDetector() SourceMediumTermDetector {
	return &fromUTMParamsSourceMediumTermDetector{}
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
	pageLocation := columns.ReadOriginalPageLocation(event)
	parsed, err := url.Parse(pageLocation)
	if err != nil {
		return SessionSourceMediumTerm{}, false
	}
	qp := parsed.Query()
	for _, condition := range d.conditions {
		sourceMediumTerm, ok := condition(qp)
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
	referrer := d.cleanedReferer(event.BoundHit.Headers.Get("Referer"))
	if referrer == "" {
		return SessionSourceMediumTerm{}, false
	}
	parsed, err := url.Parse(event.BoundHit.Headers.Get("Referer"))
	if err != nil {
		return SessionSourceMediumTerm{}, false
	}
	qp := parsed.Query()
	for _, condition := range d.conditions {
		sourceMediumTerm, ok := condition(referrer, qp)
		if ok {
			return sourceMediumTerm, true
		}
	}
	return SessionSourceMediumTerm{}, false
}

func (d *fromRefererSourceMediumTermDetector) cleanedReferer(referer string) string {
	if referer == "" {
		return ""
	}
	parsed, err := url.Parse(referer)
	if err != nil {
		return ""
	}
	hostname := parsed.Hostname()
	if hostname == "" {
		return ""
	}
	hostname = strings.ToLower(hostname)
	return hostname
}

func normalizeRefererDomain(referer string) string {
	if referer == "" {
		return ""
	}
	parsed, err := url.Parse(referer)
	if err != nil {
		return ""
	}
	hostname := parsed.Hostname()
	if hostname == "" {
		return ""
	}
	hostname = strings.ToLower(hostname)
	hostname = strings.TrimPrefix(hostname, "www.")
	return hostname
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

// NewSearchEngineSsourceMediumTermDetector returns a new source medium term detector for search engines.
func NewSearchEngineSsourceMediumTermDetector() (SourceMediumTermDetector, error) {
	searchEngines := make(map[string][]searchEngineEntry)
	err := yaml.Unmarshal(searchEnginesYAML, &searchEngines)
	if err != nil {
		return nil, err
	}
	conditions := []refererCondition{}
	for searchEngineName, searchEngine := range searchEngines {
		for _, entry := range searchEngine {
			for _, urlPattern := range entry.URLs {
				if strings.Contains(urlPattern, "{}") {
					theRegex := regexp.MustCompile(strings.ReplaceAll(
						regexp.QuoteMeta(urlPattern), "\\{\\}", ".*"))
					conditions = append(
						conditions,
						NewFromRefererRegexCondition(
							theRegex,
							func(qp url.Values) SessionSourceMediumTerm {
								return SessionSourceMediumTerm{
									Source: strings.ToLower(searchEngineName),
									Medium: "organic",
									Term:   tryMatchTerm(qp, entry),
								}
							},
						),
					)
				} else {
					conditions = append(
						conditions,
						NewFromRefererExactMatchCondition(urlPattern, func(qp url.Values) SessionSourceMediumTerm {
							return SessionSourceMediumTerm{
								Source: strings.ToLower(searchEngineName),
								Medium: "organic",
								Term:   tryMatchTerm(qp, entry),
							}
						}),
					)
				}
			}
		}
	}
	return &fromRefererSourceMediumTermDetector{
		conditions: conditions,
	}, nil
}

func tryMatchTerm(qp url.Values, searchEngineEntry searchEngineEntry) string {
	logrus.Error(searchEngineEntry)
	// This does only match for non-regex params, should be fine for now,
	// the engines using path params are rare and regexes add too much performance overhead
	for _, param := range searchEngineEntry.Params {
		if qp.Get(param) != "" {
			return qp.Get(param)
		}
	}
	return ""
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
	referer := event.BoundHit.Headers.Get("Referer")
	if referer == "" {
		return SessionSourceMediumTerm{}, false
	}
	if !strings.Contains(referer, "mail.") {
		return SessionSourceMediumTerm{}, false
	}
	normalizedDomain := normalizeRefererDomain(referer)
	if normalizedDomain == "" {
		return SessionSourceMediumTerm{}, false
	}
	return SessionSourceMediumTerm{
		Source: normalizedDomain,
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
	referer := event.BoundHit.Headers.Get("Referer")
	if referer == "" {
		return SessionSourceMediumTerm{}, false
	}
	refererDomain := normalizeRefererDomain(referer)
	if refererDomain == "" {
		return SessionSourceMediumTerm{}, false
	}
	pageLocation := columns.ReadOriginalPageLocation(event)
	if pageLocation == "" {
		return SessionSourceMediumTerm{}, false
	}
	pageDomain := normalizeRefererDomain(pageLocation)
	if pageDomain == "" {
		return SessionSourceMediumTerm{}, false
	}
	if refererDomain == pageDomain {
		return SessionSourceMediumTerm{}, false
	}
	return SessionSourceMediumTerm{
		Source: refererDomain,
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
