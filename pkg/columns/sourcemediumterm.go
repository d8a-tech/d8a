package columns

import (
	_ "embed"
	"net/url"
	"regexp"
	"strings"

	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert/yaml"
)

//go:embed searchengines.yaml
var searchEnginesYAML []byte

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
	source, ok := event.Values[CoreInterfaces.EventUtmSource.Field.Name]
	if !ok {
		return SessionSourceMediumTerm{}, false
	}
	sourceStr, ok := source.(string)
	if !ok {
		return SessionSourceMediumTerm{}, false
	}
	medium, ok := event.Values[CoreInterfaces.EventUtmMedium.Field.Name]
	if !ok {
		return SessionSourceMediumTerm{}, false
	}
	mediumStr, ok := medium.(string)
	if !ok {
		return SessionSourceMediumTerm{}, false
	}
	var termStr string = ""
	term, ok := event.Values[CoreInterfaces.EventUtmTerm.Field.Name]
	if ok {
		termStr, ok = term.(string)
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
	pageLocation := ReadOriginalPageLocation(event)
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

func NewPageLocationParamsSourceMediumTermDetector(conditions ...func(qp url.Values) (SessionSourceMediumTerm, bool)) SourceMediumTermDetector {
	return &pageLocationParamsSourceMediumTermDetector{conditions: conditions}
}

func IfHasQueryParam(param string, smt SessionSourceMediumTerm) func(qp url.Values) (SessionSourceMediumTerm, bool) {
	return func(qp url.Values) (SessionSourceMediumTerm, bool) {
		if qp.Get(param) != "" {
			return smt, true
		}
		return SessionSourceMediumTerm{}, false
	}
}

func IfQueryParamEquals(param string, value string, smt SessionSourceMediumTerm) func(qp url.Values) (SessionSourceMediumTerm, bool) {
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
	pageLocation := ReadOriginalPageLocation(event)
	parsed, err := url.Parse(pageLocation)
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
	hostname = strings.TrimPrefix(hostname, "www.")

	return hostname
}

type searchEngineEntry struct {
	URLs     []string `yaml:"urls"`
	Params   []string `yaml:"params"`
	Charsets []string `yaml:"charsets,omitempty"`
}

func NewFromRefererRegexCondition(regex *regexp.Regexp, smt SessionSourceMediumTerm) refererCondition {
	return func(cleanedReferer string, qp url.Values) (SessionSourceMediumTerm, bool) {
		if regex.MatchString(cleanedReferer) {
			return smt, true
		}
		return SessionSourceMediumTerm{}, false
	}
}

func NewFromRefererExactMatchCondition(exactMatch string, smt SessionSourceMediumTerm) refererCondition {
	return func(cleanedReferer string, qp url.Values) (SessionSourceMediumTerm, bool) {
		if cleanedReferer == exactMatch {
			return smt, true
		}
		return SessionSourceMediumTerm{}, false
	}
}

func NewFromRefererSourceMediumTermDetector() (SourceMediumTermDetector, error) {
	searchEngines := make(map[string][]searchEngineEntry)
	err := yaml.Unmarshal(searchEnginesYAML, &searchEngines)
	if err != nil {
		return nil, err
	}
	conditions := []refererCondition{}
	for searchEngineName, searchEngine := range searchEngines {
		for _, entry := range searchEngine {
			for _, url := range entry.URLs {
				if strings.Contains(url, "{}") {
					// TODO: try to extract TERMs
					theRegex := regexp.MustCompile(strings.ReplaceAll(
						regexp.QuoteMeta(url), "\\{\\}", ".*"))
					conditions = append(
						conditions,
						NewFromRefererRegexCondition(
							theRegex,
							SessionSourceMediumTerm{
								Source: strings.ToLower(searchEngineName),
								Medium: "organic",
								Term:   "",
							},
						),
					)
				} else {
					conditions = append(
						conditions,
						NewFromRefererExactMatchCondition(url, SessionSourceMediumTerm{
							Source: strings.ToLower(searchEngineName),
							Medium: "organic",
							Term:   "",
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

type SessionSourceMediumTerm struct {
	Source string
	Medium string
	Term   string
}

// SessionSourceColumn is our guess on the source of the session.
func SessionSourceColumn(
	isPageViewEvent EventBoolPredicateFunc,
	options ...SessionColumnOptions,
) schema.SessionColumn {
	detector := NewCompositeSourceMediumTermDetector(
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
		func() SourceMediumTermDetector {
			detector, err := NewFromRefererSourceMediumTermDetector()
			if err != nil {
				logrus.Panicf("failed to create from referer source medium term detector: %v", err)
			}
			return detector
		}(),
		NewDirectSourceMediumTermDetector(),
	)
	options = append(options,
		WithSessionColumnDependsOn(
			schema.DependsOnEntry{
				Interface:        CoreInterfaces.EventPageLocation.ID,
				GreaterOrEqualTo: "1.0.0",
			},
			schema.DependsOnEntry{
				Interface:        CoreInterfaces.EventUtmSource.ID,
				GreaterOrEqualTo: "1.0.0",
			},
			schema.DependsOnEntry{
				Interface:        CoreInterfaces.EventUtmMedium.ID,
				GreaterOrEqualTo: "1.0.0",
			},
			schema.DependsOnEntry{
				Interface:        CoreInterfaces.EventUtmTerm.ID,
				GreaterOrEqualTo: "1.0.0",
			},
		),
		WithSessionColumnDocs(
			"Session Source",
			"The source of the session.",
		),
	)
	return NthEventMatchingPredicateValueColumn(
		CoreInterfaces.SessionSource.ID,
		CoreInterfaces.SessionSource.Field,
		0,
		func(e *schema.Event) (any, error) {
			sourceMediumTerm, ok := detector.Detect(e)
			if !ok {
				return nil, nil
			}
			WriteSessionSourceMediumTerm(e, sourceMediumTerm)
			return sourceMediumTerm.Source, nil
		},
		isPageViewEvent,
		options...,
	)
}

func SessionMediumColumn(
	isPageViewEvent EventBoolPredicateFunc,
	options ...SessionColumnOptions,
) schema.SessionColumn {
	options = append(options,
		WithSessionColumnDependsOn(
			schema.DependsOnEntry{
				Interface:        CoreInterfaces.SessionSource.ID,
				GreaterOrEqualTo: "1.0.0",
			},
		),
		WithSessionColumnDocs(
			"Session Medium",
			"The medium of the session.",
		),
	)
	return NthEventMatchingPredicateValueColumn(
		CoreInterfaces.SessionMedium.ID,
		CoreInterfaces.SessionMedium.Field,
		0,
		func(e *schema.Event) (any, error) {
			return ReadSessionSourceMediumTerm(e).Medium, nil
		},
		isPageViewEvent,
		options...,
	)
}

func SessionTermColumn(
	isPageViewEvent EventBoolPredicateFunc,
	options ...SessionColumnOptions,
) schema.SessionColumn {
	options = append(options,
		WithSessionColumnDependsOn(
			schema.DependsOnEntry{
				Interface:        CoreInterfaces.SessionSource.ID,
				GreaterOrEqualTo: "1.0.0",
			},
		),
		WithSessionColumnDocs(
			"Session Term",
			"The term of the session.",
		),
	)
	return NthEventMatchingPredicateValueColumn(
		CoreInterfaces.SessionTerm.ID,
		CoreInterfaces.SessionTerm.Field,
		0,
		func(e *schema.Event) (any, error) {
			return ReadSessionSourceMediumTerm(e).Term, nil
		},
		isPageViewEvent,
		options...,
	)
}
