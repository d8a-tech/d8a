// nolint
package columntests

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/columnset"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/d8a-tech/d8a/pkg/sessions"
	"github.com/d8a-tech/d8a/pkg/warehouse"
)

// testHitOne represents the first test hit from partition 1, request 1 (CID=ag9, SESSION_STAMP=127.0.0.1)
var testHitOne = &hits.Hit{
	ID:                    "test-hit-one",
	AuthoritativeClientID: "ag9",
	ClientID:              "ag9",
	PropertyID:            "G-5T0Z13HKP4",
	IP:                    "127.0.0.1",
	Host:                  "localhost.d8astage.xyz",
	ServerReceivedTime:    time.Now().Format(time.RFC3339),
	QueryParams: url.Values{
		"v":                []string{"2"},
		"tid":              []string{"G-5T0Z13HKP4"},
		"gtm":              []string{"45je5580h2v9219555710za200"},
		"_p":               []string{"1746817938582"},
		"gcd":              []string{"13l3l3l2l1l1"},
		"npa":              []string{"1"},
		"dma_cps":          []string{"syphamo"},
		"dma":              []string{"1"},
		"tag_exp":          []string{"101509157~103101750~103101752~103116026~103130495~103130497~103200004~103211513~103233427~103251618~103251620~103284320~103284322~103301114~103301116"},
		"cid":              []string{"ag9"},
		"ul":               []string{"en-us"},
		"sr":               []string{"1745x982"},
		"uaa":              []string{"x86"},
		"uab":              []string{"64"},
		"uafvl":            []string{"Not(A%253ABrand%3B24.0.0.0%7CChromium%3B122.0.6261.171"},
		"uamb":             []string{"0"},
		"uam":              []string{""},
		"uap":              []string{"Linux"},
		"uapv":             []string{"6.14.5"},
		"uaw":              []string{"0"},
		"frm":              []string{"0"},
		"pscdl":            []string{"noapi"},
		"_eu":              []string{"AAAAAAQ"},
		"_s":               []string{"1"},
		"sid":              []string{"1746817858"},
		"sct":              []string{"1"},
		"seg":              []string{"1"},
		"dl":               []string{"https%3A%2F%2Fd8a-tech.github.io%2Fanalytics-playground%2Findex.html"},
		"dr":               []string{"https%3A%2F%2Fd8a-tech.github.io%2Fanalytics-playground%2Fcheckout.html"},
		"dt":               []string{"Food%20Shop"},
		"en":               []string{"page_view"},
		"_ee":              []string{"1"},
		"tfd":              []string{"565"},
		"sessionStamp":     []string{"127.0.0.1"},
		"ep.content_group": []string{"product"},
		"ep.content_id":    []string{"C_1234"},
	},
	Path:   "/g/collect",
	Method: "POST",
	Headers: url.Values{
		"authority":          []string{"region1.google-analytics.com"},
		"accept":             []string{"*/*"},
		"accept-language":    []string{"en-US,en;q=0.8"},
		"content-length":     []string{"0"},
		"origin":             []string{"https://d8a-tech.github.io"},
		"priority":           []string{"u=1, i"},
		"referer":            []string{"https://d8a-tech.github.io/"},
		"sec-ch-ua":          []string{`"Not(A:Brand";v="24", "Chromium";v="122"`},
		"sec-ch-ua-mobile":   []string{"?0"},
		"sec-ch-ua-platform": []string{`"Linux"`},
		"sec-fetch-dest":     []string{"empty"},
		"sec-fetch-mode":     []string{"no-cors"},
		"sec-fetch-site":     []string{"cross-site"},
		"user-agent":         []string{"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) QtWebEngine/6.8.3 Chrome/122.0.0.0 Safari/537.36"}, //nolint:lll // long user agent string
	},
	Metadata:  map[string]string{},
	Timestamp: time.Unix(0, 0).UTC(),
}

// testHitTwo represents the second test hit from partition 2, request 1 (CID=ag8, SESSION_STAMP=127.0.0.2)
var testHitTwo = &hits.Hit{
	ID:                    "test-hit-two",
	AuthoritativeClientID: "ag8",
	ClientID:              "ag8",
	PropertyID:            "G-5T0Z13HKP4",
	IP:                    "127.0.0.2",
	Host:                  "localhost.d8astage.xyz",
	ServerReceivedTime:    time.Now().Format(time.RFC3339),
	QueryParams: url.Values{
		"v":                    []string{"2"},
		"tid":                  []string{"G-5T0Z13HKP4"},
		"gtm":                  []string{"45je5580h2v9219555710za200"},
		"_p":                   []string{"1746817938582"},
		"gcd":                  []string{"13l3l3l2l1l1"},
		"npa":                  []string{"1"},
		"dma_cps":              []string{"syphamo"},
		"dma":                  []string{"1"},
		"tag_exp":              []string{"101509157~103101750~103101752~103116026~103130495~103130497~103200004~103211513~103233427~103251618~103251620~103284320~103284322~103301114~103301116"}, //nolint:lll // long GA4 tag experiment string
		"cid":                  []string{"ag8"},
		"ul":                   []string{"en-us"},
		"sr":                   []string{"1745x982"},
		"uaa":                  []string{"x86"},
		"uab":                  []string{"64"},
		"uafvl":                []string{"Not(A%253ABrand%3B24.0.0.0%7CChromium%3B122.0.6261.171"},
		"uamb":                 []string{"0"},
		"uam":                  []string{""},
		"uap":                  []string{"Linux"},
		"uapv":                 []string{"6.14.5"},
		"uaw":                  []string{"0"},
		"frm":                  []string{"0"},
		"pscdl":                []string{"noapi"},
		"_eu":                  []string{"AEAAAAQ"},
		"_s":                   []string{"2"},
		"sid":                  []string{"1746817858"},
		"sct":                  []string{"1"},
		"seg":                  []string{"1"},
		"dl":                   []string{"https%3A%2F%2Fd8a-tech.github.io%2Fanalytics-playground%2Findex.html"},
		"dr":                   []string{"https%3A%2F%2Fd8a-tech.github.io%2Fanalytics-playground%2Fcheckout.html"},
		"dt":                   []string{"Food%20Shop"},
		"en":                   []string{"scroll"},
		"epn.percent_scrolled": []string{"90"},
		"_et":                  []string{"10"},
		"tfd":                  []string{"5567"},
		"sessionStamp":         []string{"127.0.0.2"},
		"ep.content_group":     []string{"product"},
		"ep.content_id":        []string{"C_1234"},
	},
	Path:   "/g/collect",
	Method: "POST",
	Headers: url.Values{
		"authority":          []string{"region1.google-analytics.com"},
		"accept":             []string{"*/*"},
		"accept-language":    []string{"en-US,en;q=0.8"},
		"content-length":     []string{"0"},
		"origin":             []string{"https://d8a-tech.github.io"},
		"priority":           []string{"u=1, i"},
		"referer":            []string{"https://d8a-tech.github.io/"},
		"sec-ch-ua":          []string{`"Not(A:Brand";v="24", "Chromium";v="122"`},
		"sec-ch-ua-mobile":   []string{"?0"},
		"sec-ch-ua-platform": []string{`"Linux"`},
		"sec-fetch-dest":     []string{"empty"},
		"sec-fetch-mode":     []string{"no-cors"},
		"sec-fetch-site":     []string{"cross-site"},
		"user-agent":         []string{"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) QtWebEngine/6.8.3 Chrome/122.0.0.0 Safari/537.36"}, //nolint:lll // long user agent string
	},
	Metadata:  map[string]string{},
	Timestamp: time.Unix(0, 0).UTC(),
}

// testHitThree represents the third test hit from partition 4, request 1 (CID=ag7, SESSION_STAMP=127.0.0.1)
var testHitThree = &hits.Hit{
	ID:                    "test-hit-three",
	AuthoritativeClientID: "ag7",
	ClientID:              "ag7",
	PropertyID:            "G-5T0Z13HKP4",
	IP:                    "127.0.0.1",
	Host:                  "localhost.d8astage.xyz",
	ServerReceivedTime:    time.Now().Format(time.RFC3339),
	QueryParams: url.Values{
		"v":            []string{"2"},
		"tid":          []string{"G-5T0Z13HKP4"},
		"gtm":          []string{"45je5580h2v9219555710za200"},
		"_p":           []string{"1746817938582"},
		"gcd":          []string{"13l3l3l2l1l1"},
		"npa":          []string{"1"},
		"dma_cps":      []string{"syphamo"},
		"dma":          []string{"1"},
		"tag_exp":      []string{"101509157~103101750~103101752~103116026~103130495~103130497~103200004~103211513~103233427~103251618~103251620~103284320~103284322~103301114~103301116"},
		"cid":          []string{"ag7"},
		"ul":           []string{"en-us"},
		"sr":           []string{"1745x982"},
		"uaa":          []string{"x86"},
		"uab":          []string{"64"},
		"uafvl":        []string{"Not(A%253ABrand%3B24.0.0.0%7CChromium%3B122.0.6261.171"},
		"uamb":         []string{"0"},
		"uam":          []string{""},
		"uap":          []string{"Linux"},
		"uapv":         []string{"6.14.5"},
		"uaw":          []string{"0"},
		"frm":          []string{"0"},
		"pscdl":        []string{"noapi"},
		"_eu":          []string{"AAAAAAQ"},
		"_s":           []string{"1"},
		"sid":          []string{"1746817858"},
		"sct":          []string{"1"},
		"seg":          []string{"1"},
		"dl":           []string{"https%3A%2F%2Fd8a-tech.github.io%2Fanalytics-playground%2Findex.html"},
		"dr":           []string{"https%3A%2F%2Fd8a-tech.github.io%2Fanalytics-playground%2Fcheckout.html"},
		"dt":           []string{"Food%20Shop"},
		"en":           []string{"page_view"},
		"_ee":          []string{"1"},
		"tfd":          []string{"565"},
		"sessionStamp": []string{"127.0.0.1"},
	},
	Path:   "/g/collect",
	Method: "POST",
	Headers: url.Values{
		"authority":          []string{"region1.google-analytics.com"},
		"accept":             []string{"*/*"},
		"accept-language":    []string{"en-US,en;q=0.8"},
		"content-length":     []string{"0"},
		"origin":             []string{"https://d8a-tech.github.io"},
		"priority":           []string{"u=1, i"},
		"referer":            []string{"https://d8a-tech.github.io/"},
		"sec-ch-ua":          []string{`"Not(A:Brand";v="24", "Chromium";v="122"`},
		"sec-ch-ua-mobile":   []string{"?0"},
		"sec-ch-ua-platform": []string{`"Linux"`},
		"sec-fetch-dest":     []string{"empty"},
		"sec-fetch-mode":     []string{"no-cors"},
		"sec-fetch-site":     []string{"cross-site"},
		"user-agent":         []string{"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) QtWebEngine/6.8.3 Chrome/122.0.0.0 Safari/537.36"}, //nolint:lll // long user agent string
	},
	Metadata:  map[string]string{},
	Timestamp: time.Unix(0, 0).UTC(),
}

// testHitFour represents the fourth test hit from partition 1, request 2 (CID=ai7, SESSION_STAMP=127.0.0.3)
var testHitFour = &hits.Hit{
	ID:                    "test-hit-four",
	AuthoritativeClientID: "ai7",
	ClientID:              "ai7",
	PropertyID:            "G-5T0Z13HKP4",
	IP:                    "127.0.0.11", // From X-Forwarded-For header
	Host:                  "localhost.d8astage.xyz",
	ServerReceivedTime:    time.Now().Format(time.RFC3339),
	QueryParams: url.Values{
		"v":            []string{"2"},
		"tid":          []string{"G-5T0Z13HKP4"},
		"gtm":          []string{"45je5580h2v9219555710za200"},
		"_p":           []string{"1746817938582"},
		"gcd":          []string{"13l3l3l2l1l1"},
		"npa":          []string{"1"},
		"dma_cps":      []string{"syphamo"},
		"dma":          []string{"1"},
		"tag_exp":      []string{"101509157~103101750~103101752~103116026~103130495~103130497~103200004~103211513~103233427~103251618~103251620~103284320~103284322~103301114~103301116"},
		"cid":          []string{"ai7"},
		"ul":           []string{"en-us"},
		"sr":           []string{"1745x982"},
		"uaa":          []string{"x86"},
		"uab":          []string{"64"},
		"uafvl":        []string{"Not(A%253ABrand%3B24.0.0.0%7CChromium%3B122.0.6261.171"},
		"uamb":         []string{"0"},
		"uam":          []string{""},
		"uap":          []string{"Linux"},
		"uapv":         []string{"6.14.5"},
		"uaw":          []string{"0"},
		"frm":          []string{"0"},
		"pscdl":        []string{"noapi"},
		"_eu":          []string{"AAAAAAQ"},
		"_s":           []string{"3"},
		"sid":          []string{"1746817858"},
		"sct":          []string{"1"},
		"seg":          []string{"1"},
		"dl":           []string{"https%3A%2F%2Fd8a-tech.github.io%2Fanalytics-playground%2Findex.html"},
		"dr":           []string{"https%3A%2F%2Fd8a-tech.github.io%2Fanalytics-playground%2Fcheckout.html"},
		"dt":           []string{"Food%20Shop"},
		"en":           []string{"user_engagement"},
		"_et":          []string{"16002"},
		"tfd":          []string{"16582"},
		"sessionStamp": []string{"127.0.0.3"},
	},
	Path:   "/g/collect",
	Method: "POST",
	Headers: url.Values{
		"authority":          []string{"region1.google-analytics.com"},
		"accept":             []string{"*/*"},
		"accept-language":    []string{"en-US,en;q=0.8"},
		"content-length":     []string{"0"},
		"origin":             []string{"https://d8a-tech.github.io"},
		"priority":           []string{"u=1, i"},
		"referer":            []string{"https://d8a-tech.github.io/"},
		"sec-ch-ua":          []string{`"Not(A:Brand";v="24", "Chromium";v="122"`},
		"sec-ch-ua-mobile":   []string{"?0"},
		"sec-ch-ua-platform": []string{`"Linux"`},
		"sec-fetch-dest":     []string{"empty"},
		"X-Forwarded-For":    []string{"127.0.0.11"},
		"sec-fetch-mode":     []string{"no-cors"},
		"sec-fetch-site":     []string{"cross-site"},
		"user-agent":         []string{"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) QtWebEngine/6.8.3 Chrome/122.0.0.0 Safari/537.36"}, //nolint:lll // long user agent string
	},
	Metadata:  map[string]string{},
	Timestamp: time.Unix(0, 0).UTC(),
}

// TestHitOne returns a copy of the first test hit
func TestHitOne() *hits.Hit {
	copy := testHitOne.Copy()
	return &copy
}

// TestHitTwo returns a copy of the second test hit
func TestHitTwo() *hits.Hit {
	copy := testHitTwo.Copy()
	return &copy
}

// TestHitThree returns a copy of the third test hit
func TestHitThree() *hits.Hit {
	copy := testHitThree.Copy()
	return &copy
}

// TestHitFour returns a copy of the fourth test hit
func TestHitFour() *hits.Hit {
	copy := testHitFour.Copy()
	return &copy
}

type TestHits []*hits.Hit

func (t TestHits) EnsureQueryParam(hitNum int, param string, value string) {
	t[hitNum].QueryParams.Set(param, value)
}

type RequirememtnsFunc func(t *testing.T, hits TestHits)

func EnsureQueryParam(hitNum int, param string, value string) RequirememtnsFunc {
	return func(t *testing.T, hits TestHits) {
		hits.EnsureQueryParam(hitNum, param, value)
	}
}

func ColumnTestCase(
	t *testing.T,
	hits TestHits,
	expectations func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver),
	theProtocol protocol.Protocol,
	requirements ...RequirememtnsFunc,
) {
	for _, requirement := range requirements {
		requirement(t, hits)
	}
	warehouseDriver := &warehouse.MockWarehouseDriver{}
	closer := sessions.NewDirectCloser(
		sessions.NewSessionWriter(
			context.Background(),
			warehouse.NewStaticDriverRegistry(
				warehouseDriver,
			),
			columnset.DefaultColumnRegistry(theProtocol),
			schema.NewStaticLayoutRegistry(
				map[string]schema.Layout{},
				schema.NewEmbeddedSessionColumnsLayout(
					"events",
					"session_",
				),
			),
		),
		0,
	)

	// when
	err := closer.Close(hits)

	// then
	expectations(t, err, warehouseDriver)
}
