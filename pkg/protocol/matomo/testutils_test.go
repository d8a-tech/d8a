package matomo

import (
	"net/http"
	"net/url"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/protocol"
)

// testMatomoHitOne is a minimal Matomo-protocol page_view hit for use in Matomo column tests.
var testMatomoHitOne = &hits.Hit{
	ID:                    "test-matomo-hit-one",
	AuthoritativeClientID: "abc123",
	ClientID:              "abc123",
	PropertyID:            "42",
	EventName:             "page_view",
	Request: &hits.ParsedRequest{
		IP:                 "127.0.0.1",
		Host:               "example.com",
		ServerReceivedTime: time.Now(),
		QueryParams: url.Values{
			"idsite":      []string{"42"},
			"rec":         []string{"1"},
			"url":         []string{"https://example.com/"},
			"action_name": []string{"Home"},
			"_id":         []string{"abc123"},
		},
		Path:    "/matomo.php",
		Method:  "GET",
		Headers: http.Header{},
	},
	Metadata: map[string]string{},
}

func testHitOne() *hits.Hit {
	hitCopy := testMatomoHitOne.Copy()
	return &hitCopy
}

type staticPropertyIDExtractor struct {
	propertyID string
}

func (e *staticPropertyIDExtractor) PropertyID(_ *protocol.RequestContext) (string, error) {
	return e.propertyID, nil
}
