// Package e2e provides utilities for end-to-end testing
package e2e

import (
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/sirupsen/logrus"
)

// GA4RequestGenerator sends GA4 collect requests for testing purposes
type GA4RequestGenerator struct {
	host   string
	port   int
	client *http.Client
}

// NewGA4RequestGenerator creates a new GA4 request generator instance
func NewGA4RequestGenerator(host string, port int) *GA4RequestGenerator {
	return &GA4RequestGenerator{
		host:   host,
		port:   port,
		client: &http.Client{},
	}
}

// Hit sends a GA4 hit with the specified parameters
func (g *GA4RequestGenerator) Hit(clientID, eventType, sessionStamp string) error {
	baseURL := fmt.Sprintf("http://%s:%d/g/collect", g.host, g.port)

	params := g.buildParams(clientID, eventType, sessionStamp)

	req, err := http.NewRequest("POST", baseURL+"?"+params.Encode(), http.NoBody)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	g.addHeaders(req)

	resp, err := g.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			logrus.Error(err)
		}
	}()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("request failed with status: %d", resp.StatusCode)
	}

	return nil
}

func (g *GA4RequestGenerator) buildParams( // nolint:funlen // it's a test helper
	clientID, eventType, sessionStamp string,
) url.Values {
	params := url.Values{}

	// Base GA4 parameters
	params.Set("v", "2")
	params.Set("tid", "G-5T0Z13HKP4")
	params.Set("gtm", "45je5580h2v9219555710za200")
	params.Set("_p", "1766755958000")
	params.Set("gcd", "13l3l3l2l1l1")
	params.Set("npa", "1")
	params.Set("dma_cps", "syphamo")
	params.Set("dma", "1")
	params.Set("tag_exp", "101509157~103101750~103101752~103116026~103130495~103130497~"+
		"103200004~103211513~103233427~103251618~103251620~103284320~103284322~103301114~103301116")
	params.Set("cid", clientID)
	params.Set("ul", "en-us")
	params.Set("sr", "1745x982")
	params.Set("uaa", "x86")
	params.Set("uab", "64")
	params.Set("uafvl", "Not(A%3ABrand%3B24.0.0.0%7CChromium%3B122.0.6261.171")
	params.Set("uamb", "0")
	params.Set("uam", "")
	params.Set("uap", "Linux")
	params.Set("uapv", "6.14.4")
	params.Set("uaw", "0")
	params.Set("frm", "0")
	params.Set("pscdl", "noapi")
	params.Set("sid", "1746817858")
	params.Set("sct", "1")
	params.Set("seg", "1")
	params.Set("dl", "https://d8a-tech.github.io/analytics-playground/index.html")
	params.Set("dr", "https://d8a-tech.github.io/analytics-playground/checkout.html")
	params.Set("dt", "Food Shop")
	params.Set("sessionStamp", sessionStamp)

	// Set event-specific parameters
	switch eventType {
	case "page_view":
		params.Set("_eu", "AAAAAAQ")
		params.Set("_s", "1")
		params.Set("en", "page_view")
		params.Set("_ee", "1")
		params.Set("tfd", "565")
	case "scroll":
		params.Set("_eu", "AEAAAAQ")
		params.Set("_s", "2")
		params.Set("en", "scroll")
		params.Set("epn.percent_scrolled", "90")
		params.Set("_et", "10")
		params.Set("tfd", "5567")
	case "user_engagement":
		params.Set("_eu", "AAAAAAQ")
		params.Set("_s", "3")
		params.Set("en", "user_engagement")
		params.Set("_et", "16002")
		params.Set("tfd", "16582")
	}

	return params
}

func (g *GA4RequestGenerator) addHeaders(req *http.Request) {
	req.Header.Set("authority", "region1.google-analytics.com")
	req.Header.Set("accept", "*/*")
	req.Header.Set("accept-language", "en-US,en;q=0.8")
	req.Header.Set("content-length", "0")
	req.Header.Set("origin", "https://d8a-tech.github.io")
	req.Header.Set("priority", "u=1, i")
	req.Header.Set("referer", "https://d8a-tech.github.io/")
	req.Header.Set("sec-ch-ua", `"Not(A:Brand";v="24", "Chromium";v="122"`)
	req.Header.Set("sec-ch-ua-mobile", "?0")
	req.Header.Set("sec-ch-ua-platform", `"Linux"`)
	req.Header.Set("sec-fetch-dest", "empty")
	req.Header.Set("sec-fetch-mode", "no-cors")
	req.Header.Set("sec-fetch-site", "cross-site")
	req.Header.Set("user-agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "+
		"(KHTML, like Gecko) QtWebEngine/6.8.3 Chrome/122.0.0.0 Safari/537.36")
}

// HitSequenceItem represents a single hit in a sequence from the shell script
type HitSequenceItem struct {
	ClientID     string
	EventType    string
	SessionStamp string
	Description  string
	SleepBefore  time.Duration
}

// Replay sends a sequence of hits with optional delays
func (g *GA4RequestGenerator) Replay(sequence []HitSequenceItem) error {
	for _, hit := range sequence {
		time.Sleep(hit.SleepBefore)

		if err := g.Hit(hit.ClientID, hit.EventType, hit.SessionStamp); err != nil {
			return fmt.Errorf("failed to send hit %s: %w", hit.Description, err)
		}
	}

	return nil
}
