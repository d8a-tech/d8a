package telemetry

import (
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	protocolVersion = "2"
	measurementID   = "1337"
	httpTimeout     = 10 * time.Second
)

var httpClient = &http.Client{
	Timeout: httpTimeout,
}

// sendEvent sends a GA4 event via POST to the provided URL endpoint.
func sendEvent(endpointURL, eventName, clientID string, params map[string]any) error {
	u, err := url.Parse(endpointURL)
	if err != nil {
		logrus.Debugf("telemetry: invalid endpoint URL: %v", err)
		return fmt.Errorf("invalid endpoint URL: %w", err)
	}

	q := u.Query()
	q.Set("v", protocolVersion)
	q.Set("tid", measurementID)
	q.Set("cid", clientID)
	q.Set("en", eventName)

	for key, value := range params {
		switch v := value.(type) {
		case string:
			q.Set(fmt.Sprintf("ep.%s", key), v)
		case int64:
			q.Set(fmt.Sprintf("epn.%s", key), fmt.Sprintf("%d", v))
		case int:
			q.Set(fmt.Sprintf("epn.%s", key), fmt.Sprintf("%d", v))
		case float64:
			q.Set(fmt.Sprintf("epn.%s", key), fmt.Sprintf("%.0f", v))
		default:
			q.Set(fmt.Sprintf("ep.%s", key), fmt.Sprintf("%v", v))
		}
	}

	u.RawQuery = q.Encode()

	resp, err := httpClient.Post(u.String(), "application/x-www-form-urlencoded", nil)
	if err != nil {
		logrus.Debugf("telemetry: failed to send event: %v", err)
		return fmt.Errorf("failed to send event: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			logrus.Debugf("telemetry: failed to close response body: %v", err)
		}
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logrus.Debugf("telemetry: unexpected status code: %d", resp.StatusCode)
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}
