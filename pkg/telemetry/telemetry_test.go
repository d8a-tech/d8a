package telemetry

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestClientIDGeneratedOnStartup(t *testing.T) {
	// given
	clientIDOnce = sync.Once{}
	clientID = ""
	fn := ClientIDGeneratedOnStartup()

	// when
	cid1 := fn()
	cid2 := fn()

	// then
	assert.NotEmpty(t, cid1)
	assert.Len(t, cid1, 36)
	assert.Equal(t, cid1, cid2)
}

func TestNumberOfSecsSinceStarted(t *testing.T) {
	// given
	startTimeOnce = sync.Once{}
	startTime = 0
	pb := NumberOfSecsSinceStarted()

	// when
	val1, ok := pb.valueFunc().(int64)
	assert.True(t, ok)
	time.Sleep(1100 * time.Millisecond)
	val2, ok := pb.valueFunc().(int64)
	assert.True(t, ok)

	// then
	assert.GreaterOrEqual(t, val1, int64(0))
	assert.GreaterOrEqual(t, val2, val1)
}

func TestSendEvent(t *testing.T) {
	tests := []struct {
		name          string
		serverHandler http.HandlerFunc
		endpointURL   string
		eventName     string
		clientID      string
		params        map[string]any
		expectErr     bool
	}{
		{
			name: "sends correct GA4 format",
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				u, _ := url.Parse(r.URL.String())
				q := u.Query()
				assert.Equal(t, "2", q.Get("v"))
				assert.Equal(t, "1337", q.Get("tid"))
				assert.Equal(t, "test-cid-123", q.Get("cid"))
				assert.Equal(t, "app_running", q.Get("en"))
				assert.Equal(t, "1.0.0", q.Get("ep.app_version"))
				assert.Equal(t, "42", q.Get("epn.params_exposure_time"))
				w.WriteHeader(http.StatusOK)
			},
			eventName: "app_running",
			clientID:  "test-cid-123",
			params: map[string]any{
				"app_version":          "1.0.0",
				"params_exposure_time": int64(42),
			},
		},
		{
			name:        "returns error on invalid URL",
			endpointURL: "://invalid",
			eventName:   "app_running",
			clientID:    "test-cid",
			params:      map[string]any{},
			expectErr:   true,
		},
		{
			name: "returns error on non-2xx status",
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
			eventName: "app_running",
			clientID:  "test-cid",
			params:    map[string]any{},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			endpointURL := tt.endpointURL
			if tt.serverHandler != nil {
				server := httptest.NewServer(tt.serverHandler)
				defer server.Close()
				endpointURL = server.URL
			}

			// when
			err := sendEvent(endpointURL, tt.eventName, tt.clientID, tt.params)

			// then
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
