package ga4

import (
	"net/http"
	"net/url"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
)

const defaultTestMeasurementID = "G-TESTMEASUREMENTID"

func EnsureValidTestHit(hit *hits.Hit) {
	if hit.Request == nil {
		hit.Request = &hits.ParsedRequest{
			Headers:            http.Header{},
			QueryParams:        url.Values{},
			ServerReceivedTime: time.Now(),
		}
	}
	if hit.Request.Headers == nil {
		hit.Request.Headers = http.Header{}
	}
	if hit.Request.QueryParams == nil {
		hit.Request.QueryParams = url.Values{}
	}
	if hit.Request.QueryParams.Get("tid") == "" {
		hit.Request.QueryParams.Set("tid", defaultTestMeasurementID)
	}
}
