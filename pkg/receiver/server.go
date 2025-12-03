package receiver

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/monitoring"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	// HitProtocolMetadataKey is the key used to store the protocol ID in the hit metadata
	HitProtocolMetadataKey string = "protocol"
)

var (
	requestCounter     metric.Int64Counter
	requestLatencyHist metric.Float64Histogram
)

func init() {
	meter := otel.GetMeterProvider().Meter("receiver")

	requestCounter, _ = meter.Int64Counter(
		"receiver.requests.total",
		metric.WithDescription("Total number of HTTP requests"),
	)
	requestLatencyHist, _ = meter.Float64Histogram(
		"receiver.request.latency",
		metric.WithDescription("HTTP request latency in seconds"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.MsBuckets...),
	)
}

func statusGroup(statusCode int) string {
	switch {
	case statusCode >= 200 && statusCode < 300:
		return "2xx"
	case statusCode >= 300 && statusCode < 400:
		return "3xx"
	case statusCode >= 400 && statusCode < 500:
		return "4xx"
	case statusCode >= 500:
		return "5xx"
	default:
		return "unknown"
	}
}

func handleRequest(
	ctx *fasthttp.RequestCtx,
	storage Storage,
	rawLogStorage RawLogStorage,
	selectedProtocol protocol.Protocol,
) {
	start := time.Now()
	path := string(ctx.Path())

	// Always set CORS headers
	ctx.Response.Header.Set("Access-Control-Allow-Origin", "*")
	ctx.Response.Header.Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
	ctx.Response.Header.Set("Access-Control-Allow-Headers", "Content-Type, X-Requested-With")
	ctx.Response.Header.Set("Access-Control-Max-Age", "86400")

	// Handle preflight requests early
	if string(ctx.Method()) == fasthttp.MethodOptions {
		ctx.SetStatusCode(fasthttp.StatusNoContent)
		recordRequestMetrics(path, fasthttp.StatusNoContent, start)
		return
	}

	// Log raw HTTP request
	b := bytes.NewBuffer(make([]byte, 0, 64*1024))
	if _, err := ctx.Request.WriteTo(b); err != nil {
		logrus.Errorf("Failed to write raw request: %v", err)
	}
	if err := rawLogStorage.Store(b); err != nil {
		logrus.Errorf("Failed to store raw log: %v", err)
	}
	var err error

	hits, err := createHits(ctx, selectedProtocol)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusBadRequest)
		recordRequestMetrics(path, fasthttp.StatusBadRequest, start)
		return
	}

	for _, hit := range hits {
		hit.Metadata[HitProtocolMetadataKey] = selectedProtocol.ID()
		hit.IP = realIP(ctx)
	}

	err = storage.Push(hits)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		recordRequestMetrics(path, fasthttp.StatusInternalServerError, start)
		return
	}

	// Successful request should return 204 No Content
	ctx.SetStatusCode(fasthttp.StatusNoContent)
	recordRequestMetrics(path, fasthttp.StatusNoContent, start)
}

func recordRequestMetrics(path string, statusCode int, start time.Time) {
	statusGroup := statusGroup(statusCode)
	duration := time.Since(start).Seconds()

	requestCounter.Add(context.Background(), 1,
		metric.WithAttributes(
			attribute.String("path", path),
			attribute.String("status_group", statusGroup),
		))
	requestLatencyHist.Record(context.Background(), duration,
		metric.WithAttributes(
			attribute.String("path", path),
		))
}

var hitValidatingRuleSet = HitValidatingRuleSet()

func createHits(ctx *fasthttp.RequestCtx, p protocol.Protocol) ([]*hits.Hit, error) {
	queryParams := map[string][]string{}
	for key, value := range ctx.QueryArgs().All() {
		queryParams[string(key)] = append(queryParams[string(key)], string(value))
	}
	headers := map[string][]string{}
	for key, value := range ctx.Request.Header.All() {
		headers[string(key)] = append(headers[string(key)], string(value))
	}
	request := &protocol.Request{
		Method:      ctx.Method(),
		Host:        ctx.Host(),
		Path:        ctx.Path(),
		QueryParams: queryParams,
		Headers:     headers,
		Body:        bytes.NewBuffer(ctx.Request.Body()),
	}

	hits, err := p.Hits(request)
	if err != nil {
		return nil, err
	}

	for _, hit := range hits {
		if err := hitValidatingRuleSet.Validate(hit); err != nil {
			return nil, err
		}
	}

	return hits, nil
}

// Serve starts the HTTP server with the given storage backend and port
func Serve(
	ctx context.Context,
	storage Storage,
	rawLogStorage RawLogStorage,
	port int,
	protocols protocol.PathProtocolMapping,
	otherHandlers map[string]func(fctx *fasthttp.RequestCtx),
) error {
	s := &fasthttp.Server{
		Handler: func(fctx *fasthttp.RequestCtx) { // nolint:contextcheck // fasthttp implements context.Context
			start := time.Now()
			path := string(fctx.Path())
			var selectedProtocol protocol.Protocol
			for pathPrefix, protocol := range protocols {
				if strings.HasPrefix(path, pathPrefix) {
					selectedProtocol = protocol
					break
				}
			}
			if selectedProtocol != nil {
				handleRequest(fctx, storage, rawLogStorage, selectedProtocol)
				return
			}
			for pathPrefix, handler := range otherHandlers {
				if strings.HasPrefix(path, pathPrefix) {
					handler(fctx)
					recordRequestMetrics(path, fctx.Response.StatusCode(), start)
					return
				}
			}
			fctx.SetStatusCode(fasthttp.StatusNotFound)
			recordRequestMetrics(path, fasthttp.StatusNotFound, start)
		},
		Name: "Tracker API",
	}
	// Create a channel to signal server shutdown
	shutdownChan := make(chan struct{})
	go func() {
		// This whole abomination is because fasthttp can sometimes not shutdown
		// and therefore block whole program from exiting
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		shutdownDone := make(chan struct{})
		go func() {
			if err := s.Shutdown(); err != nil {
				fmt.Printf("Error shutting down server: %v\n", err)
			}
			close(shutdownDone)
		}()
		select {
		case <-shutdownDone:
			// Normal shutdown completed
		case <-shutdownCtx.Done():
			// Shutdown timed out
			fmt.Println("Server shutdown timed out after 2 seconds")
		}
		close(shutdownChan)
	}()
	// Start the server in a goroutine
	errChan := make(chan error, 1)
	go func() {
		logrus.Infof("Starting server on port %d", port)
		if err := s.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", port)); err != nil {
			errChan <- err
		}
	}()
	// Wait for either context cancellation or server error
	select {
	case err := <-errChan:
		return err
	case <-shutdownChan:
		return nil
	}
}
