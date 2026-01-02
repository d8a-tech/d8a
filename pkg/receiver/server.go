package receiver

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/monitoring"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/fasthttp/router"
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

// Server holds all server-related dependencies and configuration
type Server struct {
	protocols       []protocol.Protocol
	storage         Storage
	rawLogStorage   RawLogStorage
	validationRules HitValidatingRule
	port            int
}

// NewServer creates a new Server instance with the provided dependencies
func NewServer(
	storage Storage,
	rawLogStorage RawLogStorage,
	validationRules HitValidatingRule,
	protocols []protocol.Protocol,
	port int,
) *Server {
	return &Server{
		protocols:       protocols,
		storage:         storage,
		rawLogStorage:   rawLogStorage,
		validationRules: validationRules,
		port:            port,
	}
}

func (s *Server) handleRequest(
	reqCtx context.Context,
	ctx *fasthttp.RequestCtx,
	selectedProtocol protocol.Protocol,
) {
	start := time.Now()

	// Always set CORS headers
	ctx.Response.Header.Set("Access-Control-Allow-Origin", "*")
	ctx.Response.Header.Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
	ctx.Response.Header.Set("Access-Control-Allow-Headers", "Content-Type, X-Requested-With")
	ctx.Response.Header.Set("Access-Control-Max-Age", "86400")

	// Handle preflight requests early
	if string(ctx.Method()) == fasthttp.MethodOptions {
		ctx.SetStatusCode(fasthttp.StatusNoContent)
		recordRequestMetrics(reqCtx, fasthttp.StatusNoContent, start)
		return
	}

	// Log raw HTTP request
	b := bytes.NewBuffer(make([]byte, 0, 64*1024))
	if _, err := ctx.Request.WriteTo(b); err != nil {
		logrus.Errorf("Failed to write raw request: %v", err)
	}
	if err := s.rawLogStorage.Store(b); err != nil {
		logrus.Errorf("Failed to store raw log: %v", err)
	}
	var err error

	hits, err := s.createHits(ctx, selectedProtocol)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusBadRequest)
		recordRequestMetrics(reqCtx, fasthttp.StatusBadRequest, start)
		return
	}

	for _, hit := range hits {
		if hit.Request == nil {
			ctx.Error(fmt.Sprintf("server attributes are nil for hit %s", hit.ID), fasthttp.StatusInternalServerError)
			recordRequestMetrics(reqCtx, fasthttp.StatusInternalServerError, start)
			return
		}
		hit.Metadata[HitProtocolMetadataKey] = selectedProtocol.ID()
	}
	err = s.storage.Push(hits)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		recordRequestMetrics(reqCtx, fasthttp.StatusInternalServerError, start)
		return
	}

	// Successful request should return 204 No Content
	ctx.SetStatusCode(fasthttp.StatusNoContent)
	recordRequestMetrics(reqCtx, fasthttp.StatusNoContent, start)
}

func recordRequestMetrics(ctx context.Context, statusCode int, start time.Time) {
	statusGroup := statusGroup(statusCode)
	duration := time.Since(start).Seconds()

	requestCounter.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("status_group", statusGroup),
		))
	requestLatencyHist.Record(ctx, duration)
}

func (s *Server) createHits(ctx *fasthttp.RequestCtx, p protocol.Protocol) ([]*hits.Hit, error) {
	queryParams := map[string][]string{}
	for key, value := range ctx.QueryArgs().All() {
		queryParams[string(key)] = append(queryParams[string(key)], string(value))
	}
	headers := map[string][]string{}
	for key, value := range ctx.Request.Header.All() {
		headers[string(key)] = append(headers[string(key)], string(value))
	}
	bodyCopy := make([]byte, len(ctx.Request.Body()))
	copy(bodyCopy, ctx.Request.Body())
	request := &hits.Request{
		IP:                 realIP(ctx),
		Method:             string(ctx.Method()),
		Host:               string(ctx.Host()),
		Path:               string(ctx.Path()),
		ServerReceivedTime: time.Now(),
		QueryParams:        queryParams,
		Headers:            headers,
		Body:               bodyCopy,
	}

	hits, err := p.Hits(request)
	if err != nil {
		return nil, err
	}

	for _, hit := range hits {
		if err := s.validationRules.Validate(hit); err != nil {
			return nil, err
		}
	}

	return hits, nil
}

// Run starts the HTTP server and blocks until the context is cancelled or an error occurs
func (s *Server) Run(ctx context.Context) error {
	r := router.New()

	for _, protocol := range s.protocols {
		for _, endpoint := range protocol.Endpoints() {
			if endpoint.IsCustom {
				for _, method := range endpoint.Methods {
					r.Handle(method, endpoint.Path, func(fctx *fasthttp.RequestCtx) {
						start := time.Now()
						defer recordRequestMetrics(ctx, fctx.Response.StatusCode(), start)
						endpoint.CustomHandler(fctx)
					})
				}
				continue
			}
			for _, method := range endpoint.Methods {
				r.Handle(method, endpoint.Path, func(fctx *fasthttp.RequestCtx) {
					start := time.Now()
					defer recordRequestMetrics(ctx, fctx.Response.StatusCode(), start)
					s.handleRequest(ctx, fctx, protocol)
				})
			}
		}
	}

	r.GET("/healthz", func(fctx *fasthttp.RequestCtx) {
		fctx.SetStatusCode(fasthttp.StatusOK)
		fctx.SetBodyString("OK")
	})

	httpServer := &fasthttp.Server{
		Handler: r.Handler,
		Name:    "Tracker API",
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
			if err := httpServer.Shutdown(); err != nil {
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
		logrus.Infof("Starting server on port %d", s.port)
		if err := httpServer.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", s.port)); err != nil {
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
