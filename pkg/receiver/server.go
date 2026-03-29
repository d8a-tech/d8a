package receiver

import (
	"context"
	"fmt"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"
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
	hitCounter         metric.Int64Counter
)

const httpsOnHTTPListenerMessage = "received likely HTTPS/TLS traffic on the HTTP listener; " +
	"D8A does not terminate TLS, put a reverse proxy in front"

func newFastHTTPServerLogger(logger *logrus.Logger) *fasthttpServerLogger {
	if logger == nil {
		logger = logrus.StandardLogger()
	}

	return &fasthttpServerLogger{logger: logger}
}

type fasthttpServerLogger struct {
	logger *logrus.Logger
}

func (l *fasthttpServerLogger) Printf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if isLikelyHTTPSOnHTTPLog(msg) {
		l.logger.Warn(httpsOnHTTPListenerMessage)
		return
	}

	l.logger.Error(msg)
}

func isLikelyHTTPSOnHTTPLog(msg string) bool {
	return strings.Contains(msg, "error when serving connection") &&
		strings.Contains(msg, "error when reading request headers") &&
		strings.Contains(msg, "unsupported http request method")
}

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
	hitCounter, _ = meter.Int64Counter(
		"receiver.hits.total",
		metric.WithDescription("Total number of hits (as each request may contain multiple hits)"),
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

func extractTrackingLibrary(queryParams url.Values) string {
	dtn := queryParams.Get("_dtn")
	dtv := queryParams.Get("_dtv")
	if dtn == "" || dtv == "" {
		return "unknown"
	}
	return dtn + "@" + dtv
}

// Server holds all server-related dependencies and configuration
type Server struct {
	protocols       []protocol.Protocol
	storage         Storage
	rawLogStorage   RawLogStorage
	validationRules HitValidatingRule
	host            string
	port            int
	proxyTrust      proxyTrust
}

func WithHost(host string) ServerOption {
	return func(s *Server) {
		s.host = host
	}
}

type ServerOption func(*Server)

// NewServer creates a new Server instance with the provided dependencies
func NewServer(
	storage Storage,
	rawLogStorage RawLogStorage,
	validationRules HitValidatingRule,
	protocols []protocol.Protocol,
	port int,
	opts ...ServerOption,
) *Server {
	s := &Server{
		protocols:       protocols,
		storage:         storage,
		rawLogStorage:   rawLogStorage,
		validationRules: validationRules,
		host:            "0.0.0.0",
		port:            port,
		proxyTrust:      noProxyTrust{},
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

func (s *Server) handleRequest(
	reqCtx context.Context,
	ctx *fasthttp.RequestCtx,
	selectedProtocol protocol.Protocol,
) {
	var err error

	hits, err := s.createHits(ctx, selectedProtocol)
	if err != nil {
		logrus.WithError(err).Warn("failed to create hits from request")
		ctx.Error("Bad Request", fasthttp.StatusBadRequest)
		return
	}

	for _, hit := range hits {
		if hit.Request == nil {
			logrus.Errorf("server attributes are nil for hit %s", hit.ID)
			ctx.Error("Internal Server Error", fasthttp.StatusInternalServerError)
			return
		}
		hit.Metadata[HitProtocolMetadataKey] = selectedProtocol.ID()

		hitCounter.Add(reqCtx, 1,
			metric.WithAttributes(
				attribute.String("tracking_library", extractTrackingLibrary(hit.Request.QueryParams)),
			))
	}
	err = s.storage.Push(hits)
	if err != nil {
		logrus.WithError(err).Error("failed to push hits to storage")
		ctx.Error("Internal Server Error", fasthttp.StatusInternalServerError)
		return
	}

	// Successful request should return 204 No Content
	ctx.SetStatusCode(fasthttp.StatusNoContent)
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
	queryParams := url.Values{}
	for key, value := range ctx.QueryArgs().All() {
		queryParams.Add(string(key), string(value))
	}
	headers := http.Header{}
	for key, value := range ctx.Request.Header.All() {
		// Canonicalize header keys inline to ensure consistent access (e.g., "User-Agent" vs "user-agent")
		canonicalKey := textproto.CanonicalMIMEHeaderKey(string(key))
		headers.Add(canonicalKey, string(value))
	}
	bodyCopy := make([]byte, len(ctx.Request.Body()))
	copy(bodyCopy, ctx.Request.Body())
	request := &hits.ParsedRequest{
		IP:                 s.realIP(ctx),
		Method:             string(ctx.Method()),
		Host:               string(ctx.Host()),
		Path:               string(ctx.Path()),
		ServerReceivedTime: time.Now(),
		QueryParams:        queryParams,
		Headers:            headers,
		Body:               bodyCopy,
	}

	if err := s.rawLogStorage.Store(request); err != nil {
		logrus.Errorf("failed to store raw log: %v", err)
	}

	hits, err := p.Hits(ctx, request)
	if err != nil {
		return nil, err
	}

	for _, hit := range hits {
		if err := s.validationRules.Validate(p, hit); err != nil {
			return nil, err
		}
	}

	return hits, nil
}

// Run starts the HTTP server and blocks until the context is cancelled or an error occurs
func (s *Server) Run(ctx context.Context) error {
	httpServer := &fasthttp.Server{
		Handler:               s.setupRouter(ctx).Handler,
		Logger:                newFastHTTPServerLogger(logrus.StandardLogger()),
		Name:                  "Tracker API",
		SecureErrorLogMessage: true,
	}
	// Create a channel to signal server shutdown
	shutdownChan := make(chan struct{})
	go func() { //nolint:gosec // shutdown must use a fresh context independent from request lifecycle
		// This whole abomination is because fasthttp can sometimes not shutdown
		// and therefore block whole program from exiting
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			2*time.Second,
		)
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
		logrus.Infof("starting server on %s:%d", s.host, s.port)
		if err := httpServer.ListenAndServe(fmt.Sprintf("%s:%d", s.host, s.port)); err != nil {
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

func (s *Server) setupRouter(ctx context.Context) *router.Router {
	r := router.New()
	for _, protocol := range s.protocols {
		for _, endpoint := range protocol.Endpoints() {
			if endpoint.IsCustom {
				for _, method := range endpoint.Methods {
					logrus.WithFields(logrus.Fields{
						"path": endpoint.Path, "method": method, "protocol": protocol.ID(),
					}).Debug("registering endpoint")
					r.Handle(method, endpoint.Path, func(fctx *fasthttp.RequestCtx) {
						start := time.Now()
						defer func() {
							recordRequestMetrics(ctx, fctx.Response.StatusCode(), start)
						}()
						endpoint.CustomHandler(fctx)
					})
				}
				continue
			}
			for _, method := range endpoint.Methods {
				logrus.WithFields(logrus.Fields{
					"path": endpoint.Path, "method": method, "protocol": protocol.ID(),
				}).Debug("registering endpoint")
				r.Handle(method, endpoint.Path, func(fctx *fasthttp.RequestCtx) {
					start := time.Now()
					defer func() {
						recordRequestMetrics(ctx, fctx.Response.StatusCode(), start)
					}()
					s.handleRequest(ctx, fctx, protocol)
				})
			}
		}
	}
	for _, method := range []string{fasthttp.MethodGet, fasthttp.MethodOptions, fasthttp.MethodHead} {
		r.Handle(method, "/healthz", func(fctx *fasthttp.RequestCtx) {
			fctx.SetStatusCode(fasthttp.StatusOK)
			fctx.SetBodyString("OK")
		})
	}
	return r
}
