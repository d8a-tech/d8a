package protosessions

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
)

const (
	// fss -> ForceSessionStamp
	clientProvidedSessionStampQueryParam = "fss"
)

var defaultSessionStampHeaders = []string{
	"Accept",
	"Accept-Encoding",
	"Accept-Language",
	"User-Agent",
	"Sec-CH-UA",
	"Sec-CH-UA-Mobile",
	"Sec-CH-UA-Platform",
	"Sec-CH-UA-Platform-Version",
	"Sec-CH-UA-Model",
	"Sec-CH-UA-Full-Version",
}

type IdentifierIsolationGuardFactory interface {
	New(settings *properties.Settings) IdentifierIsolationGuard
}

type IdentifierIsolationGuard interface {
	IsolatedClientID(hit *hits.Hit) hits.ClientID
	IsolatedSessionStamp(hit *hits.Hit) string
	IsolatedUserID(hit *hits.Hit) string
}

type defaultIdentifierIsolationGuard struct {
	skipPropertyID    bool
	calculatedHeaders []string
}

func (g *defaultIdentifierIsolationGuard) IsolatedClientID(hit *hits.Hit) hits.ClientID {
	return hits.ClientID(sha256Hex(hit.PropertyID + "|" + string(hit.AuthoritativeClientID)))
}

func (g *defaultIdentifierIsolationGuard) IsolatedSessionStamp(hit *hits.Hit) string {
	return calculateSessionStamp(hit, g.calculatedHeaders, g.skipPropertyID)
}

func (g *defaultIdentifierIsolationGuard) IsolatedUserID(hit *hits.Hit) string {
	if hit.UserID == nil {
		return ""
	}
	return sha256Hex(hit.PropertyID + "|" + *hit.UserID)
}

type defaultIdentifierIsolationFactory struct{}

func (f *defaultIdentifierIsolationFactory) New(settings *properties.Settings) IdentifierIsolationGuard {
	return &defaultIdentifierIsolationGuard{
		calculatedHeaders: defaultSessionStampHeaders,
	}
}

func NewDefaultIdentifierIsolationGuardFactory() IdentifierIsolationGuardFactory {
	return &defaultIdentifierIsolationFactory{}
}

func calculateSessionStamp(
	hit *hits.Hit,
	calculatedHeaders []string,
	skipPropertyID bool,
) string {
	req := hit.MustParsedRequest()

	// Explicitly provided session stamps have priority over calculated values.
	// The `d8aSessionStamp` variant is reserved mostly for testing and debugging.
	clientProvidedSessionStamp := req.QueryParams.Get(clientProvidedSessionStampQueryParam)
	if clientProvidedSessionStamp != "" {
		if skipPropertyID {
			return sha256Hex(clientProvidedSessionStampQueryParam + "=" + clientProvidedSessionStamp)
		}
		return sha256Hex(clientProvidedSessionStampQueryParam + "=" + clientProvidedSessionStamp + "|" + hit.PropertyID)
	}

	// Hash calculated stamp to:
	// - keep output fixed-length
	buf := make([]byte, 0, 256)
	for i, header := range calculatedHeaders {
		if i > 0 {
			buf = append(buf, '|')
		}
		buf = append(buf, req.Headers.Get(header)...)
	}
	if len(calculatedHeaders) > 0 {
		buf = append(buf, '|')
	}
	buf = append(buf, req.IP...)
	if !skipPropertyID {
		buf = append(buf, '|')
		buf = append(buf, hit.PropertyID...)
	}
	// Append hit's day date (YYYY-MM-DD) to be more respectful
	// of the user's privacy. Uses the hit's ServerReceivedTime to ensure
	// deterministic stamping regardless of when processing occurs.
	buf = append(buf, '|')
	buf = append(buf, req.ServerReceivedTime.UTC().Format("2006-01-02")...)
	return sha256HexBytes(buf)
}

func sha256Hex(s string) string {
	// Avoid per-part allocations by hashing as bytes once.
	return sha256HexBytes([]byte(s))
}

func sha256HexBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// DANGER ZONE - no isolation entities, have real usage in some cases, but are very dangerous
// to use anywhere near production.
type noIsolationFactory struct{}

func (f *noIsolationFactory) New(settings *properties.Settings) IdentifierIsolationGuard {
	return NewNoIsolationGuard()
}

// NewNoIsolationGuardFactory creates a new no isolation guard factory - DANGER - mixes
// data from different properties in the same proto-session!
func NewNoIsolationGuardFactory() IdentifierIsolationGuardFactory {
	return &noIsolationFactory{}
}

// noIsolationGuard is a guard that doesn't apply any isolation - returns IDs as-is
type noIsolationGuard struct {
	skipPropertyID    bool
	calculatedHeaders []string
}

func (g *noIsolationGuard) IsolatedClientID(hit *hits.Hit) hits.ClientID {
	return hit.AuthoritativeClientID
}

func (g *noIsolationGuard) IsolatedSessionStamp(hit *hits.Hit) string {
	return calculateSessionStamp(hit, g.calculatedHeaders, g.skipPropertyID)
}

func (g *noIsolationGuard) IsolatedUserID(hit *hits.Hit) string {
	if hit.UserID == nil {
		return ""
	}
	if g.skipPropertyID {
		return *hit.UserID
	}
	return sha256Hex(hit.PropertyID + "|" + *hit.UserID)
}

func NewNoIsolationGuard() IdentifierIsolationGuard {
	return &noIsolationGuard{
		skipPropertyID:    true,
		calculatedHeaders: defaultSessionStampHeaders,
	}
}
