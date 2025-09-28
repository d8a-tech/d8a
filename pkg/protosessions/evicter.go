package protosessions

import (
	"fmt"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/receiver"
	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/sirupsen/logrus"
)

type evicterMiddleware struct {
	kv storage.KV
	rs receiver.Storage
}

// NewEvicterMiddleware creates a new evicter middleware instance.
func NewEvicterMiddleware(kv storage.KV, rs receiver.Storage) Middleware {
	return &evicterMiddleware{kv: kv, rs: rs}
}

func (m *evicterMiddleware) Handle(ctx *Context, hit *hits.Hit, next func() error) error {
	savedAuthoritativeClientIDBytes, err := m.kv.Set(
		[]byte(SessionStampKey(hit.SessionStamp())),
		[]byte(hit.AuthoritativeClientID),
		storage.WithSkipIfKeyAlreadyExists(true),
		storage.WithReturnPreviousValue(true),
	)
	if err != nil {
		return fmt.Errorf("failed to set session stamp: %w", err)
	}

	_, err = m.kv.Set(
		[]byte(SessionStampByClientIDKey(string(hit.AuthoritativeClientID))),
		[]byte(hit.SessionStamp()),
		storage.WithSkipIfKeyAlreadyExists(false),
		storage.WithReturnPreviousValue(false),
	)
	if err != nil {
		return fmt.Errorf("failed to set session stamp by client ID: %w", err)
	}

	if savedAuthoritativeClientIDBytes == nil || hits.ClientID(
		savedAuthoritativeClientIDBytes,
	) == hit.AuthoritativeClientID {
		return next()
	}

	savedAuthoritativeClientID := hits.ClientID(savedAuthoritativeClientIDBytes)

	// Eviction happens here
	logrus.Infof(
		"Evicting protosession: %v to %v",
		hit.AuthoritativeClientID,
		savedAuthoritativeClientID,
	)

	oldAuthoritativeClientID := hit.AuthoritativeClientID
	allHits, err := ctx.CollectAll(hit.AuthoritativeClientID)
	if err != nil {
		return fmt.Errorf("failed to collect all hits: %w", err)
	}

	allHits = append(allHits, hit)

	for i := range allHits {
		allHits[i].AuthoritativeClientID = savedAuthoritativeClientID
	}

	err = m.rs.Push(allHits)
	if err != nil {
		return fmt.Errorf("failed to push hits: %w", err)
	}

	// Session stamp will be cleaned up by the authoritative client ID when it closes
	err = m.kv.Delete([]byte(SessionStampByClientIDKey(string(oldAuthoritativeClientID))))
	if err != nil {
		return fmt.Errorf("failed to delete session stamp by client ID: %w", err)
	}

	return ctx.TriggerCleanup(oldAuthoritativeClientID)
}

// SessionStampByClientIDPrefix is the prefix for session stamps by client ID keys.
const SessionStampByClientIDPrefix = "sessions.stamps.by.client.id"

func (m *evicterMiddleware) OnCleanup(_ *Context, authoritativeClientID hits.ClientID) error {
	sessionStamp, err := m.kv.Get(
		[]byte(SessionStampByClientIDKey(string(authoritativeClientID))),
	)
	if err != nil {
		return fmt.Errorf("failed to get session stamp by client ID: %w", err)
	}
	if len(sessionStamp) > 0 {
		err = m.kv.Delete([]byte(SessionStampKey(string(sessionStamp))))
		if err != nil {
			return fmt.Errorf("failed to delete session stamp: %w", err)
		}
	}
	err = m.kv.Delete([]byte(SessionStampByClientIDKey(string(authoritativeClientID))))
	if err != nil {
		return fmt.Errorf("failed to delete session stamp: %w", err)
	}
	return nil
}

func (m *evicterMiddleware) OnCollect(_ *Context, _ hits.ClientID) ([]*hits.Hit, error) {
	return nil, nil
}

func (m *evicterMiddleware) OnPing(_ *Context, _ time.Time) error {
	return nil
}

// SessionStampPrefix is the prefix for session stamps keys.
const SessionStampPrefix = "sessions.stamps"

// SessionStampKey returns the key for the session stamp.
func SessionStampKey(sessionStamp string) string {
	return fmt.Sprintf("%s.%s", SessionStampPrefix, sessionStamp)
}

// SessionStampByClientIDKey returns the key for the session stamp by client ID.
func SessionStampByClientIDKey(clientID string) string {
	return fmt.Sprintf("%s.%s", SessionStampByClientIDPrefix, clientID)
}
