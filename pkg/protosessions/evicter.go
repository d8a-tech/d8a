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

	return ctx.TriggerCleanup(allHits)
}

func (m *evicterMiddleware) OnCleanup(_ *Context, allCleanedHits []*hits.Hit) error {
	if len(allCleanedHits) == 0 {
		return nil
	}
	sessionStamp := allCleanedHits[0].SessionStamp()
	if len(sessionStamp) > 0 {
		err := m.kv.Delete([]byte(SessionStampKey(string(sessionStamp))))
		if err != nil {
			return fmt.Errorf("failed to delete session stamp: %w", err)
		}
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

// SessionStampByClientIDPrefix is the prefix for session stamps by client ID keys.
const SessionStampByClientIDPrefix = "sessions.stamps.by.client.id"

// SessionStampByClientIDKey returns the key for the session stamp by client ID.
// func SessionStampByClientIDKey(clientID string) string {
// 	return fmt.Sprintf("%s.%s", SessionStampByClientIDPrefix, clientID)
// }
