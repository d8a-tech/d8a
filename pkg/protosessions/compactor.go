package protosessions

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/sirupsen/logrus"
)

type compactorMiddleware struct {
	kv             storage.KV
	size           map[hits.ClientID]uint32
	thresholdBytes uint32
	encoder        encoding.EncoderFunc
	decoder        encoding.DecoderFunc
	mu             sync.RWMutex
}

// NewCompactorMiddleware creates a new middleware, that compresses bigger proto-sessions
func NewCompactorMiddleware(
	kv storage.KV,
	encoder encoding.EncoderFunc,
	decoder encoding.DecoderFunc,
	thresholdBytes uint32,
) Middleware {
	return &compactorMiddleware{
		kv:             kv,
		encoder:        encoder,
		decoder:        decoder,
		thresholdBytes: thresholdBytes,
		size:           make(map[hits.ClientID]uint32),
		mu:             sync.RWMutex{},
	}
}

func (m *compactorMiddleware) Handle(ctx *Context, hit *hits.Hit, next func() error) error {
	err := next()
	if err != nil {
		return err
	}

	currentSize := m.addSize(hit.AuthoritativeClientID, hit.Size())
	if currentSize < m.thresholdBytes {
		return nil
	}

	allHits, err := ctx.CollectAll(hit.AuthoritativeClientID)
	if err != nil {
		return fmt.Errorf("failed to collect all hits: %w", err)
	}

	if len(allHits) == 0 {
		logrus.Warnf(
			"the compactor threshold %d bytes reached, but no hits found for proto-session %s",
			m.thresholdBytes,
			hit.AuthoritativeClientID,
		)
		return nil
	}

	totalSize := uint32(0)
	for _, hit := range allHits {
		totalSize += hit.Size()
	}

	b := bytes.NewBuffer(nil)
	_, err = m.encoder(b, allHits)
	if err != nil {
		return fmt.Errorf("failed to encode hits: %w", err)
	}

	_, err = m.kv.Set([]byte(CompactedHitsKey(hit.AuthoritativeClientID)), b.Bytes())
	if err != nil {
		return fmt.Errorf("failed to add compacted hits: %w", err)
	}

	err = ctx.StorageSet.Delete([]byte(ProtoSessionHitsKey(hit.AuthoritativeClientID)))
	if err != nil {
		return fmt.Errorf("failed to delete uncompressed proto-session hits: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.size[hit.AuthoritativeClientID] = 0

	logrus.Infof("compacted proto-session from %d bytes to %d bytes", totalSize, b.Len())
	return nil
}

func (m *compactorMiddleware) OnCleanup(_ *Context, authoritativeClientID hits.ClientID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	err := m.kv.Delete([]byte(CompactedHitsKey(authoritativeClientID)))
	if err != nil {
		return fmt.Errorf("failed to delete compacted hits: %w", err)
	}

	delete(m.size, authoritativeClientID)
	return nil
}

func (m *compactorMiddleware) addSize(authoritativeClientID hits.ClientID, size uint32) uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.size[authoritativeClientID] += size
	return m.size[authoritativeClientID]
}

func (m *compactorMiddleware) OnCollect(_ *Context, authoritativeClientID hits.ClientID) ([]*hits.Hit, error) {
	compactedHits, err := m.kv.Get([]byte(CompactedHitsKey(authoritativeClientID)))
	if err != nil {
		return nil, fmt.Errorf("failed to get compacted hits: %w", err)
	}
	if compactedHits == nil {
		return []*hits.Hit{}, nil
	}
	var allHits []*hits.Hit
	err = m.decoder(bytes.NewReader(compactedHits), &allHits)
	if err != nil {
		return nil, fmt.Errorf("failed to decode compacted hits: %w", err)
	}
	return allHits, nil
}

func (m *compactorMiddleware) OnPing(_ *Context, _ time.Time) error {
	return nil
}

// CompactedHitsPrefix is the prefix for the compacted hits key
const CompactedHitsPrefix = "sessions.hits.compacted"

// CompactedHitsKey returns the key for the compacted hits
func CompactedHitsKey(authoritativeClientID hits.ClientID) string {
	return fmt.Sprintf("%s.%s", CompactedHitsPrefix, authoritativeClientID)
}
