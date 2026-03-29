package receiver

import (
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/stretchr/testify/assert"
)

type noopValidatingRule struct{}

func (noopValidatingRule) Validate(_ protocol.Protocol, _ *hits.Hit) error { return nil }

func TestNewServer_DefaultTimeoutsAndConcurrency(t *testing.T) {
	// given / when
	s := NewServer(
		&mockStorage{},
		NewDummyRawLogStorage(),
		noopValidatingRule{},
		nil,
		8080,
	)

	// then
	assert.Equal(t, 5*time.Second, s.readTimeout)
	assert.Equal(t, 10*time.Second, s.writeTimeout)
	assert.Equal(t, 256*1024, s.maxConcurrency)
}

func TestNewServer_OptionOverrides(t *testing.T) {
	tests := []struct {
		name            string
		opts            []ServerOption
		wantRead        time.Duration
		wantWrite       time.Duration
		wantConcurrency int
	}{
		{
			name:            "override read timeout",
			opts:            []ServerOption{WithReadTimeout(15 * time.Second)},
			wantRead:        15 * time.Second,
			wantWrite:       defaultWriteTimeout,
			wantConcurrency: defaultMaxConcurrency,
		},
		{
			name:            "override write timeout",
			opts:            []ServerOption{WithWriteTimeout(30 * time.Second)},
			wantRead:        defaultReadTimeout,
			wantWrite:       30 * time.Second,
			wantConcurrency: defaultMaxConcurrency,
		},
		{
			name:            "override max concurrency",
			opts:            []ServerOption{WithMaxConcurrency(1000)},
			wantRead:        defaultReadTimeout,
			wantWrite:       defaultWriteTimeout,
			wantConcurrency: 1000,
		},
		{
			name: "override all",
			opts: []ServerOption{
				WithReadTimeout(3 * time.Second),
				WithWriteTimeout(7 * time.Second),
				WithMaxConcurrency(500),
			},
			wantRead:        3 * time.Second,
			wantWrite:       7 * time.Second,
			wantConcurrency: 500,
		},
		{
			name:            "zero disables timeout",
			opts:            []ServerOption{WithReadTimeout(0), WithWriteTimeout(0), WithMaxConcurrency(0)},
			wantRead:        0,
			wantWrite:       0,
			wantConcurrency: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given / when
			s := NewServer(
				&mockStorage{},
				NewDummyRawLogStorage(),
				noopValidatingRule{},
				nil,
				8080,
				tt.opts...,
			)

			// then
			assert.Equal(t, tt.wantRead, s.readTimeout)
			assert.Equal(t, tt.wantWrite, s.writeTimeout)
			assert.Equal(t, tt.wantConcurrency, s.maxConcurrency)
		})
	}
}
