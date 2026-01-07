package protosessions

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/stretchr/testify/assert"
)

func TestDefaultIdentifierIsolationGuard_IsolatedSessionStamp(t *testing.T) {
	t.Parallel()

	const ip = "1.2.3.4"
	const propertyID = "GA-12345"
	fixedDate := time.Date(2026, 1, 7, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name  string
		given *defaultIdentifierIsolationGuard
		hit   *hits.Hit
		want  string
	}{
		{
			name: "client_provided_session_stamp_has_priority",
			given: &defaultIdentifierIsolationGuard{
				calculatedHeaders: []string{"A", "B"},
				now:               func() time.Time { return fixedDate },
			},
			hit: func() *hits.Hit {
				// given
				h := hits.New()
				h.Request.IP = ip
				h.PropertyID = propertyID
				h.Request.Headers.Set("A", "a")
				h.Request.Headers.Set("B", "b")
				h.Request.QueryParams.Set(clientProvidedSessionStampQueryParam, "client-stamp")
				return h
			}(),
			want: mustSHA256Hex(clientProvidedSessionStampQueryParam + "=client-stamp"),
		},
		{
			name: "joins_headers_and_ip_in_order_with_separators_and_daily_salt",
			given: &defaultIdentifierIsolationGuard{
				calculatedHeaders: []string{"A", "B"},
				now:               func() time.Time { return fixedDate },
			},
			hit: func() *hits.Hit {
				// given
				h := hits.New()
				h.Request.IP = ip
				h.PropertyID = propertyID
				h.Request.Headers.Set("A", "a")
				h.Request.Headers.Set("B", "b")
				return h
			}(),
			want: mustSHA256Hex("a|b|" + ip + "|" + propertyID + "|" + fixedDate.Format("2006-01-02")),
		},
		{
			name: "includes_empty_header_values",
			given: &defaultIdentifierIsolationGuard{
				calculatedHeaders: []string{"A", "B"},
				now:               func() time.Time { return fixedDate },
			},
			hit: func() *hits.Hit {
				// given
				h := hits.New()
				h.Request.IP = ip
				h.PropertyID = propertyID
				h.Request.Headers.Set("A", "a")
				// B intentionally missing -> empty value
				return h
			}(),
			want: mustSHA256Hex("a||" + ip + "|" + propertyID + "|" + fixedDate.Format("2006-01-02")),
		},
		{
			name: "no_headers_returns_ip_and_property_and_daily_salt",
			given: &defaultIdentifierIsolationGuard{
				calculatedHeaders: nil,
				now:               func() time.Time { return fixedDate },
			},
			hit: func() *hits.Hit {
				// given
				h := hits.New()
				h.Request.IP = ip
				h.PropertyID = propertyID
				return h
			}(),
			want: mustSHA256Hex(ip + "|" + propertyID + "|" + fixedDate.Format("2006-01-02")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// when
			got := tt.given.IsolatedSessionStamp(tt.hit)

			// then
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDefaultIdentifierIsolationGuard_IsolatedSessionStamp_IncludesDailySalt(t *testing.T) {
	t.Parallel()

	// given
	guard1 := &defaultIdentifierIsolationGuard{
		calculatedHeaders: []string{"A"},
		now:               func() time.Time { return time.Date(2026, 1, 7, 12, 0, 0, 0, time.UTC) },
	}
	guard2 := &defaultIdentifierIsolationGuard{
		calculatedHeaders: []string{"A"},
		now:               func() time.Time { return time.Date(2026, 1, 8, 12, 0, 0, 0, time.UTC) },
	}

	h := hits.New()
	h.Request.IP = "1.2.3.4"
	h.PropertyID = "GA-12345"
	h.Request.Headers.Set("A", "a")

	// when
	stamp1 := guard1.IsolatedSessionStamp(h)
	stamp2 := guard2.IsolatedSessionStamp(h)

	// then
	assert.NotEqual(t, stamp1, stamp2, "session stamps should differ when dates differ")
}

func TestDefaultIdentifierIsolationGuard_IsolatedUserID(t *testing.T) {
	t.Parallel()

	const propertyID = "GA-12345"
	userID := "user123"

	tests := []struct {
		name    string
		given   *defaultIdentifierIsolationGuard
		hit     *hits.Hit
		want    string
		wantErr bool
	}{
		{
			name:  "hashes_property_id_with_user_id",
			given: &defaultIdentifierIsolationGuard{},
			hit: func() *hits.Hit {
				h := hits.New()
				h.PropertyID = propertyID
				h.UserID = &userID
				return h
			}(),
			want:    mustSHA256Hex(propertyID + "|" + userID),
			wantErr: false,
		},
		{
			name:  "returns_error_when_user_id_is_nil",
			given: &defaultIdentifierIsolationGuard{},
			hit: func() *hits.Hit {
				h := hits.New()
				h.PropertyID = propertyID
				h.UserID = nil
				return h
			}(),
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// when
			got, err := tt.given.IsolatedUserID(tt.hit)

			// then
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestDefaultIdentifierIsolationGuard_IsolatedClientID(t *testing.T) {
	t.Parallel()

	// given
	guard := &defaultIdentifierIsolationGuard{}
	h := hits.New()
	h.PropertyID = "GA-12345"
	h.AuthoritativeClientID = "client123"

	// when
	got := guard.IsolatedClientID(h)

	// then
	want := mustSHA256Hex("GA-12345|client123")
	assert.Equal(t, hits.ClientID(want), got)
}

func TestNoIsolationGuard_IsolatedSessionStamp(t *testing.T) {
	t.Parallel()

	const ip = "1.2.3.4"
	const propertyID = "GA-12345"
	fixedDate := time.Date(2026, 1, 7, 12, 0, 0, 0, time.UTC)

	// given
	guard := &noIsolationGuard{
		skipPropertyID:    true,
		calculatedHeaders: []string{"A", "B"},
		now:               func() time.Time { return fixedDate },
	}
	h := hits.New()
	h.Request.IP = ip
	h.PropertyID = propertyID
	h.Request.Headers.Set("A", "a")
	h.Request.Headers.Set("B", "b")

	// when
	got := guard.IsolatedSessionStamp(h)

	// then
	// Should not include property ID when skipPropertyID is true
	want := mustSHA256Hex("a|b|" + ip + "|" + fixedDate.Format("2006-01-02"))
	assert.Equal(t, want, got)
}

func TestNoIsolationGuard_IsolatedUserID(t *testing.T) {
	t.Parallel()

	userID := "user123"

	// given
	guard := &noIsolationGuard{
		skipPropertyID: true,
	}
	h := hits.New()
	h.UserID = &userID

	// when
	got, err := guard.IsolatedUserID(h)

	// then
	assert.NoError(t, err)
	assert.Equal(t, userID, got, "should return user ID as-is when skipPropertyID is true")
}

func TestNoIsolationGuard_IsolatedClientID(t *testing.T) {
	t.Parallel()

	// given
	guard := &noIsolationGuard{}
	h := hits.New()
	h.AuthoritativeClientID = "client123"

	// when
	got := guard.IsolatedClientID(h)

	// then
	assert.Equal(t, hits.ClientID("client123"), got, "should return client ID as-is")
}

func mustSHA256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
