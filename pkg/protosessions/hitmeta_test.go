package protosessions

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/stretchr/testify/assert"
)

func TestSetIsolatedSessionStamp_GetIsolatedSessionStamp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		given string
		want  string
		ok    bool
	}{
		{
			name:  "returns_stored_stamp",
			given: "stamp123",
			want:  "stamp123",
			ok:    true,
		},
		{
			name:  "returns_empty_when_not_set",
			given: "",
			want:  "",
			ok:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// given
			hit := hits.New()

			if tt.given != "" {
				SetIsolatedSessionStamp(hit, tt.given)
			}

			// when
			got, ok := GetIsolatedSessionStamp(hit)

			// then
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.ok, ok)
		})
	}
}

func TestSetIsolatedUserIDStamp_GetIsolatedUserIDStamp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		given string
		want  string
		ok    bool
	}{
		{
			name:  "returns_stored_stamp",
			given: "userid123",
			want:  "userid123",
			ok:    true,
		},
		{
			name:  "returns_empty_when_not_set",
			given: "",
			want:  "",
			ok:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// given
			hit := hits.New()

			if tt.given != "" {
				SetIsolatedUserIDStamp(hit, tt.given)
			}

			// when
			got, ok := GetIsolatedUserIDStamp(hit)

			// then
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.ok, ok)
		})
	}
}
