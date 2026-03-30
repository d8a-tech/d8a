package bigquery

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/api/googleapi"
)

func TestIsNotFoundErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "googleapi 404",
			err:  &googleapi.Error{Code: 404, Message: "Not Found"},
			want: true,
		},
		{
			name: "wrapped googleapi 404",
			err:  fmt.Errorf("getting table: %w", &googleapi.Error{Code: 404, Message: "Not Found"}),
			want: true,
		},
		{
			name: "googleapi 500",
			err:  &googleapi.Error{Code: 500, Message: "Internal Server Error"},
			want: false,
		},
		{
			name: "googleapi 409",
			err:  &googleapi.Error{Code: 409, Message: "Conflict"},
			want: false,
		},
		{
			name: "plain error with not found text",
			err:  errors.New("resource not found"),
			want: false,
		},
		{
			name: "plain error with 404 text",
			err:  errors.New("got 404 from server"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// when
			got := isNotFoundErr(tt.err)

			// then
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsAlreadyExistsErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "googleapi 409",
			err:  &googleapi.Error{Code: 409, Message: "Already Exists"},
			want: true,
		},
		{
			name: "wrapped googleapi 409",
			err:  fmt.Errorf("creating table: %w", &googleapi.Error{Code: 409, Message: "Already Exists"}),
			want: true,
		},
		{
			name: "googleapi 404",
			err:  &googleapi.Error{Code: 404, Message: "Not Found"},
			want: false,
		},
		{
			name: "googleapi 500",
			err:  &googleapi.Error{Code: 500, Message: "Internal Server Error"},
			want: false,
		},
		{
			name: "plain error with already exists text",
			err:  errors.New("table already exists"),
			want: false,
		},
		{
			name: "plain error with duplicate text",
			err:  errors.New("duplicate entry"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// when
			got := isAlreadyExistsErr(tt.err)

			// then
			assert.Equal(t, tt.want, got)
		})
	}
}
