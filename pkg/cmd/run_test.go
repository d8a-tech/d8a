package cmd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunHelp(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "root", args: []string{"--help"}},
		{name: "columns", args: []string{"columns", "--help"}},
		{name: "server", args: []string{"server", "--help"}},
		{name: "receiver", args: []string{"receiver", "--help"}},
		{name: "worker", args: []string{"worker", "--help"}},
		{name: "migrate", args: []string{"migrate", "--help"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// when
			err := Run(ctx, cancel, tt.args)

			// then
			require.NoError(t, err)
		})
	}
}
