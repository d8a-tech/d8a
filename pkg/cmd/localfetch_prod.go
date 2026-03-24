//go:build !e2e

package cmd

import "context"

func performLocalfetch(context.Context, string) error {
	panic("localfetch is only available in e2e builds")
}
