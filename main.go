// Package main provides the entry point for the tracker-api service
package main

import (
	"context"
	"os"

	"github.com/d8a-tech/d8a/pkg/cmd"
)

func main() {
	ctx, cancelF := context.WithCancel(context.Background())
	cmd.Run(ctx, cancelF, os.Args[1:])
}
