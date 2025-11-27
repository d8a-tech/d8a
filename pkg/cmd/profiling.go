package cmd

import (
	"fmt"
	"net/http"
	_ "net/http/pprof" // Register pprof handlers

	"github.com/sirupsen/logrus"
)

// StartPprofServer starts an HTTP server exposing pprof endpoints. Port 0 disables it.
func StartPprofServer(port int) {
	if port == 0 {
		return
	}

	go func() {
		addr := fmt.Sprintf(":%d", port)
		logrus.Infof("pprof server listening on %s", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			logrus.Errorf("pprof server error: %v", err)
		}
	}()
}

