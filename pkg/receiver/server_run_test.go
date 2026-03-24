package receiver

import (
	"bytes"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestFastHTTPServerLogger_TLSHandshakeError(t *testing.T) {
	// given
	logger := logrus.New()
	var output bytes.Buffer
	logger.SetOutput(&output)

	serverLogger := newFastHTTPServerLogger(logger)
	message := "error when serving connection \"127.0.0.1:8080\"<->\"127.0.0.1:54124\": " +
		"error when reading request headers: unsupported http request method \"\\x16\\x03\\x01\""

	// when
	serverLogger.Printf("%s", message)

	// then
	logged := output.String()
	assert.Contains(t, logged, "level=warning")
	assert.Contains(t, logged, httpsOnHTTPListenerMessage)
	assert.NotContains(t, logged, "Buffer size=")
	assert.NotContains(t, logged, "unsupported http request method")
}

func TestFastHTTPServerLogger_NonTLSError(t *testing.T) {
	// given
	logger := logrus.New()
	var output bytes.Buffer
	logger.SetOutput(&output)

	serverLogger := newFastHTTPServerLogger(logger)
	message := "error when serving connection \"127.0.0.1:8080\"<->\"127.0.0.1:54124\": broken pipe"

	// when
	serverLogger.Printf("%s", message)

	// then
	logged := output.String()
	assert.Contains(t, logged, "level=error")
	assert.Contains(t, logged, "broken pipe")
	assert.Contains(t, logged, "127.0.0.1:8080")
}
