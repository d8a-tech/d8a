package cmd

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewS3BlobOptions(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		// given
		disableUploadChecksums := false

		// when
		opts := newS3BlobOptions(disableUploadChecksums)

		// then
		assert.Nil(t, opts)
	})

	t.Run("disable upload checksums", func(t *testing.T) {
		// given
		disableUploadChecksums := true

		// when
		opts := newS3BlobOptions(disableUploadChecksums)

		// then
		require.NotNil(t, opts)
		assert.Equal(t, aws.RequestChecksumCalculationWhenRequired, opts.RequestChecksumCalculation)
	})
}
