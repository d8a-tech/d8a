package cmd

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
)

func TestClearS3ChecksumAlgorithm(t *testing.T) {
	t.Run("put object", func(t *testing.T) {
		// given
		input := &s3.PutObjectInput{ChecksumAlgorithm: types.ChecksumAlgorithmCrc32}

		// when
		clearS3ChecksumAlgorithm(input)

		// then
		assert.Empty(t, input.ChecksumAlgorithm)
	})

	t.Run("create multipart upload", func(t *testing.T) {
		// given
		input := &s3.CreateMultipartUploadInput{ChecksumAlgorithm: types.ChecksumAlgorithmCrc32}

		// when
		clearS3ChecksumAlgorithm(input)

		// then
		assert.Empty(t, input.ChecksumAlgorithm)
	})

	t.Run("upload part", func(t *testing.T) {
		// given
		input := &s3.UploadPartInput{ChecksumAlgorithm: types.ChecksumAlgorithmCrc32}

		// when
		clearS3ChecksumAlgorithm(input)

		// then
		assert.Empty(t, input.ChecksumAlgorithm)
	})

	t.Run("unrelated input", func(t *testing.T) {
		// given
		input := &s3.GetObjectInput{}

		// when
		clearS3ChecksumAlgorithm(input)

		// then
		assert.NotNil(t, input)
	})
}
