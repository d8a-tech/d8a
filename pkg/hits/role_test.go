package hits

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAnnotationRole(t *testing.T) {
	// given
	ordinary := New()
	annotation := New()
	noMetadata := &Hit{}

	// when
	MarkAnnotation(annotation)
	MarkAnnotation(noMetadata)

	// then
	assert.False(t, IsAnnotation(ordinary))
	assert.True(t, IsAnnotation(annotation))
	assert.True(t, IsAnnotation(noMetadata))
	assert.False(t, IsAnnotation(nil))
}
