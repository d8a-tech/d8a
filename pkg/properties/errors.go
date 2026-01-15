package properties

import "fmt"

type NotFoundError struct {
	PropertyID string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("property %q not found", e.PropertyID)
}

func NewNotFoundError(propertyID string) *NotFoundError {
	return &NotFoundError{PropertyID: propertyID}
}
