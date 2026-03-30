package bigquery

import (
	"errors"
	"net/http"

	"google.golang.org/api/googleapi"
)

func isNotFoundErr(err error) bool {
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		return apiErr.Code == http.StatusNotFound
	}
	return false
}

func isAlreadyExistsErr(err error) bool {
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		return apiErr.Code == http.StatusConflict
	}
	return false
}
