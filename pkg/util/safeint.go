// Package util provides utility functions for type conversions and other common operations.
package util

import (
	"errors"
	"math"
	"strings"

	"github.com/sirupsen/logrus"
)

// SafeIntToUint8 converts int to uint8 with overflow check.
func SafeIntToUint8(v int) uint8 {
	if v < 0 {
		logrus.Panicf("negative value %d cannot be converted to uint8", v)
	}
	if v > math.MaxUint8 {
		logrus.Panicf("value %d exceeds uint8 max value %d", v, math.MaxUint8)
	}
	return uint8(v)
}

// SafeIntToUint16 converts int to uint16 with overflow check.
func SafeIntToUint16(v int) uint16 {
	if v < 0 {
		logrus.Panicf("negative value %d cannot be converted to uint16", v)
	}
	if v > math.MaxUint16 {
		logrus.Panicf("value %d exceeds uint16 max value %d", v, math.MaxUint16)
	}
	return uint16(v)
}

// SafeIntToUint32 converts int to uint32 with overflow check.
func SafeIntToUint32(v int) uint32 {
	if v < 0 {
		logrus.Panicf("negative value %d cannot be converted to uint32", v)
	}
	if v > math.MaxUint32 {
		logrus.Panicf("value %d exceeds uint32 max value %d", v, math.MaxUint32)
	}
	return uint32(v)
}

// SafeUintToInt converts uint to int with overflow check.
func SafeUintToInt(v uint) int {
	if v > uint(math.MaxInt) {
		logrus.Panicf("value %d exceeds int max value %d", v, math.MaxInt)
	}
	return int(v)
}

// StrToBool converts a string to bool considering various truthy representations.
// Accepts: "true", "yes", "y", "on", "1", "t" as true
// Accepts: "false", "no", "n", "off", "0", "f" as false
// Returns error for empty strings or unrecognized values.
func StrToBool(str string) (bool, error) {
	// Normalize string: trim whitespace and convert to lowercase
	normalized := strings.ToLower(strings.TrimSpace(str))

	// Handle empty string as error
	if normalized == "" {
		return false, errors.New("empty string cannot be converted to bool")
	}

	// Check truthy values
	switch normalized {
	case "true", "yes", "y", "on", "1", "t":
		return true, nil
	case "false", "no", "n", "off", "0", "f":
		return false, nil
	default:
		return false, errors.New("unrecognized boolean value")
	}
}
