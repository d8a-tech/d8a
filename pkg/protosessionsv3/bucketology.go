package protosessionsv3

import "time"

// BucketNumber returns the bucket number for a given time and session duration.
func BucketNumber(time time.Time, tickInterval time.Duration) int64 {
	// A bucket every second
	if tickInterval < 1 {
		return time.Unix()
	}
	return time.Unix() / int64(tickInterval.Seconds())
}
