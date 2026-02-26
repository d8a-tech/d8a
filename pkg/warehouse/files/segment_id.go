package files

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// segmentIDFromSealTime returns a segment ID encoding the seal time as a unix
// timestamp prefix: "<unixSeconds>_<uuid>".
func segmentIDFromSealTime(sealTime time.Time) string {
	return fmt.Sprintf("%d_%s", sealTime.Unix(), uuid.NewString())
}

// parseSealTimeFromSegmentID extracts the seal time from a timestamp-prefixed
// segment ID ("<unixSeconds>_<uuid>").  It returns (time, true) on success.
// For legacy bare-UUID segment IDs the parse will fail and (zero, false) is
// returned; callers should fall back to file modtime in that case.
func parseSealTimeFromSegmentID(segmentID string) (time.Time, bool) {
	idx := strings.IndexByte(segmentID, '_')
	if idx <= 0 {
		return time.Time{}, false
	}

	sec, err := strconv.ParseInt(segmentID[:idx], 10, 64)
	if err != nil {
		return time.Time{}, false
	}

	return time.Unix(sec, 0).UTC(), true
}
