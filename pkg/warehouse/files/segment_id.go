package files

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// segmentIDFromSealTime returns a segment ID encoding the seal time as a unix
// timestamp prefix: "<unixSeconds>_<uuid>".
func segmentIDFromSealTime(sealTime time.Time) string {
	return fmt.Sprintf("%d_%s", sealTime.Unix(), uuid.NewString())
}
