package hits

// HitProcessingTaskName is the name of the task used for processing hits
const HitProcessingTaskName = "process-hits"

// HitProcessingTask represents a task containing a batch of hits to be processed
type HitProcessingTask struct {
	Hits []*Hit `cbor:"h"`
}
