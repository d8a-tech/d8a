# ProtoSession Batching Architecture

## Problem Statement

The current protosession middleware architecture (`worker.go`, `trigger.go`, `evicter.go`, `compactor.go`) has a clean API with good separation of concerns, but is focused on processing single hits. This creates challenges:

1. **Inefficient I/O**: Each hit triggers separate storage operations, missing opportunities for batching
2. **Limited backend optimization**: The `storage.KV` and `storage.Set` abstractions are simple but don't expose advanced capabilities of different storage backends
3. **Future scalability concerns**: 
   - Redis with hot/cold storage tiering isn't possible with current abstractions
   - PostgreSQL can't leverage SQL-specific optimizations (batch inserts, indexes, transactions)
   - Can't implement storage-aware optimizations transparently

## Proposed Architecture

### Core Concepts

**Business-level operations** instead of storage primitives. Each storage backend implements these operations optimally for its technology stack.

The architecture uses a **continuation-based middleware pattern** similar to the current single-hit middleware, but adapted for batches with explicit IO operations.

```go
// IOOperation represents a business-level operation
type IOOperation interface {
    Describe() string
}

// IOResult represents the result of an operation
type IOResult interface {
    Operation() IOOperation
    Error() error
}

// IOResults provides access to operation results
type IOResults interface {
    // Get finds the result for a specific operation
    Get(op IOOperation) (IOResult, bool)
    // Filter returns all results matching a predicate
    Filter(fn func(IOResult) bool) []IOResult
}

// StorageBackend implements business operations optimally
type StorageBackend interface {
    ExecuteBatch(ctx context.Context, operations []IOOperation) (IOResults, error)
}

// Middleware processes a batch of hits
type Middleware interface {
    // Handle processes batch and calls next() to execute IO operations
    // next() executes the operations and returns results
    // Can call next() multiple times to perform multiple IO phases
    Handle(ctx context.Context, batch *HitBatch, next func([]IOOperation) (IOResults, error)) error
}
```

### Flow Pattern

The `next()` function is the IO execution point. A middleware can:

1. **Compute** what operations it needs
2. **Call next(operations)** to execute them
3. **Analyze results** from next()
4. **Call next() again** if more IO is needed
5. **Return** when done

The middleware chain works like this:

```go
func Chain(middlewares []Middleware, backend StorageBackend) func(*HitBatch) error {
    return func(batch *HitBatch) error {
        // Build recursive chain
        var handle func(idx int, executeOps func([]IOOperation) (IOResults, error)) error
        
        handle = func(idx int, executeOps func([]IOOperation) (IOResults, error)) error {
            if idx >= len(middlewares) {
                // End of chain: final middleware can still execute ops
                return nil
            }
            
            // Each middleware gets an executeOps function that:
            // 1. Executes the operations via backend
            // 2. Continues to next middleware
            return middlewares[idx].Handle(batch, func(ops []IOOperation) (IOResults, error) {
                // Execute IO operations
                results, err := backend.ExecuteBatch(context.TODO(), ops)
                if err != nil {
                    return nil, err
                }
                
                // Continue to next middleware
                err = handle(idx+1, executeOps)
                if err != nil {
                    return nil, err
                }
                
                return results, nil
            })
        }
        
        return handle(0, nil)
    }
}
```

**Key insight**: `next()` becomes the IO execution boundary, not "call next middleware". Each middleware controls when to perform IO by calling `next()` with operations.

## Business Operations

### Identifier Conflict Operations

```go
// CheckIdentifierConflict checks if an identifier maps to a different client
type CheckIdentifierConflict struct {
    IdentifierType   string // "session_stamp", "device_id", etc.
    IdentifierValue  string
    ProposedClientID hits.ClientID
}

type IdentifierConflictResult struct {
    Op               *CheckIdentifierConflict
    HasConflict      bool
    ExistingClientID hits.ClientID
    Err              error
}

// MapIdentifierToClient stores identifier → clientID mapping
type MapIdentifierToClient struct {
    IdentifierType  string
    IdentifierValue string
    ClientID        hits.ClientID
}
```

### ProtoSession Lifecycle Operations

```go
// ScheduleProtoSessionExpiration marks when a session should be checked for closure
type ScheduleProtoSessionExpiration struct {
    ClientID       hits.ClientID
    ExpirationTime time.Time
}

// AddHitsToProtoSession adds hits to a proto-session
type AddHitsToProtoSession struct {
    ClientID hits.ClientID
    Hits     []*hits.Hit
}

// LoadProtoSessionHits retrieves all hits for a proto-session
type LoadProtoSessionHits struct {
    ClientID hits.ClientID
}

type ProtoSessionHitsResult struct {
    Op   *LoadProtoSessionHits
    Hits []*hits.Hit
    Err  error
}

// GetProtoSessionsExpiringInBucket retrieves sessions expiring in a time bucket
type GetProtoSessionsExpiringInBucket struct {
    BucketID int64
}

type ExpiringProtoSessionsResult struct {
    Op        *GetProtoSessionsExpiringInBucket
    ClientIDs []hits.ClientID
    Err       error
}
```

### Compaction Operations

```go
// CheckProtoSessionSize checks current size of a proto-session
type CheckProtoSessionSize struct {
    ClientID hits.ClientID
}

type ProtoSessionSizeResult struct {
    Op        *CheckProtoSessionSize
    SizeBytes uint32
    Err       error
}

// CompactProtoSession compresses a proto-session's storage
type CompactProtoSession struct {
    ClientID hits.ClientID
}
```

### Cleanup Operations

```go
// DeleteProtoSession removes all data for a proto-session
type DeleteProtoSession struct {
    ClientID hits.ClientID
}

// DeleteIdentifierMapping removes identifier mapping
type DeleteIdentifierMapping struct {
    IdentifierType  string
    IdentifierValue string
}
```

## Storage Backend Implementations

### BoltDB Backend (Simple, KV-based)

The BoltDB backend uses existing `storage.KV` and `storage.Set` primitives. It translates each business operation into the appropriate KV/Set calls.

```go
type BoltBackend struct {
    kv      storage.KV
    set     storage.Set
    encoder encoding.EncoderFunc
    decoder encoding.DecoderFunc
}

func (b *BoltBackend) ExecuteBatch(ctx context.Context, operations []IOOperation) (IOResults, error) {
    results := make([]IOResult, 0, len(operations))
    
    for _, op := range operations {
        switch o := op.(type) {
        case *CheckIdentifierConflict:
            // Simple KV lookup
            key := []byte(fmt.Sprintf("identifier.%s.%s", o.IdentifierType, o.IdentifierValue))
            val, err := b.kv.Get(key)
            // ... build IdentifierConflictResult
            
        case *ScheduleProtoSessionExpiration:
            // Add to set + store timestamp
            bucketID := BucketNumber(o.ExpirationTime, time.Second)
            b.set.Add([]byte(BucketsKey(bucketID)), []byte(o.ClientID))
            b.kv.Set([]byte(ExpirationKey(string(o.ClientID))), []byte(fmt.Sprintf("%d", bucketID)))
            
        // ... handle other operations
        }
    }
    
    return newIOResults(results), nil
}
```

**Characteristics**: Simple, works with any KV/Set implementation, no special optimization.

### PostgreSQL Backend

PostgreSQL backend batches operations into efficient SQL queries with transactions and indexes.

**Key optimizations:**
- Multiple `CheckIdentifierConflict` → single `SELECT ... WHERE (type, value) IN (VALUES ...)`
- Multiple `MapIdentifierToClient` → single `INSERT ... VALUES (...), (...), ... ON CONFLICT`
- Multiple `ScheduleProtoSessionExpiration` → single `INSERT ... ON CONFLICT DO UPDATE`
- `AddHitsToProtoSession` → `COPY` for bulk inserts
- Everything in one transaction

**Schema example:**
```sql
CREATE TABLE identifier_mappings (
    identifier_type VARCHAR(50),
    identifier_value VARCHAR(255),
    client_id VARCHAR(255),
    PRIMARY KEY (identifier_type, identifier_value)
);
CREATE INDEX idx_client_id ON identifier_mappings(client_id);
```

### Redis Backend

Redis backend uses pipelining to batch commands and can implement hot/cold storage tiering.

**Key optimizations:**
- All operations pipelined into single round-trip
- `CheckIdentifierConflict` → `HGET identifier:{type} {value}`
- `ScheduleProtoSessionExpiration` → `ZADD` for sorted expiration buckets
- TTL-based automatic eviction to cold storage
- Can keep only metadata in Redis after inactivity threshold

**Tiering strategy**: After 10 minutes of inactivity, hits are moved to slower storage (PostgreSQL/S3), Redis keeps only session metadata.

## Middleware Examples

### Identifier Clash Detection Middleware

This middleware checks if session identifiers conflict with existing mappings and marks hits for eviction if needed.

```go
type IdentifierClashMiddleware struct {
    identifierType    string
    extractIdentifier func(*hits.Hit) string
}

func (m *IdentifierClashMiddleware) Handle(
    ctx context.Context,
    batch *HitBatch,
    next func([]IOOperation) (IOResults, error),
) error {
    // Phase 1: Request conflict checks for all hits in batch
    checkOps := make([]IOOperation, 0, len(batch.Hits))
    for _, hit := range batch.Hits {
        checkOps = append(checkOps, &CheckIdentifierConflict{
            IdentifierType:   m.identifierType,
            IdentifierValue:  m.extractIdentifier(hit),
            ProposedClientID: hit.AuthoritativeClientID,
        })
    }
    
    // Execute conflict checks
    results, err := next(checkOps)
    if err != nil {
        return err
    }
    
    // Phase 2: Analyze results and prepare write operations
    writeOps := make([]IOOperation, 0)
    
    for _, result := range results.Filter(func(r IOResult) bool {
        _, ok := r.(*IdentifierConflictResult)
        return ok
    }) {
        conflictResult := result.(*IdentifierConflictResult)
        
        if conflictResult.HasConflict {
            // Mark hit for eviction to existing client ID
            for _, hit := range batch.Hits {
                if m.extractIdentifier(hit) == conflictResult.Op.IdentifierValue {
                    batch.MarkForEviction(hit, conflictResult.ExistingClientID)
                }
            }
        } else {
            // No conflict: store the mapping
            writeOps = append(writeOps, &MapIdentifierToClient{
                IdentifierType:  conflictResult.Op.IdentifierType,
                IdentifierValue: conflictResult.Op.IdentifierValue,
                ClientID:        conflictResult.Op.ProposedClientID,
            })
        }
    }
    
    // Execute write operations if any
    if len(writeOps) > 0 {
        _, err = next(writeOps)
        if err != nil {
            return err
        }
    }
    
    return nil
}
```

**Flow:**
1. Gather all identifier conflict checks
2. Call `next(checkOps)` to execute them
3. Analyze results
4. Gather write operations
5. Call `next(writeOps)` to execute them

### Expiration Scheduling Middleware

This middleware schedules when proto-sessions should be checked for closure.

```go
type ExpirationSchedulingMiddleware struct {
    sessionDuration time.Duration
}

func (m *ExpirationSchedulingMiddleware) Handle(
    ctx context.Context,
    batch *HitBatch,
    next func([]IOOperation) (IOResults, error),
) error {
    // Single phase: schedule expirations for all non-evicted hits
    ops := make([]IOOperation, 0, len(batch.Hits))
    
    for _, hit := range batch.Hits {
        if batch.IsMarkedForEviction(hit) {
            continue // Skip hits being evicted
        }
        
        expirationTime := hit.ServerReceivedTime.Add(m.sessionDuration)
        ops = append(ops, &ScheduleProtoSessionExpiration{
            ClientID:       hit.AuthoritativeClientID,
            ExpirationTime: expirationTime,
        })
    }
    
    // Execute all schedule operations
    _, err := next(ops)
    return err
}
```

**Flow:**
1. Build list of expiration schedule operations
2. Call `next(ops)` once to execute them all

### Hit Storage Middleware

This middleware stores hits in the proto-session, grouped by client ID.

```go
type HitStorageMiddleware struct{}

func (m *HitStorageMiddleware) Handle(
    ctx context.Context,
    batch *HitBatch,
    next func([]IOOperation) (IOResults, error),
) error {
    // Group hits by client ID for efficient storage
    hitsByClient := make(map[hits.ClientID][]*hits.Hit)
    
    for _, hit := range batch.Hits {
        if batch.IsMarkedForEviction(hit) {
            continue // Skip evicted hits
        }
        hitsByClient[hit.AuthoritativeClientID] = append(
            hitsByClient[hit.AuthoritativeClientID], 
            hit,
        )
    }
    
    // Create batch add operations
    ops := make([]IOOperation, 0, len(hitsByClient))
    for clientID, hits := range hitsByClient {
        ops = append(ops, &AddHitsToProtoSession{
            ClientID: clientID,
            Hits:     hits,
        })
    }
    
    // Execute storage operations
    _, err := next(ops)
    return err
}
```

**Flow:**
1. Group hits by client ID
2. Create `AddHitsToProtoSession` operations
3. Call `next(ops)` to store them all

### Compaction Middleware (Multi-phase example)

This middleware demonstrates calling `next()` multiple times for different phases.

```go
type CompactionMiddleware struct {
    thresholdBytes uint32
}

func (m *CompactionMiddleware) Handle(
    ctx context.Context,
    batch *HitBatch,
    next func([]IOOperation) (IOResults, error),
) error {
    // Phase 1: Check sizes of all affected proto-sessions
    clientIDs := batch.UniqueClientIDs()
    checkOps := make([]IOOperation, 0, len(clientIDs))
    for _, clientID := range clientIDs {
        checkOps = append(checkOps, &CheckProtoSessionSize{ClientID: clientID})
    }
    
    results, err := next(checkOps)
    if err != nil {
        return err
    }
    
    // Phase 2: Compact sessions that exceed threshold
    compactOps := make([]IOOperation, 0)
    for _, result := range results.Filter(func(r IOResult) bool {
        _, ok := r.(*ProtoSessionSizeResult)
        return ok
    }) {
        sizeResult := result.(*ProtoSessionSizeResult)
        if sizeResult.SizeBytes >= m.thresholdBytes {
            compactOps = append(compactOps, &CompactProtoSession{
                ClientID: sizeResult.Op.ClientID,
            })
        }
    }
    
    if len(compactOps) > 0 {
        _, err = next(compactOps)
        if err != nil {
            return err
        }
    }
    
    return nil
}
```

**Flow:**
1. Check sizes (first `next()` call)
2. Analyze results
3. Compact large sessions (second `next()` call if needed)

## Usage Example

```go
func NewHandler(
    ctx context.Context,
    backend StorageBackend,
) func(map[string]string, *hits.HitProcessingTask) *worker.Error {
    
    // Define middleware chain
    middlewares := []Middleware{
        // 1. Check for identifier conflicts
        &IdentifierClashMiddleware{
            identifierType:    "session_stamp",
            extractIdentifier: func(h *hits.Hit) string { return h.SessionStamp() },
        },
        
        // 2. Handle evictions (processes hits marked for eviction)
        &EvictionMiddleware{},
        
        // 3. Schedule expiration checks
        &ExpirationSchedulingMiddleware{
            sessionDuration: 30 * time.Minute,
        },
        
        // 4. Store hits in proto-sessions
        &HitStorageMiddleware{},
        
        // 5. Check if compaction is needed
        &CompactionMiddleware{
            thresholdBytes: 1024 * 1024, // 1MB
        },
    }
    
    // Build the handler chain
    handler := Chain(middlewares, backend)
    
    return func(md map[string]string, task *hits.HitProcessingTask) *worker.Error {
        batch := &HitBatch{Hits: task.Hits}
        err := handler(batch)
        if err != nil {
            return worker.NewError(worker.ErrTypeDroppable, err)
        }
        return nil
    }
}

// Example: Creating different backends
func main() {
    // Option 1: Simple BoltDB backend
    boltBackend := &BoltBackend{
        kv:      boltKV,
        set:     boltSet,
        encoder: msgpackEncoder,
        decoder: msgpackDecoder,
    }
    handler1 := NewHandler(ctx, boltBackend)
    
    // Option 2: PostgreSQL backend (same middleware, different backend)
    pgBackend := &PostgreSQLBackend{
        db:      pgDB,
        encoder: msgpackEncoder,
        decoder: msgpackDecoder,
    }
    handler2 := NewHandler(ctx, pgBackend)
    
    // Option 3: Redis backend with hot/cold tiering
    redisBackend := &RedisBackend{
        redis:      redisClient,
        encoder:    msgpackEncoder,
        decoder:    msgpackDecoder,
        hotTimeout: 10 * time.Minute,
    }
    handler3 := NewHandler(ctx, redisBackend)
    
    // All handlers use the SAME middleware logic,
    // but each backend optimizes IO operations differently
}
```
