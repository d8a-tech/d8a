package spools

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

const (
	headerSize                      = 4
	spoolExt                        = ".spool"
	inflightMarker                  = ".spool.inflight."
	filePerms                       = 0o644
	maxPayloadSz                    = 0xFFFFFFFF
	defaultMaxFailure               = 20
	defaultBufferSize               = 1
	defaultFlushBatchSize           = 5
	defaultFlushBatchMaxBytes int64 = 10 << 20
	flushRetryDelay                 = 10 * time.Millisecond
)

// FlushHandler is called during flush cycles for each inflight file.
type FlushHandler func(key string, next func() ([][]byte, error)) error

// Spool is a crash-safe keyed append-only framed-file store.
type Spool interface {
	Append(key string, payload []byte) error
}

// Factory creates and manages spool lifecycle.
type Factory interface {
	Create(handler FlushHandler) (Spool, error)
	Close() error
}

// FailureStrategy defines the action to take when an inflight spool file
// exceeds the maximum number of consecutive flush failures.
type FailureStrategy interface {
	OnExceededFailures(fs afero.Fs, inflightPath string) error
}

// deleteStrategy removes the inflight file on exceeded failures (best-effort delivery).
type deleteStrategy struct{}

// NewDeleteStrategy creates a FailureStrategy that deletes inflight files
// when consecutive flush failures exceed the threshold.
func NewDeleteStrategy() FailureStrategy {
	return &deleteStrategy{}
}

// OnExceededFailures implements FailureStrategy.
func (s *deleteStrategy) OnExceededFailures(fs afero.Fs, inflightPath string) error {
	if err := fs.Remove(inflightPath); err != nil {
		return fmt.Errorf("removing discarded spool file %q: %w", inflightPath, err)
	}
	return nil
}

// quarantineStrategy renames the inflight file to .quarantine on exceeded failures (at-least-once delivery).
type quarantineStrategy struct{}

// NewQuarantineStrategy creates a FailureStrategy that quarantines inflight files
// by renaming them with a .quarantine suffix when consecutive flush failures
// exceed the threshold.
func NewQuarantineStrategy() FailureStrategy {
	return &quarantineStrategy{}
}

// OnExceededFailures implements FailureStrategy.
func (s *quarantineStrategy) OnExceededFailures(fs afero.Fs, inflightPath string) error {
	quarantinePath := inflightPath + ".quarantine"
	if err := fs.Rename(inflightPath, quarantinePath); err != nil {
		return fmt.Errorf("quarantining spool file %q: %w", inflightPath, err)
	}
	logrus.Warnf("quarantined spool file %q to %q after exceeding failure threshold", inflightPath, quarantinePath)
	return nil
}

// FileFactoryOption configures a fileFactory.
type FileFactoryOption func(*fileFactory)

// WithFailureStrategy sets the strategy invoked when an inflight file
// exceeds the maximum number of consecutive flush failures.
// When nil (the default), inflight files are deleted on threshold breach.
func WithFailureStrategy(s FailureStrategy) FileFactoryOption {
	return func(f *fileFactory) {
		f.failureStrategy = s
	}
}

// WithMaxFailures sets the per-key consecutive failure threshold
// before the failure strategy is invoked. Default is 20.
func WithMaxFailures(n int) FileFactoryOption {
	return func(f *fileFactory) {
		f.maxFailures = n
	}
}

// WithNowFunc overrides the clock used to generate inflight timestamps.
// Intended for testing.
func WithNowFunc(fn func() time.Time) FileFactoryOption {
	return func(f *fileFactory) {
		f.nowFunc = fn
	}
}

// WithMaxActiveSize sets the maximum size (in bytes) for an active spool
// file. When an Append would cause the active file to exceed this limit,
// the current active file is rotated to a sealed inflight file and a fresh
// active file is started. Zero (default) means no size limit.
func WithMaxActiveSize(bytes int64) FileFactoryOption {
	return func(f *fileFactory) {
		f.maxActiveSize = bytes
	}
}

// WithFlushBatchSize sets the maximum number of frames returned per call
// to the next function passed to the Flush callback. Zero disables batching,
// causing all frames in the inflight file to be returned in a single batch.
func WithFlushBatchSize(n int) FileFactoryOption {
	return func(f *fileFactory) {
		f.flushBatchSize = n
	}
}

// WithFlushBatchMaxBytes sets the maximum payload bytes returned per call
// to the next function passed to the Flush callback. Zero disables the limit.
func WithFlushBatchMaxBytes(bytes int64) FileFactoryOption {
	return func(f *fileFactory) {
		f.flushBatchMaxBytes = bytes
	}
}

// WithFlushInterval sets periodic flush interval. Zero disables the timer.
func WithFlushInterval(interval time.Duration) FileFactoryOption {
	return func(f *fileFactory) {
		f.flushInterval = interval
	}
}

// WithFlushOnClose enables final flush during Close. Default is true.
func WithFlushOnClose(enabled bool) FileFactoryOption {
	return func(f *fileFactory) {
		f.flushOnClose = enabled
	}
}

// WithMaxBytesBeforeFlush triggers flush after this many appended bytes.
// Zero disables this trigger.
func WithMaxBytesBeforeFlush(bytes int64) FileFactoryOption {
	return func(f *fileFactory) {
		f.maxBytesBeforeFlush = bytes
	}
}

// WithMaxAppendsBeforeFlush triggers flush after this many append calls.
// Zero disables this trigger.
func WithMaxAppendsBeforeFlush(n int) FileFactoryOption {
	return func(f *fileFactory) {
		f.maxAppendsBeforeFlush = n
	}
}

type fileFactory struct {
	fs                    afero.Fs
	dir                   string
	failureStrategy       FailureStrategy
	maxFailures           int
	maxActiveSize         int64
	flushBatchSize        int
	flushBatchMaxBytes    int64
	nowFunc               func() time.Time
	flushInterval         time.Duration
	flushOnClose          bool
	maxBytesBeforeFlush   int64
	maxAppendsBeforeFlush int
	newTicker             func(time.Duration) ticker

	mu      sync.Mutex
	created bool
	closed  bool
	spool   *fileSpool

	closeOnce sync.Once
	closeErr  error
}

// NewFileFactory creates a file-backed spool factory.
func NewFileFactory(fs afero.Fs, dir string, opts ...FileFactoryOption) (Factory, error) {
	if err := fs.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating spool directory: %w", err)
	}

	f := &fileFactory{
		fs:                 fs,
		dir:                dir,
		failureStrategy:    &deleteStrategy{},
		maxFailures:        defaultMaxFailure,
		flushBatchSize:     defaultFlushBatchSize,
		flushBatchMaxBytes: defaultFlushBatchMaxBytes,
		nowFunc:            time.Now,
		flushOnClose:       true,
		newTicker:          newRealTicker,
	}

	for _, opt := range opts {
		opt(f)
	}

	return f, nil
}

// Create implements Factory.
func (f *fileFactory) Create(handler FlushHandler) (Spool, error) {
	if handler == nil {
		return nil, fmt.Errorf("flush handler is required")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return nil, fmt.Errorf("factory is closed")
	}

	if f.created {
		return nil, fmt.Errorf("spool already created")
	}
	f.created = true

	s := &fileSpool{
		fs:                    f.fs,
		dir:                   f.dir,
		failureStrategy:       f.failureStrategy,
		maxFailures:           f.maxFailures,
		maxActiveSize:         f.maxActiveSize,
		flushBatchSize:        f.flushBatchSize,
		flushBatchMaxBytes:    f.flushBatchMaxBytes,
		nowFunc:               f.nowFunc,
		failuresByKey:         make(map[string]int),
		handler:               handler,
		flushInterval:         f.flushInterval,
		flushOnClose:          f.flushOnClose,
		stopCh:                make(chan struct{}),
		triggerCh:             make(chan struct{}, defaultBufferSize),
		newTicker:             f.newTicker,
		maxBytesBeforeFlush:   f.maxBytesBeforeFlush,
		maxAppendsBeforeFlush: f.maxAppendsBeforeFlush,
	}

	if err := s.recover(); err != nil {
		return nil, fmt.Errorf("recovering spool: %w", err)
	}

	s.start()
	f.spool = s

	return s, nil
}

// Close implements Factory.
func (f *fileFactory) Close() error {
	f.closeOnce.Do(func() {
		f.mu.Lock()
		f.closed = true
		s := f.spool
		f.mu.Unlock()

		if s != nil {
			f.closeErr = s.close()
		}
	})

	return f.closeErr
}

type fileSpool struct {
	fs                 afero.Fs
	dir                string
	failureStrategy    FailureStrategy
	maxFailures        int
	maxActiveSize      int64
	flushBatchSize     int
	flushBatchMaxBytes int64
	nowFunc            func() time.Time

	mu     sync.Mutex
	closed bool

	failMu        sync.Mutex
	failuresByKey map[string]int

	handler               FlushHandler
	flushInterval         time.Duration
	flushOnClose          bool
	stopCh                chan struct{}
	wg                    sync.WaitGroup
	newTicker             func(time.Duration) ticker
	triggerCh             chan struct{}
	appendBytes           atomic.Int64
	appendCount           atomic.Int32
	maxBytesBeforeFlush   int64
	maxAppendsBeforeFlush int
	closeOnce             sync.Once
}

type ticker interface {
	C() <-chan time.Time
	Stop()
}

type realTicker struct {
	t *time.Ticker
}

func (r *realTicker) C() <-chan time.Time { return r.t.C }
func (r *realTicker) Stop()               { r.t.Stop() }

func newRealTicker(d time.Duration) ticker {
	return &realTicker{t: time.NewTicker(d)}
}

func sanitizeKey(key string) string {
	return strings.NewReplacer("/", "_", "\\", "_").Replace(key)
}

func (s *fileSpool) activePath(key string) string {
	return s.dir + "/" + sanitizeKey(key) + spoolExt
}

// isInflightFile reports whether name is an inflight spool file.
func isInflightFile(name string) bool {
	idx := strings.Index(name, inflightMarker)
	if idx < 0 {
		return false
	}

	timestamp := name[idx+len(inflightMarker):]
	if timestamp == "" {
		return false
	}

	for _, ch := range timestamp {
		if ch < '0' || ch > '9' {
			return false
		}
	}

	return true
}

// isActiveFile reports whether name is an active spool file (not inflight).
func isActiveFile(name string) bool {
	return strings.HasSuffix(name, spoolExt) && !isInflightFile(name)
}

// keyFromInflight extracts the key from an inflight filename
// (everything before the inflightMarker).
func keyFromInflight(name string) string {
	idx := strings.Index(name, inflightMarker)
	if idx < 0 {
		return ""
	}
	return name[:idx]
}

// Append implements Spool.
func (s *fileSpool) Append(key string, payload []byte) error {
	startedAt := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("spool is closed")
	}

	if len(payload) > maxPayloadSz {
		return fmt.Errorf("payload too large for key %q: %d bytes", key, len(payload))
	}

	if err := s.appendToFile(s.activePath(key), payload); err != nil {
		return err
	}

	if s.maxActiveSize > 0 {
		if err := s.maybeRotateActive(key); err != nil {
			return err
		}
	}

	s.appendBytes.Add(int64(len(payload)))
	s.appendCount.Add(1)

	if s.shouldTriggerFlush() {
		signalNonBlocking(s.triggerCh)
	}

	recordAppendMetrics(s.dir, len(payload), time.Since(startedAt))

	return nil
}

func (s *fileSpool) shouldTriggerFlush() bool {
	if s.maxBytesBeforeFlush > 0 && s.appendBytes.Load() >= s.maxBytesBeforeFlush {
		return true
	}
	if s.maxAppendsBeforeFlush > 0 && int(s.appendCount.Load()) >= s.maxAppendsBeforeFlush {
		return true
	}
	return false
}

func signalNonBlocking(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (s *fileSpool) start() {
	if s.flushInterval <= 0 && s.maxBytesBeforeFlush <= 0 && s.maxAppendsBeforeFlush <= 0 {
		return
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		var tickC <-chan time.Time
		var tk ticker
		if s.flushInterval > 0 {
			tk = s.newTicker(s.flushInterval)
			tickC = tk.C()
		}
		if tk != nil {
			defer tk.Stop()
		}

		for {
			select {
			case <-tickC:
				s.runFlushCycle()
			case <-s.triggerCh:
				s.runFlushCycle()
			case <-s.stopCh:
				return
			}
		}
	}()
}

func (s *fileSpool) runFlushCycle() {
	bytesAtStart := s.appendBytes.Load()
	countAtStart := s.appendCount.Load()

	if err := s.flush(); err != nil {
		logrus.Errorf("flush cycle failed: %v", err)
		s.scheduleRetryAfterFailedFlush()
		return
	}

	s.subtractAppendBytes(bytesAtStart)
	s.subtractAppendCount(countAtStart)
}

func (s *fileSpool) scheduleRetryAfterFailedFlush() {
	if s.flushInterval > 0 {
		return
	}
	if s.maxBytesBeforeFlush <= 0 && s.maxAppendsBeforeFlush <= 0 {
		return
	}
	if !s.shouldTriggerFlush() {
		return
	}

	go func() {
		timer := time.NewTimer(flushRetryDelay)
		defer timer.Stop()

		select {
		case <-timer.C:
			select {
			case <-s.stopCh:
				return
			default:
			}
			signalNonBlocking(s.triggerCh)
		case <-s.stopCh:
		}
	}()
}

func (s *fileSpool) subtractAppendBytes(delta int64) {
	if delta <= 0 {
		return
	}

	for {
		current := s.appendBytes.Load()
		next := current - delta
		if next < 0 {
			next = 0
		}
		if s.appendBytes.CompareAndSwap(current, next) {
			return
		}
	}
}

func (s *fileSpool) subtractAppendCount(delta int32) {
	if delta <= 0 {
		return
	}

	for {
		current := s.appendCount.Load()
		next := current - delta
		if next < 0 {
			next = 0
		}
		if s.appendCount.CompareAndSwap(current, next) {
			return
		}
	}
}

// maybeRotateActive checks if the active file for key exceeds maxActiveSize
// and renames it to an inflight file if so. Must be called with s.mu held.
func (s *fileSpool) maybeRotateActive(key string) error {
	path := s.activePath(key)
	info, err := s.fs.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat active file %q: %w", path, err)
	}
	if info.Size() < s.maxActiveSize {
		return nil
	}
	inflight := fmt.Sprintf("%s/%s%s%d", s.dir, sanitizeKey(key), inflightMarker, s.nowFunc().UnixNano())
	if err := s.fs.Rename(path, inflight); err != nil {
		return fmt.Errorf("rotating active file %q: %w", path, err)
	}
	return nil
}

func (s *fileSpool) appendToFile(path string, payload []byte) error {
	f, err := s.fs.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, filePerms)
	if err != nil {
		return fmt.Errorf("opening spool file %q: %w", path, err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			logrus.Errorf("closing spool file %q: %v", path, closeErr)
		}
	}()

	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header, uint32(len(payload))) //nolint:gosec // checked above
	if _, err := f.Write(header); err != nil {
		return fmt.Errorf("writing header to %q: %w", path, err)
	}
	if _, err := f.Write(payload); err != nil {
		return fmt.Errorf("writing payload to %q: %w", path, err)
	}

	if syncer, ok := f.(interface{ Sync() error }); ok {
		if err := syncer.Sync(); err != nil {
			return fmt.Errorf("syncing spool file %q: %w", path, err)
		}
	}

	return nil
}

// collectInflight renames active spool files to inflight and collects all
// inflight files grouped by key. Must be called with s.mu held; the caller
// must release it after this returns.
// Returns a map of key -> sorted inflight paths (oldest first).
func (s *fileSpool) collectInflight() (map[string][]string, error) {
	entries, err := afero.ReadDir(s.fs, s.dir)
	if err != nil {
		return nil, fmt.Errorf("reading spool directory: %w", err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if !isActiveFile(name) {
			continue
		}
		key := strings.TrimSuffix(name, spoolExt)
		active := s.dir + "/" + name
		inflight := fmt.Sprintf("%s/%s%s%d", s.dir, key, inflightMarker, s.nowFunc().UnixNano())

		if err := s.fs.Rename(active, inflight); err != nil {
			return nil, fmt.Errorf("renaming %q to inflight: %w", active, err)
		}
	}

	entries, err = afero.ReadDir(s.fs, s.dir)
	if err != nil {
		return nil, fmt.Errorf("reading spool directory after rename: %w", err)
	}

	byKey := make(map[string][]string)
	for _, entry := range entries {
		name := entry.Name()
		if !isInflightFile(name) {
			continue
		}
		key := keyFromInflight(name)
		byKey[key] = append(byKey[key], s.dir+"/"+name)
	}

	for _, paths := range byKey {
		sort.Strings(paths)
	}

	return byKey, nil
}

func (s *fileSpool) flush() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return fmt.Errorf("spool is closed")
	}

	byKey, err := s.collectInflight()
	if err != nil {
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()

	var flushErr error
	for key, paths := range byKey {
		if err := s.flushKey(key, paths, s.handler); err != nil {
			flushErr = err
		}
	}

	return flushErr
}

func (s *fileSpool) flushKey(key string, paths []string, fn FlushHandler) error {
	startedAt := time.Now()
	defer func() {
		recordFlushKeyProcessingLatency(s.dir, time.Since(startedAt))
	}()

	for i, path := range paths {
		if err := s.flushOneInflight(path); err != nil {
			if errors.Is(err, errEmptyInflight) {
				continue
			}
			return fmt.Errorf("reading inflight file %q: %w", path, err)
		}

		next, cleanup := s.makeNextFunc(path)
		fnErr := fn(key, next)
		cleanup()

		if fnErr != nil {
			return s.handleFlushFailure(key, paths[i:], fnErr)
		}

		s.failMu.Lock()
		delete(s.failuresByKey, key)
		s.failMu.Unlock()
		s.removeInflight(path)
	}
	return nil
}

var errEmptyInflight = fmt.Errorf("empty inflight file")

func (s *fileSpool) flushOneInflight(path string) error {
	info, err := s.fs.Stat(path)
	if err != nil {
		return fmt.Errorf("stat inflight %q: %w", path, err)
	}
	if info.Size() == 0 {
		s.removeInflight(path)
		return errEmptyInflight
	}
	return nil
}

type frameReader struct {
	file         afero.File
	path         string
	header       []byte
	pendingFrame []byte
	done         bool
}

func newFrameReader(fs afero.Fs, path string) (*frameReader, error) {
	f, err := fs.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening %q: %w", path, err)
	}
	return &frameReader{file: f, path: path, header: make([]byte, headerSize)}, nil
}

func (r *frameReader) readFrame() ([]byte, error) {
	if r.done {
		return nil, io.EOF
	}

	if r.pendingFrame != nil {
		frame := r.pendingFrame
		r.pendingFrame = nil
		return frame, nil
	}

	_, err := io.ReadFull(r.file, r.header)
	if err != nil {
		if err == io.EOF {
			r.done = true
			return nil, io.EOF
		}
		if err == io.ErrUnexpectedEOF {
			logrus.Warnf("truncated frame header in %q; stopping incremental read", r.path)
			r.done = true
			return nil, io.EOF
		}
		return nil, fmt.Errorf("reading frame header from %q: %w", r.path, err)
	}

	size := binary.LittleEndian.Uint32(r.header)
	payload := make([]byte, size)
	_, err = io.ReadFull(r.file, payload)
	if err != nil {
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			logrus.Warnf("truncated frame payload in %q (expected %d bytes); stopping incremental read", r.path, size)
			r.done = true
			return nil, io.EOF
		}
		return nil, fmt.Errorf("reading frame payload from %q: %w", r.path, err)
	}

	return payload, nil
}

func (r *frameReader) unreadFrame(frame []byte) {
	r.pendingFrame = frame
}

func (r *frameReader) close() {
	if err := r.file.Close(); err != nil {
		logrus.Errorf("closing frame reader for %q: %v", r.path, err)
	}
}

func (s *fileSpool) makeNextFunc(path string) (next func() ([][]byte, error), cleanup func()) {
	var (
		reader    *frameReader
		exhausted bool
	)

	closeReader := func() {
		if reader != nil {
			reader.close()
			reader = nil
		}
	}

	next = func() ([][]byte, error) {
		if exhausted {
			return nil, io.EOF
		}

		if reader == nil {
			var err error
			reader, err = newFrameReader(s.fs, path)
			if err != nil {
				return nil, fmt.Errorf("reading frames from %q: %w", path, err)
			}
		}

		batch, err := s.readBatch(reader)
		if err != nil {
			closeReader()
			return nil, err
		}

		if len(batch) == 0 {
			closeReader()
			exhausted = true
			return nil, io.EOF
		}

		recordFlushReturnedBatchMetrics(s.dir, batchPayloadBytes(batch))

		return batch, nil
	}

	return next, closeReader
}

func (s *fileSpool) readBatch(r *frameReader) ([][]byte, error) {
	var (
		batch        [][]byte
		payloadBytes int64
	)

	for {
		frame, err := r.readFrame()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return batch, nil
			}
			return nil, err
		}

		frameBytes := int64(len(frame))
		if len(batch) > 0 && s.flushBatchMaxBytes > 0 && payloadBytes+frameBytes > s.flushBatchMaxBytes {
			r.unreadFrame(frame)
			return batch, nil
		}

		batch = append(batch, frame)
		payloadBytes += frameBytes

		if s.flushBatchSize > 0 && len(batch) >= s.flushBatchSize {
			return batch, nil
		}
	}
}

func batchPayloadBytes(batch [][]byte) int64 {
	var bytes int64
	for _, frame := range batch {
		bytes += int64(len(frame))
	}
	return bytes
}

func (s *fileSpool) removeInflight(path string) {
	if err := s.fs.Remove(path); err != nil && !os.IsNotExist(err) {
		logrus.Errorf("removing inflight file %q: %v", path, err)
	}
}

func (s *fileSpool) handleFlushFailure(key string, allPaths []string, fnErr error) error {
	s.failMu.Lock()
	s.failuresByKey[key]++
	count := s.failuresByKey[key]
	s.failMu.Unlock()

	if count >= s.maxFailures {
		logrus.Errorf(
			"exceeded failure threshold for key %q after %d consecutive failures (threshold: %d)",
			key, count, s.maxFailures,
		)
		for _, p := range allPaths {
			if stratErr := s.failureStrategy.OnExceededFailures(s.fs, p); stratErr != nil {
				logrus.Errorf("failure strategy error for key %q, file %q: %v", key, p, stratErr)
				return fmt.Errorf("failure strategy for key %q: %w", key, stratErr)
			}
		}
		s.failMu.Lock()
		delete(s.failuresByKey, key)
		s.failMu.Unlock()
		return nil
	}

	logrus.Warnf("flush callback failed for key %q (%d/%d): %v", key, count, s.maxFailures, fnErr)
	return fnErr
}

func (s *fileSpool) recover() error {
	entries, err := afero.ReadDir(s.fs, s.dir)
	if err != nil {
		return fmt.Errorf("reading spool directory for recovery: %w", err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if !isInflightFile(name) {
			continue
		}

		inflightPath := s.dir + "/" + name

		if entry.Size() == 0 {
			if removeErr := s.fs.Remove(inflightPath); removeErr != nil {
				return fmt.Errorf("removing empty inflight file %q during recovery: %w", inflightPath, removeErr)
			}
		}
	}

	return nil
}

func (s *fileSpool) close() error {
	var closeErr error
	s.closeOnce.Do(func() {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		s.closed = true
		s.mu.Unlock()

		close(s.stopCh)
		s.wg.Wait()

		if s.flushOnClose {
			closeErr = s.flushClosed()
		}
	})

	return closeErr
}

func (s *fileSpool) flushClosed() error {
	s.mu.Lock()
	byKey, err := s.collectInflight()
	s.mu.Unlock()
	if err != nil {
		return err
	}

	var flushErr error
	for key, paths := range byKey {
		if err := s.flushKey(key, paths, s.handler); err != nil {
			flushErr = err
		}
	}
	return flushErr
}
