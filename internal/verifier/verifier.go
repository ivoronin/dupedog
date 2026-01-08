// Package verifier confirms duplicates using progressive content hashing.
//
// # Architecture Overview
//
// The verifier uses progressive hashing to efficiently confirm duplicate candidates.
// Instead of hashing entire files upfront, it hashes in stages: head → tail → chunks,
// eliminating non-duplicates as early as possible to minimize I/O.
//
// # Sibling Group Optimization
//
// Files in the same sibling group (same dev+ino) are hardlinks - they are guaranteed
// to have identical content. The verifier hashes only ONE representative file per
// sibling group, preserving all paths for later deduplication decisions.
//
// # Concurrency Model
//
// The verifier employs three concurrent components:
//
//  1. WORKER GOROUTINES (fixed pool)
//     - N workers (configurable) consume jobs from the queue
//     - Each worker processes one job at a time
//     - Jobs spawn sibling-group-level goroutines limited by semaphore
//
//  2. COLLECTOR (main goroutine)
//     - Reads from results channel
//     - Aggregates confirmed duplicate groups
//     - Runs until results channel closed
//
//  3. ORCHESTRATOR (goroutines)
//     - Queues initial jobs and closes queue when pending work done
//     - Closes results when workers complete
//
// # Synchronization Primitives
//
//	┌─────────────────┬────────────────────────────────────────────────┐
//	│ Primitive       │ Purpose                                        │
//	├─────────────────┼────────────────────────────────────────────────┤
//	│ workerSem       │ Limits concurrent file reads (backpressure)    │
//	│ pending         │ Tracks jobs (initial + spawned) for completion │
//	│ workerWg        │ Signals worker pool completion                 │
//	│ jobCh           │ Buffered channel for jobs (fan-in/fan-out)     │
//	│ resultsCh       │ Buffered channel for confirmed duplicates      │
//	└─────────────────┴────────────────────────────────────────────────┘
//
// # Data Flow
//
//	Run() starts
//	    │
//	    ├──► start N workers (consume queue)
//	    │
//	    ├──► queue initial jobs (one per candidate group)
//	    │        │
//	    │        └──► pending.Add(len(groups))
//	    │
//	    ├──► goroutine: pending.Wait() → close(queue)
//	    │
//	    ├──► goroutine: workerWg.Wait() → close(results)
//	    │
//	    └──► collect from results → return duplicates
//
//	Worker processes job:
//	    │
//	    ├──► verifyFilesInJob() [sem-limited concurrency]
//	    │        │
//	    │        └──► hash one file per sibling group → group by hash
//	    │
//	    └──► for each group with 2+ sibling groups:
//	             ├──► if done → send to results as DuplicateGroup
//	             └──► else → pending.Add(1), queue next stage
//
// # Progressive Verification Strategy
//
//	File Size < 1MB:      [0, fileSize) → done (single read)
//	File Size 1MB-2MB:    [0, 1MB) → [fileSize-1MB, fileSize) → done (HEAD + TAIL cover it)
//	File Size > 2MB:      HEAD → TAIL → CHUNK[0] → [CHUNK[1]...] → done
//	                      Chunks cover [1MB, fileSize-1MB), avoiding overlap with probes
//
// # Why This Design?
//
//   - Progressive hashing minimizes I/O for non-duplicates (eliminated early)
//   - Sibling group optimization reduces I/O (hardlinks hashed once)
//   - Semaphore controls concurrent file reads (prevents fd exhaustion)
//   - Fixed worker pool bounds goroutine count
//   - Job spawning handles arbitrary file sizes with chunked verification
//   - Buffered channels smooth producer/consumer rate differences
package verifier

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/ivoronin/dupedog/internal/cache"
	"github.com/ivoronin/dupedog/internal/progress"
	"github.com/ivoronin/dupedog/internal/types"
)

// Verification constants
const (
	// probeSize is the size of head/tail probes (1MB)
	probeSize = 1 << 20
	// chunkSize is the chunk size for file content hashing (1GB)
	chunkSize = 1 << 30
	// blockSize is the read buffer size (64KB)
	blockSize = 64 * 1024
)

// fmtBytes is a shorthand for humanize.IBytes (human-readable byte sizes).
var fmtBytes = humanize.IBytes

// job represents a unit of verification work.
// Contains sibling groups to verify at a specific byte range.
type job struct {
	siblings   types.CandidateGroup // Sibling groups being verified
	start      int64                // Byte offset to read
	size       int64                // Number of bytes to read
	totalBytes int64                // Cumulative bytes read INCLUDING this job
}

// stats tracks verification progress.
type stats struct {
	totalCandidateBytes uint64        // total bytes to verify (calculated upfront)
	verifiedBytes       atomic.Uint64 // hashed data for output
	skippedBytes        atomic.Uint64 // bytes avoided due to early elimination
	cachedBytes         atomic.Uint64 // bytes retrieved from cache (skipped I/O)
	confirmedCandidates atomic.Int64  // number of confirmed duplicates
	confirmedBytes      atomic.Uint64 // bytes in confirmed duplicates
	confirmedSets       atomic.Int64  // number of confirmed duplicate sets
	startTime           time.Time
}

func (s *stats) String() string {
	elapsed := time.Since(s.startTime).Truncate(time.Millisecond)
	verified := s.verifiedBytes.Load()
	skipped := s.skippedBytes.Load()
	cached := s.cachedBytes.Load()
	total := verified + skipped + cached
	pct := 0.0
	if s.totalCandidateBytes > 0 {
		pct = float64(total) / float64(s.totalCandidateBytes) * 100
	}
	if cached > 0 {
		return fmt.Sprintf("Verified %s + cached %s + skipped %s out of %s (%.0f%%), confirmed %d duplicates (%s) in %d sets in %v",
			fmtBytes(verified), fmtBytes(cached), fmtBytes(skipped), fmtBytes(s.totalCandidateBytes),
			pct, s.confirmedCandidates.Load(), fmtBytes(s.confirmedBytes.Load()), s.confirmedSets.Load(), elapsed)
	}
	return fmt.Sprintf("Verified %s + skipped %s out of %s (%.0f%%), confirmed %d duplicates (%s) in %d sets in %v",
		fmtBytes(verified), fmtBytes(skipped), fmtBytes(s.totalCandidateBytes),
		pct, s.confirmedCandidates.Load(), fmtBytes(s.confirmedBytes.Load()), s.confirmedSets.Load(), elapsed)
}

// Verifier confirms duplicates among candidate groups using progressive hashing.
//
// The verifier is designed for single-use: create with New(), call Run() once.
type Verifier struct {
	// Config (immutable, set by New)
	groups       types.CandidateGroups // Input: candidate groups from screener
	workers      int                   // Max concurrent file reads
	showProgress bool                  // Whether to display progress bar
	errCh        chan error            // Non-fatal errors (permission denied, etc.)
	cache        *cache.Cache      // Optional hash cache (nil = disabled)

	// Runtime (initialized in Run)
	jobCh     chan job                  // Jobs to process
	resultsCh chan types.DuplicateGroup // Output: confirmed duplicate groups
	workerSem types.Semaphore           // Limits concurrent file reads
	pending   sync.WaitGroup            // Tracks pending jobs
	workerWg  sync.WaitGroup            // Tracks worker goroutines
	bar       *progress.Bar             // Progress display (thread-safe)
	stats     *stats                    // Progress tracking
}

// New creates a Verifier for confirming duplicates among candidate groups.
// Use cache.Open("") for disabled cache; nil will panic.
func New(groups types.CandidateGroups, workers int, showProgress bool, errCh chan error, hashCache *cache.Cache) *Verifier {
	return &Verifier{
		groups:       groups,
		workers:      workers,
		showProgress: showProgress,
		errCh:        errCh,
		cache:        hashCache,
	}
}

// Run executes progressive verification and returns confirmed duplicate groups.
//
// Coordination sequence:
//  1. Initialize runtime fields (channels, semaphore, progress)
//  2. Start N worker goroutines (consume from queue)
//  3. Queue initial jobs (one per candidate group)
//  4. Goroutine: Wait for pending jobs → close queue
//  5. Goroutine: Wait for workers → close results
//  6. Collect confirmed duplicates from results channel
//  7. Return aggregated results
//
// Progressive verification strategy:
//   - < 1MB: CHUNK[0] → done  (single chunk covers whole file)
//   - ≥ 1MB: HEAD → TAIL → CHUNK[0] → [CHUNK[1] → ...] → done
func (v *Verifier) Run() types.DuplicateGroups {
	if v.groups.Len() == 0 {
		return types.NewDuplicateGroups(nil)
	}

	// Calculate total candidate bytes upfront
	var totalCandidateBytes uint64
	for _, cg := range v.groups.Items() {
		fileSize := uint64(cg.First().First().Size)
		totalCandidateBytes += fileSize * uint64(cg.Len())
	}

	// Initialize runtime fields
	v.jobCh = make(chan job, 1000)
	v.resultsCh = make(chan types.DuplicateGroup, 100)
	v.workerSem = types.NewSemaphore(v.workers)
	v.bar = progress.New(v.showProgress, -1) // Spinner mode
	v.stats = &stats{totalCandidateBytes: totalCandidateBytes, startTime: time.Now()}
	v.bar.Describe(v.stats) // Render progress bar immediately

	// Start workers
	for i := 0; i < v.workers; i++ {
		v.workerWg.Add(1)
		go func() {
			defer v.workerWg.Done()
			for j := range v.jobCh {
				v.processJob(j)
			}
		}()
	}

	// Queue initial jobs (one per candidate group)
	v.pending.Add(v.groups.Len())
	go func() {
		for _, candidateGroup := range v.groups.Items() {
			j, _ := nextJob(nil, candidateGroup)
			v.jobCh <- j
		}
	}()

	// Close jobCh when all jobs complete
	go func() {
		v.pending.Wait()
		close(v.jobCh)
	}()

	// Close resultsCh when workers done
	go func() {
		v.workerWg.Wait()
		close(v.resultsCh)
	}()

	// Collect confirmed duplicates
	var duplicates []types.DuplicateGroup
	for group := range v.resultsCh {
		duplicates = append(duplicates, group)
		// Track confirmed duplicate stats (exclude original - only count files to be replaced)
		v.stats.confirmedCandidates.Add(int64(group.Len() - 1))
		v.stats.confirmedBytes.Add(uint64(group.First().First().Size) * uint64(group.Len()-1))
		v.stats.confirmedSets.Add(1)
		v.bar.Describe(v.stats)
	}

	v.bar.Finish(v.stats)
	return types.NewDuplicateGroups(duplicates)
}

// hashResult pairs a sibling group with its computed hash for aggregation.
type hashResult struct {
	hash     string
	siblings types.SiblingGroup
}

// verifyFilesInJob verifies sibling groups in a job with semaphore-limited concurrency.
//
// Spawns a goroutine per sibling group, limited by semaphore to prevent fd exhaustion.
// Hashes only ONE representative file per sibling group (same inode = identical content).
// Returns sibling groups grouped by their hash - groups with 2+ siblings are potential duplicates.
func (v *Verifier) verifyFilesInJob(j job) map[string][]types.SiblingGroup {
	results := make(chan hashResult, j.siblings.Len())
	var wg sync.WaitGroup

	for _, siblings := range j.siblings.Items() {
		wg.Add(1)
		go func(sibs types.SiblingGroup) {
			defer wg.Done()
			v.workerSem.Acquire()
			defer v.workerSem.Release()

			// Hash only the first file - all siblings are hardlinks with identical content
			rep := sibs.First()

			// Try cache first
			cachedHash, err := v.cache.Lookup(rep, j.start, j.size)
			if err != nil {
				v.sendError(fmt.Errorf("cache lookup %s: %w", rep.Path, err))
				// Continue with hash computation on cache error
			}
			if cachedHash != nil {
				v.stats.cachedBytes.Add(uint64(j.size))
				v.bar.Describe(v.stats)
				results <- hashResult{hex.EncodeToString(cachedHash), sibs}
				return
			}

			// Cache miss - compute hash
			hash, n, err := hashRange(rep.Path, j.start, j.size)
			if err != nil {
				v.sendError(fmt.Errorf("%s: %w", rep.Path, err))
				return
			}

			hashBytes, _ := hex.DecodeString(hash)
			if err := v.cache.Store(rep, j.start, j.size, hashBytes); err != nil {
				v.sendError(fmt.Errorf("cache store %s: %w", rep.Path, err))
			}
			v.stats.verifiedBytes.Add(uint64(n))
			v.bar.Describe(v.stats)

			results <- hashResult{hash, sibs}
		}(siblings)
	}
	wg.Wait()
	close(results)

	// Collect raw slices first (map iteration order is non-deterministic)
	byHash := make(map[string][]types.SiblingGroup)
	for r := range results {
		byHash[r.hash] = append(byHash[r.hash], r.siblings)
	}
	return byHash
}

// processJob verifies sibling groups, splits by hash, and routes results.
//
// For each hash group with 2+ sibling groups:
//   - If verification complete → send to results channel (confirmed duplicates)
//   - If more ranges needed → queue next job (pending.Add + queue send)
func (v *Verifier) processJob(j job) {
	defer v.pending.Done()

	for _, rawSiblings := range v.verifyFilesInJob(j) {
		// Convert raw slice to sorted CandidateGroup
		candidateGroup := types.NewCandidateGroup(rawSiblings)
		if candidateGroup.Len() < 2 {
			// Eliminated early - track bytes we avoided reading
			fileSize := candidateGroup.First().First().Size
			v.stats.skippedBytes.Add(uint64(fileSize - j.totalBytes))
			v.bar.Describe(v.stats)
			continue
		}
		if next, done := nextJob(&j, candidateGroup); done {
			v.resultsCh <- types.NewDuplicateGroup(candidateGroup.Items())
		} else {
			v.pending.Add(1)
			v.jobCh <- next // Need more verification
		}
	}
}

// nextJob returns the next verification job, or done=true if verification is complete.
//
// RULE: Never read the same byte twice.
//
// State machine (linear flow, early exits):
//
//	INITIAL     → emit HEAD [0, min(probeSize, fileSize))
//	DONE        → totalBytes == fileSize (handles ALL completion cases)
//	AFTER_HEAD  → medium: emit [probeSize, fileSize), large: emit TAIL
//	IN_CHUNKS   → emit next chunk in [probeSize, tailStart)
func nextJob(prev *job, candidateGroup types.CandidateGroup) (next job, done bool) {
	fileSize := candidateGroup.First().First().Size

	// ─────────────────────────────────────────────────
	// INITIAL → emit HEAD (or entire file if small)
	// ─────────────────────────────────────────────────
	if prev == nil {
		size := min(probeSize, fileSize)
		return job{siblings: candidateGroup, start: 0, size: size, totalBytes: size}, false
	}

	// ─────────────────────────────────────────────────
	// DONE → file fully verified
	// ─────────────────────────────────────────────────
	if prev.totalBytes == fileSize {
		return job{}, true
	}

	// ─────────────────────────────────────────────────
	// AFTER_HEAD → emit remaining (medium) or TAIL (large)
	// ─────────────────────────────────────────────────
	if prev.totalBytes == probeSize {
		remaining := fileSize - probeSize
		size := min(probeSize, remaining)
		start := max(probeSize, remaining)
		return job{siblings: candidateGroup, start: start, size: size, totalBytes: probeSize + size}, false
	}

	// ─────────────────────────────────────────────────
	// IN_CHUNKS → emit next chunk in [probeSize, tailStart)
	// ─────────────────────────────────────────────────
	start := prev.totalBytes - probeSize
	size := min(chunkSize, fileSize-prev.totalBytes)
	return job{siblings: candidateGroup, start: start, size: size, totalBytes: prev.totalBytes + size}, false
}

// sendError sends an error to the errors channel if it's not nil.
func (v *Verifier) sendError(err error) {
	if v.errCh != nil {
		v.errCh <- err
	}
}

// hashRange hashes a specific byte range of a file.
//
// Returns the SHA-256 hash (hex-encoded), bytes actually read, and any error.
// Uses blockSize buffer for efficient I/O.
func hashRange(path string, start, size int64) (hash string, n int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = f.Close() }()

	if _, err := f.Seek(start, io.SeekStart); err != nil {
		return "", 0, err
	}

	hasher := sha256.New()
	buf := make([]byte, blockSize)
	n, err = io.CopyBuffer(hasher, io.LimitReader(f, size), buf)
	if err != nil {
		return "", n, err
	}

	return hex.EncodeToString(hasher.Sum(nil)), n, nil
}
