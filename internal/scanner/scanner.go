// Package scanner provides parallel filesystem scanning for duplicate detection.
//
// # Architecture Overview
//
// The scanner uses a concurrent fan-out/fan-in architecture to efficiently
// traverse directory trees while respecting system resource limits.
//
// # Concurrency Model
//
// The scanner employs three concurrent components:
//
//  1. WALKER GOROUTINES (fan-out)
//     - One goroutine spawned per directory discovered
//     - Concurrency limited by semaphore (walkerSem)
//     - Each walker: acquires semaphore → lists directory → releases semaphore → spawns child walkers
//
//  2. COLLECTOR GOROUTINE (fan-in)
//     - Single goroutine that drains resultCh into a slice
//     - Provides the aggregation point for all walker outputs
//     - Runs until resultCh is closed
//
//  3. MAIN GOROUTINE (orchestrator)
//     - Initializes channels and spawns initial walkers
//     - Waits for all walkers (walkerWg.Wait)
//     - Closes resultCh to signal collector
//     - Waits for collector (collectorWg.Wait)
//
// # Synchronization Primitives
//
//	┌─────────────────┬────────────────────────────────────────────────┐
//	│ Primitive       │ Purpose                                        │
//	├─────────────────┼────────────────────────────────────────────────┤
//	│ walkerSem       │ Limits concurrent directory reads (backpressure)│
//	│ walkerWg        │ Tracks active walker goroutines                │
//	│ collectorWg     │ Signals collector goroutine completion         │
//	│ resultCh        │ Buffered channel for matched files (fan-in)    │
//	│ atomic counters │ Lock-free stats updates from any goroutine     │
//	└─────────────────┴────────────────────────────────────────────────┘
//
// # Data Flow
//
//	Run() starts
//	    │
//	    ├──► spawn collector goroutine (reads resultCh)
//	    │
//	    ├──► for each root path:
//	    │        └──► walkDirectory(path)
//	    │                 │
//	    │                 ├──► acquire semaphore (blocks if at limit)
//	    │                 ├──► listDirectory() → files, subdirs
//	    │                 ├──► filter files → send matches to resultCh
//	    │                 └──► for each subdir: walkDirectory(subdir)  [recursive fan-out]
//	    │                 ├──► release semaphore
//	    │
//	    ├──► walkerWg.Wait() [all directories processed]
//	    ├──► close(resultCh) [signal collector to finish]
//	    ├──► collectorWg.Wait() [collector drained channel]
//	    │
//	    └──► return results
//
// # Why This Design?
//
//   - Semaphore controls concurrent directory reads
//   - Atomic counters eliminate lock contention for stats updates
//   - Buffered channel (1000) smooths producer/consumer rate differences
//   - Single collector avoids slice synchronization complexity
//   - Recursive spawning naturally handles arbitrary directory depth
package scanner

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/ivoronin/dupedog/internal/progress"
	"github.com/ivoronin/dupedog/internal/types"
)

// Scanner discovers files matching filter criteria using parallel directory traversal.
//
// The scanner is designed for single-use: create with New(), call Run() once.
type Scanner struct {
	// Config (immutable, set by New)
	paths        []string   // Root paths to scan
	minSize      int64      // Minimum file size filter (bytes)
	excludes     []string   // Glob patterns for filename exclusion
	workers      int        // Max concurrent directory reads
	showProgress bool       // Whether to display progress bar
	errCh        chan error // Non-fatal errors (permission denied, etc.)

	// Runtime (initialized in Run)
	walkerWg  sync.WaitGroup       // Tracks in-flight walker goroutines
	walkerSem types.Semaphore      // Limits concurrent directory reads
	resultCh  chan *types.FileInfo // Fan-in channel: walkers → collector
	stats     *stats               // Atomic counters for progress tracking
	bar       *progress.Bar        // Progress display (thread-safe)
}

// New creates a Scanner for discovering files.
func New(paths []string, minSize int64, excludes []string, workers int, showProgress bool, errCh chan error) *Scanner {
	return &Scanner{
		paths:        paths,
		minSize:      minSize,
		excludes:     excludes,
		workers:      workers,
		showProgress: showProgress,
		errCh:        errCh,
	}
}

// stats tracks scanning progress using atomic counters for lock-free updates.
//
// Atomic counters allow multiple walker goroutines to update stats concurrently
// without mutex contention. Each walker calls Add() which is guaranteed atomic.
// The collector (String method) calls Load() to read consistent snapshots.
//
// Trade-off: Individual reads may not see a perfectly consistent view across
// all four counters (scannedFiles might be newer than matchedFiles), but this
// is acceptable for progress display where exactness isn't required.
type stats struct {
	scannedFiles atomic.Int64 // Total files discovered (all walkers)
	matchedFiles atomic.Int64 // Files passing size/exclude filters
	scannedBytes atomic.Int64 // Total bytes across all scanned files
	matchedBytes atomic.Int64 // Bytes of matched files only
	startTime    time.Time    // For elapsed time calculation
}

func (s *stats) String() string {
	return fmt.Sprintf("Scanned %d (%s), matched %d files (%s) in %.1fs",
		s.scannedFiles.Load(), humanize.IBytes(uint64(s.scannedBytes.Load())),
		s.matchedFiles.Load(), humanize.IBytes(uint64(s.matchedBytes.Load())),
		time.Since(s.startTime).Seconds())
}

// Run executes the scan and returns matching files.
//
// Coordination sequence:
//  1. Start collector goroutine (drains resultCh → results slice)
//  2. Spawn walker for each root path (fan-out begins)
//  3. Wait for all walkers to complete (walkerWg.Wait)
//  4. Close resultCh to signal collector to finish
//  5. Wait for collector to drain remaining items (collectorWg.Wait)
//  6. Return aggregated results
//
// The buffered channel (1000) prevents walkers from blocking on slow collection,
// while the WaitGroup ensures we don't close the channel prematurely.
func (s *Scanner) Run() []*types.FileInfo {
	// Initialize runtime fields
	s.walkerSem = types.NewSemaphore(s.workers)
	s.bar = progress.New(s.showProgress, -1)
	s.stats = &stats{startTime: time.Now()}
	s.bar.Describe(s.stats) // Render progress bar immediately
	s.resultCh = make(chan *types.FileInfo, 1000) // Buffer smooths producer/consumer rates

	// Collector goroutine: single consumer aggregates all walker outputs.
	// Runs until resultCh is closed, then signals completion via collectorWg.
	var results []*types.FileInfo
	collectorWg := sync.WaitGroup{}

	collectorWg.Add(1)
	go func() {
		for r := range s.resultCh {
			results = append(results, r)
		}
		collectorWg.Done()
	}()

	// Spawn initial walkers for each root path (fan-out entry point)
	for _, p := range s.paths {
		absPath, err := filepath.Abs(p)
		if err != nil {
			s.sendError(err)
			continue
		}
		s.walkDirectory(absPath)
	}

	// Shutdown sequence: wait for producers, then signal consumer, then wait for consumer
	s.walkerWg.Wait()   // All walkers done
	close(s.resultCh)   // Signal collector: no more items coming
	collectorWg.Wait()  // Collector drained channel

	s.bar.Finish(s.stats)
	return results
}

// walkDirectory spawns a goroutine to process one directory and recursively spawn children.
//
// Semaphore pattern:
//   - walkerWg.Add(1) BEFORE goroutine spawn (prevents race with Wait)
//   - acquire semaphore at goroutine start (blocks if at concurrency limit)
//   - release semaphore AFTER listing but BEFORE spawning children
//     (allows children to acquire while parent processes files)
//
// This creates a "breadth-controlled depth-first" traversal where the semaphore
// limits how many directories are being read simultaneously, but doesn't limit
// the total number of pending goroutines (which is bounded by directory count).
func (s *Scanner) walkDirectory(dir string) {
	s.walkerWg.Add(1) // Increment BEFORE spawn to prevent race with Wait()
	go func() {
		defer s.walkerWg.Done()

		// Semaphore limits concurrent directory reads
		s.walkerSem.Acquire()
		defer s.walkerSem.Release()

		files, subdirs, err := s.listDirectory(dir)
		if err != nil {
			s.sendError(err)
			return
		}

		// Process files: atomic stats + channel send (no locks needed)
		for _, f := range files {
			s.stats.scannedFiles.Add(1)
			s.stats.scannedBytes.Add(f.Size)
			if f.Size >= s.minSize && !s.shouldExclude(f.Path) {
				s.resultCh <- f // May block briefly if channel buffer full
				s.stats.matchedFiles.Add(1)
				s.stats.matchedBytes.Add(f.Size)
			}
		}
		s.bar.Describe(s.stats)

		// Recursive fan-out: spawn walker for each subdirectory
		for _, sub := range subdirs {
			s.walkDirectory(sub)
		}
	}()
}

// listDirectory reads a single directory, returning files and subdirectories.
//
// Uses batched ReadDir (1000 entries per batch) to handle large directories efficiently.
// This is the ONLY place where directory I/O occurs - protected by walkerSem.
//
// Filtering:
//   - Directories → subdirs (for recursive walking)
//   - Regular files → files (with metadata via Info())
//   - Symlinks, devices, etc. → skipped
func (s *Scanner) listDirectory(dirPath string) (files []*types.FileInfo, subdirs []string, err error) {
	dir, err := os.Open(dirPath)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = dir.Close() }()

	// Batch reading: ReadDir(n) returns up to n entries at a time.
	// This bounds memory usage when listing directories with millions of files.
	const batchSize = 1000
	for {
		entries, err := dir.ReadDir(batchSize)
		if len(entries) == 0 {
			if err != nil && err != io.EOF {
				return files, subdirs, err
			}
			break
		}

		for _, entry := range entries {
			f, sub := s.processEntry(dirPath, entry)
			if f != nil {
				files = append(files, f)
			}
			if sub != "" {
				subdirs = append(subdirs, sub)
			}
		}
	}

	return files, subdirs, nil
}

// processEntry processes a single directory entry, returning a file or subdirectory path.
// Returns (nil, "") for entries that should be skipped (symlinks, devices, excluded items).
func (s *Scanner) processEntry(dirPath string, entry os.DirEntry) (file *types.FileInfo, subdir string) {
	fullPath := filepath.Join(dirPath, entry.Name())

	if entry.IsDir() {
		if s.shouldExclude(fullPath) {
			return nil, ""
		}
		return nil, fullPath
	}

	// Skip non-regular files (symlinks, devices, sockets, etc.)
	if !entry.Type().IsRegular() {
		return nil, ""
	}

	// Info() may trigger additional stat call (platform-dependent)
	info, err := entry.Info()
	if err != nil {
		return nil, "" // Skip files we can't stat (race condition, permissions)
	}

	return newFileInfo(fullPath, info), ""
}

// sendError sends an error to the errors channel if it's not nil.
func (s *Scanner) sendError(err error) {
	if s.errCh != nil {
		s.errCh <- err
	}
}

// shouldExclude checks if a path matches any glob exclude pattern.
func (s *Scanner) shouldExclude(path string) bool {
	if len(s.excludes) == 0 {
		return false
	}
	base := filepath.Base(path)
	for _, pattern := range s.excludes {
		if matched, _ := filepath.Match(pattern, base); matched {
			return true
		}
	}
	return false
}
