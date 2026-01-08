// Package screener screens files to find duplicate candidates.
//
// # Overview
//
// The screener is the first filtering stage in the duplicate detection pipeline.
// It groups files by size and then by dev+ino (sibling groups), producing
// candidate groups for more expensive verification (hashing).
//
// # Processing Pipeline
//
//	Input: []*types.FileInfo (all scanned files)
//	    │
//	    ├──► Group by file size
//	    │
//	    ├──► Group by dev+ino (preserves all paths as SiblingGroups)
//	    │
//	    ├──► Filter: keep groups with 2+ unique inodes
//	    │
//	    └──► Output: types.CandidateGroups (candidate groups)
//
// # Why This Design?
//
//   - Size grouping is O(n) and eliminates most files cheaply
//   - Sibling grouping preserves ALL paths for each inode (critical for path priority)
//   - No I/O required - uses metadata from scanner
//   - Single-threaded (CPU-bound, not I/O-bound)
package screener

import (
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/ivoronin/dupedog/internal/progress"
	"github.com/ivoronin/dupedog/internal/types"
)

// Screener screens files by size to find potential duplicates.
//
// The screener is designed for single-use: create with New(), call Run() once.
type Screener struct {
	// Config (immutable, set by New)
	files                 []*types.FileInfo // Files to screen for duplicates
	showProgress          bool              // Whether to display progress bar
	trustDeviceBoundaries bool              // If true, use (dev,ino); if false, use ino only
}

// New creates a Screener for finding duplicate candidates.
//
// The trustDeviceBoundaries parameter controls how files are grouped:
//   - false (default): Group by inode only. Safe for NFS where same file
//     can appear with different device IDs across mount points.
//   - true: Group by (device, inode). Assumes each device has independent
//     inode spaces. WARNING: Unsafe if the same filesystem is mounted at
//     multiple paths (e.g., NFS mounted twice).
func New(files []*types.FileInfo, showProgress, trustDeviceBoundaries bool) *Screener {
	return &Screener{
		files:                 files,
		showProgress:          showProgress,
		trustDeviceBoundaries: trustDeviceBoundaries,
	}
}

// stats tracks screening progress.
type stats struct {
	candidateFiles int
	candidateBytes int64
	startTime      time.Time
}

func (s *stats) String() string {
	return fmt.Sprintf("Selected %d candidates (%s) in %.1fs",
		s.candidateFiles, humanize.IBytes(uint64(s.candidateBytes)),
		time.Since(s.startTime).Seconds())
}

// devIno uniquely identifies a file by device and inode.
// Used to detect hardlinks (different paths pointing to same file).
type devIno struct {
	dev, ino uint64
}

// Run screens files and returns candidate duplicate groups.
//
// Processing steps:
//  1. Group files by size (different sizes can't be duplicates)
//  2. Group by inode (or dev+ino if trustDeviceBoundaries) into sibling groups
//  3. Filter to groups with 2+ unique inodes (potential duplicates)
func (s *Screener) Run() types.CandidateGroups {
	bar := progress.New(s.showProgress, -1)
	st := &stats{startTime: time.Now()}

	// Group files by size
	bySize := make(map[int64][]*types.FileInfo)
	for _, f := range s.files {
		bySize[f.Size] = append(bySize[f.Size], f)
	}

	// Select grouping strategy based on trustDeviceBoundaries
	groupFunc := groupByIno
	if s.trustDeviceBoundaries {
		groupFunc = groupByDevIno
	}

	// For each size group, create sibling groups and filter
	var result []types.CandidateGroup
	for _, files := range bySize {
		siblings := groupFunc(files)
		if siblings.Len() >= 2 { // 2+ unique inodes = potential duplicates
			result = append(result, siblings)
		}
	}

	// Accumulate stats (count unique inodes, not paths)
	for _, group := range result {
		st.candidateFiles += group.Len()
		st.candidateBytes += group.First().First().Size * int64(group.Len())
	}

	bar.Finish(st)

	return types.NewCandidateGroups(result)
}

// groupByIno groups files by their inode number only.
// This is the default and safe behavior for NFS where the same file can appear
// with different device IDs across different mount points.
func groupByIno(files []*types.FileInfo) types.CandidateGroup {
	byIno := make(map[uint64][]*types.FileInfo)
	for _, f := range files {
		byIno[f.Ino] = append(byIno[f.Ino], f)
	}

	siblings := make([]types.SiblingGroup, 0, len(byIno))
	for _, files := range byIno {
		siblings = append(siblings, types.NewSiblingGroup(files))
	}

	return types.NewCandidateGroup(siblings)
}

// groupByDevIno groups files by their device and inode numbers.
// Files with the same dev+ino are hardlinks and form a sibling group.
// Use this only with --trust-device-boundaries when you know devices have
// independent inode namespaces.
func groupByDevIno(files []*types.FileInfo) types.CandidateGroup {
	// Collect raw slices first (map iteration is non-deterministic)
	byDevIno := make(map[devIno][]*types.FileInfo)
	for _, f := range files {
		key := devIno{f.Dev, f.Ino}
		byDevIno[key] = append(byDevIno[key], f)
	}

	// Convert to sorted SiblingGroups
	siblings := make([]types.SiblingGroup, 0, len(byDevIno))
	for _, files := range byDevIno {
		siblings = append(siblings, types.NewSiblingGroup(files))
	}

	// Return sorted CandidateGroup (sorting enforced at construction)
	return types.NewCandidateGroup(siblings)
}
