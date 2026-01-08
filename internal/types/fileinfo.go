// Package types provides shared types used across the dupedog codebase.
package types

import (
	"cmp"
	"slices"
	"time"
)

// FileInfo holds metadata for a scanned file.
type FileInfo struct {
	Path    string
	Size    int64
	ModTime time.Time
	Dev     uint64
	Ino     uint64
	Nlink   uint32
}

// Sorted is an ordered collection that maintains sort order by a key function.
// T is the element type, K is the comparable key type.
// Once constructed, items are guaranteed to be sorted by key.
type Sorted[T any, K cmp.Ordered] struct {
	items   []T
	keyFunc func(T) K
}

// NewSorted creates a sorted collection from items using keyFunc for ordering.
// Items are copied and sorted at construction time.
func NewSorted[T any, K cmp.Ordered](items []T, keyFunc func(T) K) Sorted[T, K] {
	sorted := make([]T, len(items))
	copy(sorted, items)
	slices.SortFunc(sorted, func(a, b T) int {
		return cmp.Compare(keyFunc(a), keyFunc(b))
	})
	return Sorted[T, K]{items: sorted, keyFunc: keyFunc}
}

// Items returns the sorted items.
func (s Sorted[T, K]) Items() []T { return s.items }

// First returns the first item (smallest key), or zero value if empty.
func (s Sorted[T, K]) First() T {
	if len(s.items) == 0 {
		var zero T
		return zero
	}
	return s.items[0]
}

// Len returns the number of items.
func (s Sorted[T, K]) Len() int { return len(s.items) }

// SiblingGroup contains files sharing the same inode (hardlinks).
// Files are always sorted by Path for deterministic iteration.
type SiblingGroup = Sorted[*FileInfo, string]

// NewSiblingGroup creates a SiblingGroup sorted by file path.
func NewSiblingGroup(files []*FileInfo) SiblingGroup {
	return NewSorted(files, func(f *FileInfo) string { return f.Path })
}

// CandidateGroup contains sibling groups with same size (potential duplicates).
// Sorted by first file's path in each sibling group.
type CandidateGroup = Sorted[SiblingGroup, string]

// NewCandidateGroup creates a CandidateGroup sorted by first file's path.
func NewCandidateGroup(siblings []SiblingGroup) CandidateGroup {
	return NewSorted(siblings, func(sg SiblingGroup) string { return sg.First().Path })
}

// CandidateGroups is a sorted collection of candidate groups.
type CandidateGroups = Sorted[CandidateGroup, string]

// NewCandidateGroups creates sorted CandidateGroups.
func NewCandidateGroups(groups []CandidateGroup) CandidateGroups {
	return NewSorted(groups, func(cg CandidateGroup) string {
		return cg.First().First().Path
	})
}

// DuplicateGroup contains sibling groups with identical content.
// Sorted by first file's path in each sibling group.
type DuplicateGroup = Sorted[SiblingGroup, string]

// NewDuplicateGroup creates a DuplicateGroup sorted by first file's path.
func NewDuplicateGroup(siblings []SiblingGroup) DuplicateGroup {
	return NewSorted(siblings, func(sg SiblingGroup) string { return sg.First().Path })
}

// DuplicateGroups is a sorted collection of duplicate groups.
type DuplicateGroups = Sorted[DuplicateGroup, string]

// NewDuplicateGroups creates sorted DuplicateGroups.
func NewDuplicateGroups(groups []DuplicateGroup) DuplicateGroups {
	return NewSorted(groups, func(dg DuplicateGroup) string {
		return dg.First().First().Path
	})
}

// Semaphore implements a counting semaphore using a buffered channel.
// It limits concurrent access to a resource by blocking when the limit is reached.
type Semaphore chan struct{}

// NewSemaphore creates a semaphore that allows up to n concurrent acquisitions.
func NewSemaphore(n int) Semaphore { return make(chan struct{}, n) }

// Acquire blocks until a slot is available, then claims it.
func (s Semaphore) Acquire() { s <- struct{}{} }

// Release frees a slot, unblocking one waiting Acquire call.
func (s Semaphore) Release() { <-s }
