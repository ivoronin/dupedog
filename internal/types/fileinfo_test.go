package types

import (
	"testing"
	"time"
)

// =============================================================================
// Section 1: Generic Sorted[T, K] Tests
// =============================================================================

// TestSortedBasic tests basic sorting with string keys.
func TestSortedBasic(t *testing.T) {
	items := []string{"charlie", "alpha", "bravo"}
	sorted := NewSorted(items, func(s string) string { return s })

	if sorted.Len() != 3 {
		t.Errorf("expected Len() = 3, got %d", sorted.Len())
	}

	expected := []string{"alpha", "bravo", "charlie"}
	for i, item := range sorted.Items() {
		if item != expected[i] {
			t.Errorf("Items()[%d] = %q, want %q", i, item, expected[i])
		}
	}
}

// TestSortedFirst tests First() returns smallest key element.
func TestSortedFirst(t *testing.T) {
	items := []int{30, 10, 20}
	sorted := NewSorted(items, func(i int) int { return i })

	if sorted.First() != 10 {
		t.Errorf("First() = %d, want 10", sorted.First())
	}
}

// TestSortedFirstEmpty tests First() returns zero value on empty.
func TestSortedFirstEmpty(t *testing.T) {
	sorted := NewSorted([]string{}, func(s string) string { return s })

	if sorted.First() != "" {
		t.Errorf("First() on empty = %q, want empty string", sorted.First())
	}
}

// TestSortedLenEmpty tests Len() on empty collection.
func TestSortedLenEmpty(t *testing.T) {
	sorted := NewSorted([]int{}, func(i int) int { return i })

	if sorted.Len() != 0 {
		t.Errorf("Len() on empty = %d, want 0", sorted.Len())
	}
}

// TestSortedDoesNotMutateInput tests that input slice is not modified.
func TestSortedDoesNotMutateInput(t *testing.T) {
	original := []string{"charlie", "alpha", "bravo"}
	originalCopy := make([]string, len(original))
	copy(originalCopy, original)

	_ = NewSorted(original, func(s string) string { return s })

	for i := range original {
		if original[i] != originalCopy[i] {
			t.Errorf("Input was mutated: original[%d] = %q, was %q", i, original[i], originalCopy[i])
		}
	}
}

// TestSortedIntKeys tests sorting by integer key.
func TestSortedIntKeys(t *testing.T) {
	type item struct {
		name  string
		value int
	}
	items := []item{
		{name: "c", value: 30},
		{name: "a", value: 10},
		{name: "b", value: 20},
	}

	sorted := NewSorted(items, func(i item) int { return i.value })

	expected := []string{"a", "b", "c"}
	for i, item := range sorted.Items() {
		if item.name != expected[i] {
			t.Errorf("Items()[%d].name = %q, want %q", i, item.name, expected[i])
		}
	}
}

// TestSortedDeterminism tests that same input always produces same output.
func TestSortedDeterminism(t *testing.T) {
	items := []string{"delta", "alpha", "charlie", "bravo"}

	// Run multiple times, verify same result
	var firstResult []string
	for i := 0; i < 10; i++ {
		sorted := NewSorted(items, func(s string) string { return s })
		if firstResult == nil {
			firstResult = sorted.Items()
		} else {
			for j, item := range sorted.Items() {
				if item != firstResult[j] {
					t.Errorf("Run %d: Items()[%d] = %q, want %q (non-deterministic)", i, j, item, firstResult[j])
				}
			}
		}
	}
}

// TestSortedSingleItem tests behavior with single item.
func TestSortedSingleItem(t *testing.T) {
	sorted := NewSorted([]string{"only"}, func(s string) string { return s })

	if sorted.Len() != 1 {
		t.Errorf("Len() = %d, want 1", sorted.Len())
	}
	if sorted.First() != "only" {
		t.Errorf("First() = %q, want %q", sorted.First(), "only")
	}
}

// =============================================================================
// Section 2: SiblingGroup Tests
// =============================================================================

// TestNewSiblingGroup tests SiblingGroup sorts by Path.
func TestNewSiblingGroup(t *testing.T) {
	files := []*FileInfo{
		{Path: "/z/file.txt", Size: 100},
		{Path: "/a/file.txt", Size: 100},
		{Path: "/m/file.txt", Size: 100},
	}

	sg := NewSiblingGroup(files)

	if sg.Len() != 3 {
		t.Errorf("Len() = %d, want 3", sg.Len())
	}
	if sg.First().Path != "/a/file.txt" {
		t.Errorf("First().Path = %q, want %q", sg.First().Path, "/a/file.txt")
	}

	expected := []string{"/a/file.txt", "/m/file.txt", "/z/file.txt"}
	for i, f := range sg.Items() {
		if f.Path != expected[i] {
			t.Errorf("Items()[%d].Path = %q, want %q", i, f.Path, expected[i])
		}
	}
}

// TestNewSiblingGroupEmpty tests empty SiblingGroup.
func TestNewSiblingGroupEmpty(t *testing.T) {
	sg := NewSiblingGroup([]*FileInfo{})

	if sg.Len() != 0 {
		t.Errorf("Len() = %d, want 0", sg.Len())
	}
	if sg.First() != nil {
		t.Errorf("First() = %v, want nil", sg.First())
	}
}

// =============================================================================
// Section 3: CandidateGroup Tests
// =============================================================================

// TestNewCandidateGroup tests CandidateGroup sorts by first file's path.
func TestNewCandidateGroup(t *testing.T) {
	sg1 := NewSiblingGroup([]*FileInfo{{Path: "/z/file.txt"}})
	sg2 := NewSiblingGroup([]*FileInfo{{Path: "/a/file.txt"}})
	sg3 := NewSiblingGroup([]*FileInfo{{Path: "/m/file.txt"}})

	cg := NewCandidateGroup([]SiblingGroup{sg1, sg2, sg3})

	if cg.Len() != 3 {
		t.Errorf("Len() = %d, want 3", cg.Len())
	}
	if cg.First().First().Path != "/a/file.txt" {
		t.Errorf("First().First().Path = %q, want %q", cg.First().First().Path, "/a/file.txt")
	}
}

// =============================================================================
// Section 4: DuplicateGroup Tests
// =============================================================================

// TestNewDuplicateGroup tests DuplicateGroup sorts by first file's path.
func TestNewDuplicateGroup(t *testing.T) {
	sg1 := NewSiblingGroup([]*FileInfo{{Path: "/z/file.txt", Size: 100}})
	sg2 := NewSiblingGroup([]*FileInfo{{Path: "/a/file.txt", Size: 100}})

	dg := NewDuplicateGroup([]SiblingGroup{sg1, sg2})

	if dg.Len() != 2 {
		t.Errorf("Len() = %d, want 2", dg.Len())
	}
	if dg.First().First().Path != "/a/file.txt" {
		t.Errorf("First().First().Path = %q, want %q", dg.First().First().Path, "/a/file.txt")
	}
}

// =============================================================================
// Section 5: CandidateGroups Tests
// =============================================================================

// TestNewCandidateGroups tests CandidateGroups sorts by first path.
func TestNewCandidateGroups(t *testing.T) {
	cg1 := NewCandidateGroup([]SiblingGroup{
		NewSiblingGroup([]*FileInfo{{Path: "/z/file.txt"}}),
	})
	cg2 := NewCandidateGroup([]SiblingGroup{
		NewSiblingGroup([]*FileInfo{{Path: "/a/file.txt"}}),
	})

	cgs := NewCandidateGroups([]CandidateGroup{cg1, cg2})

	if cgs.Len() != 2 {
		t.Errorf("Len() = %d, want 2", cgs.Len())
	}
	if cgs.First().First().First().Path != "/a/file.txt" {
		t.Errorf("First path = %q, want %q", cgs.First().First().First().Path, "/a/file.txt")
	}
}

// =============================================================================
// Section 6: DuplicateGroups Tests
// =============================================================================

// TestNewDuplicateGroups tests DuplicateGroups sorts by first path.
func TestNewDuplicateGroups(t *testing.T) {
	dg1 := NewDuplicateGroup([]SiblingGroup{
		NewSiblingGroup([]*FileInfo{{Path: "/z/file.txt", Size: 100}}),
	})
	dg2 := NewDuplicateGroup([]SiblingGroup{
		NewSiblingGroup([]*FileInfo{{Path: "/a/file.txt", Size: 100}}),
	})

	dgs := NewDuplicateGroups([]DuplicateGroup{dg1, dg2})

	if dgs.Len() != 2 {
		t.Errorf("Len() = %d, want 2", dgs.Len())
	}
	if dgs.First().First().First().Path != "/a/file.txt" {
		t.Errorf("First path = %q, want %q", dgs.First().First().First().Path, "/a/file.txt")
	}
}

// =============================================================================
// Section 7: FileInfo Tests
// =============================================================================

// TestFileInfoFields tests that FileInfo can store all expected metadata.
func TestFileInfoFields(t *testing.T) {
	now := time.Now()
	fi := &FileInfo{
		Path:    "/test/file.txt",
		Size:    1024,
		ModTime: now,
		Dev:     1,
		Ino:     12345,
		Nlink:   2,
	}

	if fi.Path != "/test/file.txt" {
		t.Errorf("Path = %q, want %q", fi.Path, "/test/file.txt")
	}
	if fi.Size != 1024 {
		t.Errorf("Size = %d, want 1024", fi.Size)
	}
	if !fi.ModTime.Equal(now) {
		t.Errorf("ModTime = %v, want %v", fi.ModTime, now)
	}
	if fi.Dev != 1 {
		t.Errorf("Dev = %d, want 1", fi.Dev)
	}
	if fi.Ino != 12345 {
		t.Errorf("Ino = %d, want 12345", fi.Ino)
	}
	if fi.Nlink != 2 {
		t.Errorf("Nlink = %d, want 2", fi.Nlink)
	}
}

// =============================================================================
// Section 8: Semaphore Tests
// =============================================================================

// TestSemaphoreBasic tests basic semaphore acquire/release.
func TestSemaphoreBasic(t *testing.T) {
	sem := NewSemaphore(2)

	// Should be able to acquire twice without blocking
	sem.Acquire()
	sem.Acquire()

	// Release one
	sem.Release()

	// Should be able to acquire again
	sem.Acquire()

	// Clean up
	sem.Release()
	sem.Release()
}
