//go:build unix

package screener

import (
	"testing"

	"github.com/ivoronin/dupedog/internal/types"
)

// =============================================================================
// Section 4.1: Core Screener Tests
// =============================================================================

// TestGroupByDevInoBasic tests basic hardlink grouping.
func TestGroupByDevInoBasic(t *testing.T) {
	// Two files with different inodes
	files := []*types.FileInfo{
		{Path: "/a.txt", Size: 100, Dev: 1, Ino: 1},
		{Path: "/b.txt", Size: 100, Dev: 1, Ino: 2},
	}

	groups := groupByDevIno(files)

	if groups.Len() != 2 {
		t.Errorf("expected 2 sibling groups, got %d", groups.Len())
	}
}

// TestGroupByDevInoHardlinks tests that hardlinks are grouped together.
func TestGroupByDevInoHardlinks(t *testing.T) {
	// Three paths, two are hardlinks (same inode)
	files := []*types.FileInfo{
		{Path: "/a.txt", Size: 100, Dev: 1, Ino: 1},
		{Path: "/b.txt", Size: 100, Dev: 1, Ino: 1}, // hardlink to a.txt
		{Path: "/c.txt", Size: 100, Dev: 1, Ino: 2},
	}

	groups := groupByDevIno(files)

	if groups.Len() != 2 {
		t.Errorf("expected 2 sibling groups, got %d", groups.Len())
	}

	// Find the group with 2 files (hardlinks)
	var foundDoubleGroup bool
	for _, g := range groups.Items() {
		if g.Len() == 2 {
			foundDoubleGroup = true
			// Verify both are inode 1
			for _, f := range g.Items() {
				if f.Ino != 1 {
					t.Errorf("expected inode 1, got %d", f.Ino)
				}
			}
		}
	}
	if !foundDoubleGroup {
		t.Error("expected to find sibling group with 2 hardlinks")
	}
}

// TestScreenerSizeGrouping tests that files are grouped by size.
func TestScreenerSizeGrouping(t *testing.T) {
	files := []*types.FileInfo{
		{Path: "/a.txt", Size: 100, Dev: 1, Ino: 1},
		{Path: "/b.txt", Size: 100, Dev: 1, Ino: 2},
		{Path: "/c.txt", Size: 200, Dev: 1, Ino: 3}, // Different size
	}

	s := New(files, false, false)
	candidates := s.Run()

	// Only size=100 group has 2+ inodes
	if candidates.Len() != 1 {
		t.Errorf("expected 1 candidate group, got %d", candidates.Len())
	}
}

// TestScreenerSingleInodeFiltered tests that single-inode groups are filtered out.
func TestScreenerSingleInodeFiltered(t *testing.T) {
	// Two hardlinks (same inode) - should be filtered
	files := []*types.FileInfo{
		{Path: "/a.txt", Size: 100, Dev: 1, Ino: 1},
		{Path: "/b.txt", Size: 100, Dev: 1, Ino: 1}, // same inode
	}

	s := New(files, false, false)
	candidates := s.Run()

	// Single inode = no potential duplicates
	if candidates.Len() != 0 {
		t.Errorf("expected 0 candidate groups (single inode), got %d", candidates.Len())
	}
}

// =============================================================================
// Section 4.2: Screener Edge Cases
// =============================================================================

// TestScreenerEmptyInput tests behavior with empty input.
func TestScreenerEmptyInput(t *testing.T) {
	s := New([]*types.FileInfo{}, false, false)
	candidates := s.Run()

	if candidates.Len() != 0 {
		t.Errorf("expected 0 candidates for empty input, got %d", candidates.Len())
	}
}

// TestScreenerAllUniqueSizes tests that all unique sizes yield no candidates.
func TestScreenerAllUniqueSizes(t *testing.T) {
	files := []*types.FileInfo{
		{Path: "/a.txt", Size: 100, Dev: 1, Ino: 1},
		{Path: "/b.txt", Size: 200, Dev: 1, Ino: 2},
		{Path: "/c.txt", Size: 300, Dev: 1, Ino: 3},
	}

	s := New(files, false, false)
	candidates := s.Run()

	// All unique sizes = no duplicates possible
	if candidates.Len() != 0 {
		t.Errorf("expected 0 candidates (unique sizes), got %d", candidates.Len())
	}
}

// TestScreenerAllSameInode tests behavior when all paths point to same inode.
func TestScreenerAllSameInode(t *testing.T) {
	// All paths are hardlinks to the same file
	files := []*types.FileInfo{
		{Path: "/a.txt", Size: 100, Dev: 1, Ino: 1},
		{Path: "/b.txt", Size: 100, Dev: 1, Ino: 1},
		{Path: "/c.txt", Size: 100, Dev: 1, Ino: 1},
	}

	s := New(files, false, false)
	candidates := s.Run()

	// Single inode = already deduplicated
	if candidates.Len() != 0 {
		t.Errorf("expected 0 candidates (all same inode), got %d", candidates.Len())
	}
}

// TestScreenerCrossDeviceSameInode tests files from different devices with same inode number.
// This is the NFS-mounted-twice scenario: same file accessible via different mount points.
func TestScreenerCrossDeviceSameInode(t *testing.T) {
	// Same inode number but different devices = same underlying file (e.g., NFS)
	files := []*types.FileInfo{
		{Path: "/mnt/nfs1/file.txt", Size: 100, Dev: 1, Ino: 5000},
		{Path: "/mnt/nfs2/file.txt", Size: 100, Dev: 2, Ino: 5000}, // same ino!
	}

	tests := []struct {
		name                  string
		trustDeviceBoundaries bool
		wantCandidates        int
	}{
		{
			name:                  "default groups by ino only",
			trustDeviceBoundaries: false,
			wantCandidates:        0, // same ino = same file = no duplicates
		},
		{
			name:                  "trust-device-boundaries groups by dev+ino",
			trustDeviceBoundaries: true,
			wantCandidates:        1, // different dev = different files = 1 candidate group
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New(files, false, tt.trustDeviceBoundaries)
			candidates := s.Run()

			if candidates.Len() != tt.wantCandidates {
				t.Errorf("got %d candidate groups, want %d", candidates.Len(), tt.wantCandidates)
			}
		})
	}
}

// TestScreenerMixedHardlinksAndDuplicates tests complex scenario.
func TestScreenerMixedHardlinksAndDuplicates(t *testing.T) {
	// Files a,b are hardlinks (ino=1), c,d are hardlinks (ino=2), e is unique (ino=3)
	files := []*types.FileInfo{
		{Path: "/a.txt", Size: 100, Dev: 1, Ino: 1},
		{Path: "/b.txt", Size: 100, Dev: 1, Ino: 1},
		{Path: "/c.txt", Size: 100, Dev: 1, Ino: 2},
		{Path: "/d.txt", Size: 100, Dev: 1, Ino: 2},
		{Path: "/e.txt", Size: 100, Dev: 1, Ino: 3},
	}

	s := New(files, false, false)
	candidates := s.Run()

	// 3 unique inodes, all size 100 = 1 candidate group
	if candidates.Len() != 1 {
		t.Fatalf("expected 1 candidate group, got %d", candidates.Len())
	}

	// The group should have 3 sibling groups
	if candidates.First().Len() != 3 {
		t.Errorf("expected 3 sibling groups, got %d", candidates.First().Len())
	}
}

// TestScreenerPreservesAllPaths tests that all hardlink paths are preserved.
func TestScreenerPreservesAllPaths(t *testing.T) {
	files := []*types.FileInfo{
		{Path: "/a.txt", Size: 100, Dev: 1, Ino: 1},
		{Path: "/b.txt", Size: 100, Dev: 1, Ino: 1},
		{Path: "/c.txt", Size: 100, Dev: 1, Ino: 1},
		{Path: "/d.txt", Size: 100, Dev: 1, Ino: 2},
	}

	s := New(files, false, false)
	candidates := s.Run()

	if candidates.Len() != 1 {
		t.Fatalf("expected 1 candidate group, got %d", candidates.Len())
	}

	// Find the sibling group with 3 paths
	var pathCount int
	for _, siblings := range candidates.First().Items() {
		pathCount += siblings.Len()
	}

	// All 4 paths should be preserved
	if pathCount != 4 {
		t.Errorf("expected 4 paths preserved, got %d", pathCount)
	}
}

// TestScreenerLargeSizeGroup tests handling of many files with same size.
func TestScreenerLargeSizeGroup(t *testing.T) {
	// Create 100 files with same size, all unique inodes
	var files []*types.FileInfo
	for i := 0; i < 100; i++ {
		files = append(files, &types.FileInfo{
			Path: "/file" + string(rune(i)),
			Size: 100,
			Dev:  1,
			Ino:  uint64(i + 1),
		})
	}

	s := New(files, false, false)
	candidates := s.Run()

	if candidates.Len() != 1 {
		t.Fatalf("expected 1 candidate group, got %d", candidates.Len())
	}

	if candidates.First().Len() != 100 {
		t.Errorf("expected 100 sibling groups, got %d", candidates.First().Len())
	}
}
