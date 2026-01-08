//go:build unix && !e2e

package internal

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/ivoronin/dupedog/internal/cache"
	"github.com/ivoronin/dupedog/internal/deduper"
	"github.com/ivoronin/dupedog/internal/scanner"
	"github.com/ivoronin/dupedog/internal/screener"
	"github.com/ivoronin/dupedog/internal/testfs"
	"github.com/ivoronin/dupedog/internal/verifier"
)

// noCache is a disabled cache for tests (cache.Open("") returns no-op cache).
var noCache, _ = cache.Open("")

// =============================================================================
// Section 8.1: Full Pipeline Integration Tests
// =============================================================================

// TestFullPipelineBasicDuplicates tests basic duplicate detection and hardlinking.
func TestFullPipelineBasicDuplicates(t *testing.T) {
	spec := testfs.FileTree{
		Volumes: []testfs.Volume{
			{
				MountPoint: "/data",
				Files: []testfs.File{
					{Path: []string{"a.txt"}, Chunks: []testfs.Chunk{{Pattern: 'D', Size: "1KiB"}}},
					{Path: []string{"b.txt"}, Chunks: []testfs.Chunk{{Pattern: 'D', Size: "1KiB"}}},
				},
			},
		},
	}

	h := testfs.New(t, spec)

	// Run pipeline
	runPipeline(t, h.Root(), nil, 0, false)

	// Assert files are now hardlinked
	expectedSpec := testfs.FileTree{
		Volumes: []testfs.Volume{
			{
				MountPoint: "/data",
				Files: []testfs.File{
					{Path: []string{"a.txt", "b.txt"}},
				},
			},
		},
	}
	h.Assert(expectedSpec)
}

// TestFullPipelineExistingHardlinks tests that existing hardlinks are preserved.
func TestFullPipelineExistingHardlinks(t *testing.T) {
	spec := testfs.FileTree{
		Volumes: []testfs.Volume{
			{
				MountPoint: "/data",
				Files: []testfs.File{
					// a.txt and a_link.txt are already hardlinked
					{Path: []string{"a.txt", "a_link.txt"}, Chunks: []testfs.Chunk{{Pattern: 'O', Size: "1KiB"}}},
					// b.txt is a duplicate (different inode)
					{Path: []string{"b.txt"}, Chunks: []testfs.Chunk{{Pattern: 'O', Size: "1KiB"}}},
				},
			},
		},
	}

	h := testfs.New(t, spec)

	// Run pipeline
	runPipeline(t, h.Root(), nil, 0, false)

	// All three should now be hardlinked
	expectedSpec := testfs.FileTree{
		Volumes: []testfs.Volume{
			{
				MountPoint: "/data",
				Files: []testfs.File{
					{Path: []string{"a.txt", "a_link.txt", "b.txt"}},
				},
			},
		},
	}
	h.Assert(expectedSpec)
}

// TestFullPipelineMixedDuplicatesAndUnique tests mixed duplicates and unique files.
func TestFullPipelineMixedDuplicatesAndUnique(t *testing.T) {
	spec := testfs.FileTree{
		Volumes: []testfs.Volume{
			{
				MountPoint: "/data",
				Files: []testfs.File{
					// Duplicate group 1
					{Path: []string{"dup1_a.txt"}, Chunks: []testfs.Chunk{{Pattern: '1', Size: "1KiB"}}},
					{Path: []string{"dup1_b.txt"}, Chunks: []testfs.Chunk{{Pattern: '1', Size: "1KiB"}}},
					// Duplicate group 2
					{Path: []string{"dup2_a.txt"}, Chunks: []testfs.Chunk{{Pattern: '2', Size: "2KiB"}}},
					{Path: []string{"dup2_b.txt"}, Chunks: []testfs.Chunk{{Pattern: '2', Size: "2KiB"}}},
					// Unique file (different size)
					{Path: []string{"unique.txt"}, Chunks: []testfs.Chunk{{Pattern: 'U', Size: "3KiB"}}},
				},
			},
		},
	}

	h := testfs.New(t, spec)

	// Run pipeline
	runPipeline(t, h.Root(), nil, 0, false)

	// Verify duplicate groups are hardlinked, unique is unchanged
	expectedSpec := testfs.FileTree{
		Volumes: []testfs.Volume{
			{
				MountPoint: "/data",
				Files: []testfs.File{
					{Path: []string{"dup1_a.txt", "dup1_b.txt"}},
					{Path: []string{"dup2_a.txt", "dup2_b.txt"}},
					{Path: []string{"unique.txt"}},
				},
			},
		},
	}
	h.Assert(expectedSpec)
}

// TestFullPipelineMinSizeFilter tests --min-size filtering.
func TestFullPipelineMinSizeFilter(t *testing.T) {
	spec := testfs.FileTree{
		Volumes: []testfs.Volume{
			{
				MountPoint: "/data",
				Files: []testfs.File{
					// Small duplicates (should be filtered)
					{Path: []string{"small_a.txt"}, Chunks: []testfs.Chunk{{Pattern: 'S', Size: "100"}}},
					{Path: []string{"small_b.txt"}, Chunks: []testfs.Chunk{{Pattern: 'S', Size: "100"}}},
					// Large duplicates (should be processed)
					{Path: []string{"large_a.txt"}, Chunks: []testfs.Chunk{{Pattern: 'L', Size: "1KiB"}}},
					{Path: []string{"large_b.txt"}, Chunks: []testfs.Chunk{{Pattern: 'L', Size: "1KiB"}}},
				},
			},
		},
	}

	h := testfs.New(t, spec)

	// Run pipeline with minSize=500
	runPipeline(t, h.Root(), nil, 500, false)

	// Small files should NOT be hardlinked, large files should be
	smallA := filepath.Join(h.Root(), "data", "small_a.txt")
	smallB := filepath.Join(h.Root(), "data", "small_b.txt")
	largeA := filepath.Join(h.Root(), "data", "large_a.txt")
	largeB := filepath.Join(h.Root(), "data", "large_b.txt")

	if sameInode(t, smallA, smallB) {
		t.Error("small files should NOT be hardlinked (filtered by min-size)")
	}
	if !sameInode(t, largeA, largeB) {
		t.Error("large files should be hardlinked")
	}
}

// TestFullPipelineExcludePatterns tests --exclude patterns.
func TestFullPipelineExcludePatterns(t *testing.T) {
	spec := testfs.FileTree{
		Volumes: []testfs.Volume{
			{
				MountPoint: "/data",
				Files: []testfs.File{
					{Path: []string{"keep_a.txt"}, Chunks: []testfs.Chunk{{Pattern: 'K', Size: "1KiB"}}},
					{Path: []string{"keep_b.txt"}, Chunks: []testfs.Chunk{{Pattern: 'K', Size: "1KiB"}}},
					{Path: []string{"exclude_a.bak"}, Chunks: []testfs.Chunk{{Pattern: 'K', Size: "1KiB"}}},
					{Path: []string{"exclude_b.bak"}, Chunks: []testfs.Chunk{{Pattern: 'K', Size: "1KiB"}}},
				},
			},
		},
	}

	h := testfs.New(t, spec)

	// Run pipeline excluding *.bak
	s := scanner.New([]string{filepath.Join(h.Root(), "data")}, 0, []string{"*.bak"}, 2, false, nil)
	files := s.Run()

	// Should only find .txt files
	if len(files) != 2 {
		t.Errorf("expected 2 files (excluding .bak), got %d", len(files))
	}
}

// =============================================================================
// Section 8.2: Empty/No-Results Scenarios (table-driven)
// =============================================================================

func TestFullPipelineEmptyScenarios(t *testing.T) {
	tests := []struct {
		name string
		spec testfs.FileTree
	}{
		{
			name: "empty directory",
			spec: testfs.FileTree{
				Volumes: []testfs.Volume{
					{MountPoint: "/data", Files: []testfs.File{}},
				},
			},
		},
		{
			name: "single file",
			spec: testfs.FileTree{
				Volumes: []testfs.Volume{
					{
						MountPoint: "/data",
						Files: []testfs.File{
							{Path: []string{"only.txt"}, Chunks: []testfs.Chunk{{Pattern: 'O', Size: "1KiB"}}},
						},
					},
				},
			},
		},
		{
			name: "all unique sizes",
			spec: testfs.FileTree{
				Volumes: []testfs.Volume{
					{
						MountPoint: "/data",
						Files: []testfs.File{
							{Path: []string{"a.txt"}, Chunks: []testfs.Chunk{{Pattern: 'A', Size: "1KiB"}}},
							{Path: []string{"b.txt"}, Chunks: []testfs.Chunk{{Pattern: 'B', Size: "2KiB"}}},
							{Path: []string{"c.txt"}, Chunks: []testfs.Chunk{{Pattern: 'C', Size: "3KiB"}}},
						},
					},
				},
			},
		},
		{
			name: "same size different content",
			spec: testfs.FileTree{
				Volumes: []testfs.Volume{
					{
						MountPoint: "/data",
						Files: []testfs.File{
							{Path: []string{"a.txt"}, Chunks: []testfs.Chunk{{Pattern: 'A', Size: "1KiB"}}},
							{Path: []string{"b.txt"}, Chunks: []testfs.Chunk{{Pattern: 'B', Size: "1KiB"}}},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := testfs.New(t, tt.spec)

			// Run pipeline - should complete without errors
			s := scanner.New([]string{filepath.Join(h.Root(), "data")}, 0, nil, 2, false, nil)
			files := s.Run()

			sc := screener.New(files, false, false)
			candidates := sc.Run()

			v := verifier.New(candidates, 2, false, nil, noCache)
			duplicates := v.Run()

			// No duplicates expected in these scenarios
			if tt.name == "same size different content" && duplicates.Len() > 0 {
				t.Errorf("expected no duplicates (different content), got %d groups", duplicates.Len())
			}
		})
	}
}

// =============================================================================
// Section 8.4: Data Integrity Tests
// =============================================================================

// TestDataIntegrityHardlinksShareData tests that hardlinks actually share data.
func TestDataIntegrityHardlinksShareData(t *testing.T) {
	spec := testfs.FileTree{
		Volumes: []testfs.Volume{
			{
				MountPoint: "/data",
				Files: []testfs.File{
					{Path: []string{"a.txt"}, Chunks: []testfs.Chunk{{Pattern: 'C', Size: "100"}}},
					{Path: []string{"b.txt"}, Chunks: []testfs.Chunk{{Pattern: 'C', Size: "100"}}},
				},
			},
		},
	}

	h := testfs.New(t, spec)
	runPipeline(t, h.Root(), nil, 0, false)

	pathA := filepath.Join(h.Root(), "data", "a.txt")
	pathB := filepath.Join(h.Root(), "data", "b.txt")

	// Read original content
	contentA, err := os.ReadFile(pathA)
	if err != nil {
		t.Fatal(err)
	}

	// Modify via one path
	if err := os.WriteFile(pathA, []byte("modified"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Read via other path - should see the change (hardlinks share data)
	contentB, err := os.ReadFile(pathB)
	if err != nil {
		t.Fatal(err)
	}

	if string(contentB) != "modified" {
		t.Errorf("hardlinks should share data: wrote 'modified' to a.txt, read %q from b.txt", contentB)
	}

	// Restore for cleanup
	if err := os.WriteFile(pathA, contentA, 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestDataIntegrityOriginalDataPreserved tests that original data is never lost.
func TestDataIntegrityOriginalDataPreserved(t *testing.T) {
	spec := testfs.FileTree{
		Volumes: []testfs.Volume{
			{
				MountPoint: "/data",
				Files: []testfs.File{
					{Path: []string{"original.txt"}, Chunks: []testfs.Chunk{{Pattern: 'U', Size: "100"}}},
					{Path: []string{"duplicate.txt"}, Chunks: []testfs.Chunk{{Pattern: 'U', Size: "100"}}},
				},
			},
		},
	}

	h := testfs.New(t, spec)

	// Read content before dedup
	pathOrig := filepath.Join(h.Root(), "data", "original.txt")
	contentBefore, err := os.ReadFile(pathOrig)
	if err != nil {
		t.Fatal(err)
	}

	runPipeline(t, h.Root(), nil, 0, false)

	// Read content after dedup - should be identical
	contentAfter, err := os.ReadFile(pathOrig)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(contentBefore, contentAfter) {
		t.Error("original data should be preserved after deduplication")
	}
}

// =============================================================================
// Section 8.5: Progressive Checksum Tests
// =============================================================================

// TestProgressiveChecksumSameHeadDifferentTail tests that files with
// same HEAD (first 1MiB) but different TAIL (last 1MiB) are correctly
// identified as non-duplicates.
//
// Verifier strategy: HEAD -> TAIL -> CHUNK[0] -> CHUNK[1]...
// This test verifies separation at the TAIL stage.
func TestProgressiveChecksumSameHeadDifferentTail(t *testing.T) {
	spec := testfs.FileTree{
		Volumes: []testfs.Volume{
			{
				MountPoint: "/data",
				Files: []testfs.File{
					// File 1: HEAD='A', TAIL='A' (2MiB total, uniform content)
					{Path: []string{"uniform.txt"}, Chunks: []testfs.Chunk{
						{Pattern: 'A', Size: "2MiB"},
					}},
					// File 2: HEAD='A', TAIL='B' (same head, different tail)
					{Path: []string{"mixed.txt"}, Chunks: []testfs.Chunk{
						{Pattern: 'A', Size: "1MiB"}, // HEAD matches uniform.txt
						{Pattern: 'B', Size: "1MiB"}, // TAIL differs
					}},
				},
			},
		},
	}

	h := testfs.New(t, spec)

	// Run pipeline
	runPipeline(t, h.Root(), nil, 0, false)

	// Files should NOT be hardlinked (different at TAIL boundary)
	uniformPath := filepath.Join(h.Root(), "data", "uniform.txt")
	mixedPath := filepath.Join(h.Root(), "data", "mixed.txt")

	if sameInode(t, uniformPath, mixedPath) {
		t.Error("files with same HEAD but different TAIL should NOT be hardlinked")
	}
}

// TestProgressiveChecksumMultiChunk tests files with multiple chunks
// demonstrating precise content control at verifier boundaries.
func TestProgressiveChecksumMultiChunk(t *testing.T) {
	spec := testfs.FileTree{
		Volumes: []testfs.Volume{
			{
				MountPoint: "/data",
				Files: []testfs.File{
					// File with multiple chunks - all 'X'
					{Path: []string{"all_x.txt"}, Chunks: []testfs.Chunk{
						{Pattern: 'X', Size: "1MiB"},
						{Pattern: 'X', Size: "1MiB"},
					}},
					// File with same total size but different pattern at second chunk
					{Path: []string{"x_then_y.txt"}, Chunks: []testfs.Chunk{
						{Pattern: 'X', Size: "1MiB"},
						{Pattern: 'Y', Size: "1MiB"},
					}},
					// Duplicate of first file (should be hardlinked)
					{Path: []string{"all_x_copy.txt"}, Chunks: []testfs.Chunk{
						{Pattern: 'X', Size: "1MiB"},
						{Pattern: 'X', Size: "1MiB"},
					}},
				},
			},
		},
	}

	h := testfs.New(t, spec)

	// Run pipeline
	runPipeline(t, h.Root(), nil, 0, false)

	// all_x.txt and all_x_copy.txt should be hardlinked
	allXPath := filepath.Join(h.Root(), "data", "all_x.txt")
	allXCopyPath := filepath.Join(h.Root(), "data", "all_x_copy.txt")
	xThenYPath := filepath.Join(h.Root(), "data", "x_then_y.txt")

	if !sameInode(t, allXPath, allXCopyPath) {
		t.Error("all_x.txt and all_x_copy.txt should be hardlinked (identical content)")
	}
	if sameInode(t, allXPath, xThenYPath) {
		t.Error("all_x.txt and x_then_y.txt should NOT be hardlinked (different TAIL)")
	}
}

// TestProgressiveChecksumLargeChunks tests progressive checksumming with GiB-sized chunks.
//
// Verifier strategy for large files:
//   - HEAD (first 1MiB)
//   - TAIL (last 1MiB)
//   - CHUNK[0] (0-1GiB)
//   - CHUNK[1] (1GiB-2GiB)
//   - CHUNK[2] (2GiB-3GiB)
//   - ...
//
// This test creates two files with:
//   - CHUNK[0]: same content (1GiB of 'A')
//   - CHUNK[1]: same content (1GiB of 'B')
//   - CHUNK[2]: DIFFERENT content ('X' vs 'Y')
//   - CHUNK[3]: same content (1GiB of 'D')
//
// Files should NOT be deduplicated because they differ at CHUNK[2].
func TestProgressiveChecksumLargeChunks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large file test in short mode")
	}

	spec := testfs.FileTree{
		Volumes: []testfs.Volume{
			{
				MountPoint: "/data",
				Files: []testfs.File{
					// File 1: 4GiB with pattern A-B-X-D
					{Path: []string{"file1.dat"}, Chunks: []testfs.Chunk{
						{Pattern: 'A', Size: "1GiB"}, // CHUNK[0] - matches
						{Pattern: 'B', Size: "1GiB"}, // CHUNK[1] - matches
						{Pattern: 'X', Size: "1GiB"}, // CHUNK[2] - DIFFERENT
						{Pattern: 'D', Size: "1GiB"}, // CHUNK[3] - matches
						{Pattern: 'E', Size: "512MiB"}, // CHUNK[4] - matches
					}},
					// File 2: 4GiB with pattern A-B-Y-D
					{Path: []string{"file2.dat"}, Chunks: []testfs.Chunk{
						{Pattern: 'A', Size: "1GiB"}, // CHUNK[0] - matches
						{Pattern: 'B', Size: "1GiB"}, // CHUNK[1] - matches
						{Pattern: 'Y', Size: "1GiB"}, // CHUNK[2] - DIFFERENT
						{Pattern: 'D', Size: "1GiB"}, // CHUNK[3] - matches
						{Pattern: 'E', Size: "512MiB"}, // CHUNK[4] - matches
					}},
				},
			},
		},
	}

	h := testfs.New(t, spec)

	// Run pipeline
	runPipeline(t, h.Root(), nil, 0, false)

	// Files should NOT be hardlinked (different at CHUNK[2])
	file1Path := filepath.Join(h.Root(), "data", "file1.dat")
	file2Path := filepath.Join(h.Root(), "data", "file2.dat")

	if sameInode(t, file1Path, file2Path) {
		t.Error("files with same CHUNK[0,1,3] but different CHUNK[2] should NOT be hardlinked")
	}
}

// =============================================================================
// Helper Functions
// =============================================================================

func runPipeline(t *testing.T, root string, exclude []string, minSize int64, dryRun bool) {
	t.Helper()

	dataDir := filepath.Join(root, "data")

	// Scanner
	s := scanner.New([]string{dataDir}, minSize, exclude, 2, false, nil)
	files := s.Run()

	// Screener
	sc := screener.New(files, false, false)
	candidates := sc.Run()

	// Verifier
	v := verifier.New(candidates, 2, false, nil, noCache)
	duplicates := v.Run()

	// Deduper
	d := deduper.New(duplicates, nil, dryRun, false, false, false, nil)
	d.Run()
}

func sameInode(t *testing.T, path1, path2 string) bool {
	t.Helper()

	info1, err := os.Stat(path1)
	if err != nil {
		t.Fatalf("failed to stat %s: %v", path1, err)
	}
	info2, err := os.Stat(path2)
	if err != nil {
		t.Fatalf("failed to stat %s: %v", path2, err)
	}

	stat1 := info1.Sys().(*syscall.Stat_t)
	stat2 := info2.Sys().(*syscall.Stat_t)

	return stat1.Dev == stat2.Dev && stat1.Ino == stat2.Ino
}
