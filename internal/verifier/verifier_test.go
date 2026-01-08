//go:build unix

package verifier

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/ivoronin/dupedog/internal/cache"
	"github.com/ivoronin/dupedog/internal/types"
)

// noCache is a disabled cache for tests (cache.Open("") returns no-op cache).
var noCache, _ = cache.Open("")

// =============================================================================
// Section 5.1: Core Verifier Tests
// =============================================================================

// TestInitialStage tests the initial stage selection based on file size.
func TestInitialStage(t *testing.T) {
	tests := []struct {
		name     string
		fileSize int64
		want     stage
	}{
		{"small file", probeSize - 1, stageChunk},
		{"exactly probeSize", probeSize, stageHead},
		{"large file", probeSize + 1, stageHead},
		{"zero bytes", 0, stageChunk},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := initialStage(tt.fileSize)
			if got != tt.want {
				t.Errorf("initialStage(%d) = %v, want %v", tt.fileSize, got, tt.want)
			}
		})
	}
}

// TestCalcRange tests byte range calculation for different stages.
func TestCalcRange(t *testing.T) {
	tests := []struct {
		name      string
		stage     stage
		offset    int64
		fileSize  int64
		wantStart int64
		wantSize  int64
	}{
		// HEAD stage
		{"head large file", stageHead, 0, 10 * probeSize, 0, probeSize},
		{"head small file", stageHead, 0, probeSize / 2, 0, probeSize / 2},
		{"head exact probeSize", stageHead, 0, probeSize, 0, probeSize},

		// TAIL stage
		{"tail large file", stageTail, 0, 10 * probeSize, 9 * probeSize, probeSize},
		{"tail small file", stageTail, 0, probeSize / 2, 0, probeSize / 2},
		{"tail exact probeSize", stageTail, 0, probeSize, 0, probeSize},
		{"tail 2MB file", stageTail, 0, 2 * probeSize, probeSize, probeSize},

		// CHUNK stage
		{"chunk first", stageChunk, 0, 2 * chunkSize, 0, chunkSize},
		{"chunk second", stageChunk, chunkSize, 2 * chunkSize, chunkSize, chunkSize},
		{"chunk partial", stageChunk, chunkSize, chunkSize + 100, chunkSize, 100},
		{"chunk small file", stageChunk, 0, 100, 0, 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, size := calcRange(tt.stage, tt.offset, tt.fileSize)
			if start != tt.wantStart || size != tt.wantSize {
				t.Errorf("calcRange(%v, %d, %d) = (%d, %d), want (%d, %d)",
					tt.stage, tt.offset, tt.fileSize, start, size, tt.wantStart, tt.wantSize)
			}
		})
	}
}

// TestNextJob tests stage progression logic.
func TestNextJob(t *testing.T) {
	smallFile := types.NewCandidateGroup([]types.SiblingGroup{
		types.NewSiblingGroup([]*types.FileInfo{{Path: "a", Size: 100}}),
	})
	largeFile := types.NewCandidateGroup([]types.SiblingGroup{
		types.NewSiblingGroup([]*types.FileInfo{{Path: "b", Size: 2 * chunkSize}}),
	})

	tests := []struct {
		name      string
		parent    job
		group     types.CandidateGroup
		wantStage stage
		wantDone  bool
	}{
		{"head to tail", job{stage: stageHead}, smallFile, stageTail, false},
		{"tail to chunk", job{stage: stageTail}, smallFile, stageChunk, false},
		{"chunk done (small)", job{stage: stageChunk, offset: 0}, smallFile, 0, true},
		{"chunk continues (large)", job{stage: stageChunk, offset: 0}, largeFile, stageChunk, false},
		{"chunk done (large)", job{stage: stageChunk, offset: 2 * chunkSize}, largeFile, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			next, done := nextJob(tt.parent, tt.group)
			if done != tt.wantDone {
				t.Errorf("nextJob() done = %v, want %v", done, tt.wantDone)
			}
			if !done && next.stage != tt.wantStage {
				t.Errorf("nextJob() stage = %v, want %v", next.stage, tt.wantStage)
			}
		})
	}
}

// TestHashRange tests file content hashing.
func TestHashRange(t *testing.T) {
	root := t.TempDir()

	// Create a test file with known content
	content := []byte("hello world")
	path := filepath.Join(root, "test.txt")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}

	hash, bytesRead, err := hashRange(path, 0, int64(len(content)))
	if err != nil {
		t.Fatalf("hashRange failed: %v", err)
	}

	if bytesRead != uint64(len(content)) {
		t.Errorf("bytesRead = %d, want %d", bytesRead, len(content))
	}

	// SHA256 of "hello world" is known
	expectedHash := "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
	if hash != expectedHash {
		t.Errorf("hash = %s, want %s", hash, expectedHash)
	}
}

// TestHashRangePartial tests partial file hashing.
func TestHashRangePartial(t *testing.T) {
	root := t.TempDir()

	content := []byte("hello world")
	path := filepath.Join(root, "test.txt")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}

	// Hash only "hello"
	hash, bytesRead, err := hashRange(path, 0, 5)
	if err != nil {
		t.Fatalf("hashRange failed: %v", err)
	}

	if bytesRead != 5 {
		t.Errorf("bytesRead = %d, want 5", bytesRead)
	}

	// SHA256 of "hello"
	expectedHash := "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	if hash != expectedHash {
		t.Errorf("hash = %s, want %s", hash, expectedHash)
	}
}

// =============================================================================
// Section 5.2: Verifier Boundary Conditions (CRITICAL)
// =============================================================================

// TestVerifierSmallFiles tests verification of files smaller than probeSize.
func TestVerifierSmallFiles(t *testing.T) {
	root := t.TempDir()

	// Create duplicate small files
	content := make([]byte, 100)
	for i := range content {
		content[i] = byte(i)
	}

	path1 := filepath.Join(root, "a.txt")
	path2 := filepath.Join(root, "b.txt")
	if err := os.WriteFile(path1, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path2, content, 0o644); err != nil {
		t.Fatal(err)
	}

	// Get file info
	info1 := getFileInfo(t, path1)
	info2 := getFileInfo(t, path2)

	groups := types.NewCandidateGroups([]types.CandidateGroup{
		types.NewCandidateGroup([]types.SiblingGroup{
			types.NewSiblingGroup([]*types.FileInfo{info1}),
			types.NewSiblingGroup([]*types.FileInfo{info2}),
		}),
	})

	v := New(groups, 2, false, nil, noCache)
	duplicates := v.Run()

	if duplicates.Len() != 1 {
		t.Fatalf("expected 1 duplicate group, got %d", duplicates.Len())
	}
}

// TestVerifierDifferentContent tests that different content yields no duplicates.
func TestVerifierDifferentContent(t *testing.T) {
	root := t.TempDir()

	path1 := filepath.Join(root, "a.txt")
	path2 := filepath.Join(root, "b.txt")
	if err := os.WriteFile(path1, []byte("content A"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path2, []byte("content B"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Different sizes aren't candidates, so make them same size
	// Actually different content, different hash
	content1 := make([]byte, 100)
	content2 := make([]byte, 100)
	content1[0] = 'A'
	content2[0] = 'B'

	if err := os.WriteFile(path1, content1, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path2, content2, 0o644); err != nil {
		t.Fatal(err)
	}

	info1 := getFileInfo(t, path1)
	info2 := getFileInfo(t, path2)

	groups := types.NewCandidateGroups([]types.CandidateGroup{
		types.NewCandidateGroup([]types.SiblingGroup{
			types.NewSiblingGroup([]*types.FileInfo{info1}),
			types.NewSiblingGroup([]*types.FileInfo{info2}),
		}),
	})

	v := New(groups, 2, false, nil, noCache)
	duplicates := v.Run()

	if duplicates.Len() != 0 {
		t.Errorf("expected 0 duplicate groups (different content), got %d", duplicates.Len())
	}
}

// TestVerifierEmptyFiles tests verification of zero-byte files.
func TestVerifierEmptyFiles(t *testing.T) {
	root := t.TempDir()

	path1 := filepath.Join(root, "empty1.txt")
	path2 := filepath.Join(root, "empty2.txt")
	if err := os.WriteFile(path1, []byte{}, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path2, []byte{}, 0o644); err != nil {
		t.Fatal(err)
	}

	info1 := getFileInfo(t, path1)
	info2 := getFileInfo(t, path2)

	groups := types.NewCandidateGroups([]types.CandidateGroup{
		types.NewCandidateGroup([]types.SiblingGroup{
			types.NewSiblingGroup([]*types.FileInfo{info1}),
			types.NewSiblingGroup([]*types.FileInfo{info2}),
		}),
	})

	v := New(groups, 2, false, nil, noCache)
	duplicates := v.Run()

	// Empty files should be considered duplicates (same content: nothing)
	if duplicates.Len() != 1 {
		t.Errorf("expected 1 duplicate group (empty files), got %d", duplicates.Len())
	}
}

// TestVerifierExactlyProbeSize tests file exactly at probeSize boundary.
func TestVerifierExactlyProbeSize(t *testing.T) {
	root := t.TempDir()

	// Create files exactly probeSize
	content := make([]byte, probeSize)
	for i := range content {
		content[i] = byte(i % 256)
	}

	path1 := filepath.Join(root, "a.bin")
	path2 := filepath.Join(root, "b.bin")
	if err := os.WriteFile(path1, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path2, content, 0o644); err != nil {
		t.Fatal(err)
	}

	info1 := getFileInfo(t, path1)
	info2 := getFileInfo(t, path2)

	groups := types.NewCandidateGroups([]types.CandidateGroup{
		types.NewCandidateGroup([]types.SiblingGroup{
			types.NewSiblingGroup([]*types.FileInfo{info1}),
			types.NewSiblingGroup([]*types.FileInfo{info2}),
		}),
	})

	v := New(groups, 2, false, nil, noCache)
	duplicates := v.Run()

	if duplicates.Len() != 1 {
		t.Fatalf("expected 1 duplicate group, got %d", duplicates.Len())
	}
}

// TestVerifierSiblingGroupOptimization tests that hardlinks are hashed once.
func TestVerifierSiblingGroupOptimization(t *testing.T) {
	root := t.TempDir()

	content := make([]byte, 100)
	path1 := filepath.Join(root, "a.txt")
	if err := os.WriteFile(path1, content, 0o644); err != nil {
		t.Fatal(err)
	}

	// Create hardlink
	path2 := filepath.Join(root, "b.txt")
	if err := os.Link(path1, path2); err != nil {
		t.Fatal(err)
	}

	// Create another file with same content (different inode)
	path3 := filepath.Join(root, "c.txt")
	if err := os.WriteFile(path3, content, 0o644); err != nil {
		t.Fatal(err)
	}

	info1 := getFileInfo(t, path1)
	info2 := getFileInfo(t, path2)
	info3 := getFileInfo(t, path3)

	// a.txt and b.txt are same inode (sibling group)
	// c.txt is different inode
	groups := types.NewCandidateGroups([]types.CandidateGroup{
		types.NewCandidateGroup([]types.SiblingGroup{
			types.NewSiblingGroup([]*types.FileInfo{info1, info2}), // sibling group (hardlinks)
			types.NewSiblingGroup([]*types.FileInfo{info3}),        // separate inode
		}),
	})

	v := New(groups, 2, false, nil, noCache)
	duplicates := v.Run()

	if duplicates.Len() != 1 {
		t.Fatalf("expected 1 duplicate group, got %d", duplicates.Len())
	}

	// Should have 2 sibling groups in the duplicate group
	if duplicates.First().Len() != 2 {
		t.Errorf("expected 2 sibling groups, got %d", duplicates.First().Len())
	}

	// First sibling group should have 2 paths (the hardlinks)
	var foundDouble bool
	for _, siblings := range duplicates.First().Items() {
		if siblings.Len() == 2 {
			foundDouble = true
		}
	}
	if !foundDouble {
		t.Error("expected to find sibling group with 2 paths (hardlinks)")
	}
}

// =============================================================================
// Section 5.3: Verifier Error Handling
// =============================================================================

// TestVerifierEmptyInput tests behavior with no candidate groups.
func TestVerifierEmptyInput(t *testing.T) {
	v := New(types.NewCandidateGroups(nil), 2, false, nil, noCache)
	duplicates := v.Run()

	if duplicates.Len() != 0 {
		t.Errorf("expected 0 for empty input, got %d", duplicates.Len())
	}
}

// TestVerifierUnreadableFile tests handling of permission denied.
func TestVerifierUnreadableFile(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("skipping permission test when running as root")
	}

	root := t.TempDir()

	content := make([]byte, 100)
	path1 := filepath.Join(root, "readable.txt")
	path2 := filepath.Join(root, "unreadable.txt")

	if err := os.WriteFile(path1, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path2, content, 0o000); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(path2, 0o644) }()

	info1 := getFileInfo(t, path1)
	info2 := getFileInfo(t, path2)

	errCh := make(chan error, 10)
	groups := types.NewCandidateGroups([]types.CandidateGroup{
		types.NewCandidateGroup([]types.SiblingGroup{
			types.NewSiblingGroup([]*types.FileInfo{info1}),
			types.NewSiblingGroup([]*types.FileInfo{info2}),
		}),
	})

	v := New(groups, 2, false, errCh, noCache)
	duplicates := v.Run()
	close(errCh)

	// Should have reported an error
	var errCount int
	for range errCh {
		errCount++
	}
	if errCount == 0 {
		t.Error("expected permission error to be reported")
	}

	// No duplicates since one file couldn't be read
	if duplicates.Len() != 0 {
		t.Errorf("expected 0 duplicates with unreadable file, got %d", duplicates.Len())
	}
}

// TestVerifierFileDeleted tests handling of file deleted between scan and verify.
func TestVerifierFileDeleted(t *testing.T) {
	root := t.TempDir()

	content := make([]byte, 100)
	path1 := filepath.Join(root, "exists.txt")
	path2 := filepath.Join(root, "deleted.txt")

	if err := os.WriteFile(path1, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path2, content, 0o644); err != nil {
		t.Fatal(err)
	}

	info1 := getFileInfo(t, path1)
	info2 := getFileInfo(t, path2)

	// Delete the second file before verification
	if err := os.Remove(path2); err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 10)
	groups := types.NewCandidateGroups([]types.CandidateGroup{
		types.NewCandidateGroup([]types.SiblingGroup{
			types.NewSiblingGroup([]*types.FileInfo{info1}),
			types.NewSiblingGroup([]*types.FileInfo{info2}),
		}),
	})

	v := New(groups, 2, false, errCh, noCache)
	duplicates := v.Run()
	close(errCh)

	// Should have reported an error
	var errCount int
	for range errCh {
		errCount++
	}
	if errCount == 0 {
		t.Error("expected file-not-found error to be reported")
	}

	// No duplicates since one file was deleted
	if duplicates.Len() != 0 {
		t.Errorf("expected 0 duplicates with deleted file, got %d", duplicates.Len())
	}
}

// TestVerifierMultipleCandidateGroups tests verification of multiple candidate groups.
func TestVerifierMultipleCandidateGroups(t *testing.T) {
	root := t.TempDir()

	// Group 1: two 100-byte duplicates
	content1 := make([]byte, 100)
	for i := range content1 {
		content1[i] = 'A'
	}
	path1a := filepath.Join(root, "a1.txt")
	path1b := filepath.Join(root, "a2.txt")
	if err := os.WriteFile(path1a, content1, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path1b, content1, 0o644); err != nil {
		t.Fatal(err)
	}

	// Group 2: two 200-byte duplicates
	content2 := make([]byte, 200)
	for i := range content2 {
		content2[i] = 'B'
	}
	path2a := filepath.Join(root, "b1.txt")
	path2b := filepath.Join(root, "b2.txt")
	if err := os.WriteFile(path2a, content2, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path2b, content2, 0o644); err != nil {
		t.Fatal(err)
	}

	info1a := getFileInfo(t, path1a)
	info1b := getFileInfo(t, path1b)
	info2a := getFileInfo(t, path2a)
	info2b := getFileInfo(t, path2b)

	groups := types.NewCandidateGroups([]types.CandidateGroup{
		types.NewCandidateGroup([]types.SiblingGroup{
			types.NewSiblingGroup([]*types.FileInfo{info1a}),
			types.NewSiblingGroup([]*types.FileInfo{info1b}),
		}),
		types.NewCandidateGroup([]types.SiblingGroup{
			types.NewSiblingGroup([]*types.FileInfo{info2a}),
			types.NewSiblingGroup([]*types.FileInfo{info2b}),
		}),
	})

	v := New(groups, 2, false, nil, noCache)
	duplicates := v.Run()

	if duplicates.Len() != 2 {
		t.Errorf("expected 2 duplicate groups, got %d", duplicates.Len())
	}
}

// =============================================================================
// Helper Functions
// =============================================================================

func getFileInfo(t *testing.T, path string) *types.FileInfo {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("failed to stat %s: %v", path, err)
	}
	stat := info.Sys().(*syscall.Stat_t)
	return &types.FileInfo{
		Path:    path,
		Size:    info.Size(),
		ModTime: info.ModTime(),
		Dev:     uint64(stat.Dev), //nolint:unconvert // platform-dependent type
		Ino:     stat.Ino,
		Nlink:   uint32(stat.Nlink),
	}
}
