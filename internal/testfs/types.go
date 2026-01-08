// Package testfs provides test infrastructure for filesystem operations.
//
// It supports two modes:
//   - Integration tests: TempDirHarness creates files in t.TempDir()
//   - E2E tests: DockerHarness uses Docker containers with tmpfs mounts
//
// The E2E mode enables cross-device deduplication testing where each
// tmpfs mount appears as a separate filesystem with distinct device IDs.
//
// # Unified FileTree Specification
//
// Tests use a single FileTree type for both setup and verification:
//
//	given := testfs.FileTree{
//	    Volumes: []Volume{
//	        {
//	            MountPoint: "/data",
//	            Files: []File{
//	                {Path: []string{"a.txt", "backup/a.txt"}, Chunks: []Chunk{{Pattern: 'A', Size: "1MiB"}}},
//	            },
//	        },
//	        {
//	            MountPoint: "/data/subdir",  // Nested mount on different device
//	            Files: []File{
//	                {Path: []string{"mirror/a.txt"}, Chunks: []Chunk{{Pattern: 'A', Size: "1MiB"}}},
//	            },
//	        },
//	    },
//	}
//	then := testfs.FileTree{
//	    Volumes: []Volume{
//	        {
//	            MountPoint: "/data",
//	            Files: []File{
//	                {Path: []string{"a.txt", "backup/a.txt"}}, // same inode
//	            },
//	        },
//	        {
//	            MountPoint: "/data/subdir",
//	            Symlinks: []Symlink{
//	                {Path: "mirror/a.txt", Target: "../a.txt"},
//	            },
//	        },
//	    },
//	}
//
// Subdirectories are created automatically from file paths (mkdir -p semantics).
// File paths are relative to the volume mount point.
//
//	h := testfs.New(t, given)
//	h.RunDupedog("--symlink-fallback", "/data", "/data/subdir")
//	h.Assert(then)
//
// # Context-Dependent Field Usage
//
//	| Field          | Setup              | Verification             |
//	|----------------|--------------------|--------------------------|
//	| Volumes        | Creates mounts     | Scope for assertions     |
//	| File.Path      | Create file/links  | Assert same inode        |
//	| File.Chunks    | Generate content   | Ignored                  |
//	| Symlink.Path   | Create symlink     | Assert is symlink        |
//	| Symlink.Target | Symlink target     | Assert symlink target    |
//	| ExitCode       | Ignored            | Assert matches           |
package testfs

import "github.com/dustin/go-humanize"

// -----------------------------------------------------------------------------
// FileTree Specification Types
// -----------------------------------------------------------------------------

// FileTree describes a filesystem state (used for both setup and verification).
type FileTree struct {
	// Volumes in the filesystem (each is a separate tmpfs mount).
	Volumes []Volume `json:"volumes"`

	// ExitCode expected from dupedog (verification only, default 0).
	ExitCode int `json:"-"` // Not serialized - harness-only field
}

// Volume represents a separate filesystem (tmpfs mount).
//
// Each volume appears as a distinct filesystem with its own device ID,
// enabling testing of cross-device scenarios where hardlinks fail with EXDEV.
type Volume struct {
	// MountPoint is the absolute path where this volume is mounted.
	// Examples: "/data", "/data/subdir", "/vol1"
	// Nested mounts are supported (e.g., "/data/subdir" inside "/data").
	MountPoint string `json:"mountPoint"`

	// Files in this volume (regular files, possibly hardlinked).
	Files []File `json:"files,omitempty"`

	// Symlinks in this volume.
	Symlinks []Symlink `json:"symlinks,omitempty"`
}

// File defines a regular file, possibly with hardlinks.
//
// In setup context:
//   - Path[0] is created with content from Chunks specification
//   - Path[1:] are hardlinked to Path[0]
//
// In verification context:
//   - All paths must exist
//   - All paths must share the same inode
//
// Content is specified via Chunks - each chunk fills a region with its pattern byte.
// Same chunks = same content = duplicates detected.
type File struct {
	// Path contains one or more paths (relative to volume).
	// Multiple paths indicate hardlinks sharing the same inode.
	// Example: []string{"data/file.txt", "backup/file.txt"}
	Path []string `json:"path"`

	// Chunks specifies file content as a sequence of filled regions.
	// Each chunk fills its size with the pattern byte.
	// Use IEC units for sizes: "1KiB", "1MiB", "1GiB".
	Chunks []Chunk `json:"chunks,omitempty"`
}

// Chunk defines a region of file content filled with a pattern byte.
type Chunk struct {
	// Pattern is the fill byte for this chunk region.
	// Example: 'A' fills the region with 0x41 bytes.
	Pattern rune `json:"pattern"`

	// Size in IEC units (1024-based): "1KiB", "1MiB", "1GiB".
	// Parsed via go-humanize for precise alignment with verifier boundaries.
	Size string `json:"size"`
}

// TotalSize calculates the sum of all chunk sizes in bytes.
func (f *File) TotalSize() int64 {
	var total int64
	for _, c := range f.Chunks {
		size, _ := humanize.ParseBytes(c.Size)
		total += int64(size)
	}
	return total
}

// Symlink defines a symbolic link.
//
// In setup context:
//   - Creates a symlink at Path pointing to Target
//
// In verification context:
//   - Asserts Path exists and is a symlink
//   - Asserts the symlink points to Target
type Symlink struct {
	// Path is relative to the volume mount point.
	Path string `json:"path"`

	// Target is the absolute path the symlink points to.
	Target string `json:"target"`
}

// -----------------------------------------------------------------------------
// Execution Result Types
// -----------------------------------------------------------------------------

// RunResult captures the results of a dupedog execution.
type RunResult struct {
	ExitCode int    // Process exit code
	Stdout   string // Standard output
	Stderr   string // Standard error
}

// -----------------------------------------------------------------------------
// Reap Types (filesystem state captured from container)
// -----------------------------------------------------------------------------

// ReapResult is the output format from testfs-helper reap command.
// It captures the actual filesystem state for verification against expected FileTree.
type ReapResult struct {
	Volumes []ReapVolume `json:"volumes"`
}

// ReapVolume contains scanned filesystem state for a single volume.
type ReapVolume struct {
	Name     string        `json:"name"`               // Mount point path (e.g., "/data")
	Files    []ReapFile    `json:"files,omitempty"`    // Regular files (grouped by inode)
	Symlinks []ReapSymlink `json:"symlinks,omitempty"` // Symbolic links
}

// ReapFile contains file metadata including inode for hardlink verification.
type ReapFile struct {
	Path  []string `json:"path"`  // All paths sharing this inode
	Inode uint64   `json:"inode"` // Inode number
	Nlink uint64   `json:"nlink"` // Link count
	Size  int64    `json:"size"`  // File size in bytes
}

// ReapSymlink contains symlink metadata.
type ReapSymlink struct {
	Path   string `json:"path"`   // Symlink path (relative to volume)
	Target string `json:"target"` // Symlink target
}
