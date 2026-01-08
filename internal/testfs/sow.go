package testfs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/dustin/go-humanize"
)

// -----------------------------------------------------------------------------
// Sow Operations - Create filesystem from spec
// -----------------------------------------------------------------------------

// SowFileTree creates a filesystem structure from a FileTree specification.
//
// The root parameter specifies the base directory where volumes are created.
// Each volume's MountPoint becomes a subdirectory under root.
//
// For E2E tests, root is "/" and MountPoints are actual tmpfs mounts.
// For integration tests, root is t.TempDir() and MountPoints become subdirs.
func SowFileTree(root string, spec FileTree) error {
	for _, vol := range spec.Volumes {
		if err := sowVolume(root, vol); err != nil {
			return fmt.Errorf("sow volume %s: %w", vol.MountPoint, err)
		}
	}
	return nil
}

// SowFromReader reads a FileTree JSON from the reader and creates the filesystem.
// Used by testfs-helper CLI tool to read from stdin.
func SowFromReader(r io.Reader, root string) error {
	var spec FileTree
	if err := json.NewDecoder(r).Decode(&spec); err != nil {
		return fmt.Errorf("decode spec: %w", err)
	}
	return SowFileTree(root, spec)
}

// sowVolume creates all files and symlinks in a volume.
func sowVolume(root string, vol Volume) error {
	volPath := resolveVolumePath(root, vol.MountPoint)

	if err := os.MkdirAll(volPath, 0o755); err != nil {
		return fmt.Errorf("create volume dir: %w", err)
	}

	if err := sowFiles(volPath, vol.Files); err != nil {
		return err
	}

	return sowSymlinks(volPath, vol.Symlinks)
}

// resolveVolumePath determines the actual filesystem path for a volume.
func resolveVolumePath(root, mountPoint string) string {
	if root == "" || root == "/" {
		return mountPoint
	}
	return filepath.Join(root, mountPoint)
}

// sowFiles creates files and their hardlinks.
func sowFiles(volPath string, files []File) error {
	for _, f := range files {
		if err := sowFile(volPath, f); err != nil {
			return err
		}
	}
	return nil
}

// sowFile creates a single file entry (with optional hardlinks).
func sowFile(volPath string, f File) error {
	if len(f.Path) == 0 {
		return nil
	}

	firstPath := filepath.Join(volPath, f.Path[0])
	if err := writeChunkedFile(firstPath, f.Chunks); err != nil {
		return fmt.Errorf("create %s: %w", firstPath, err)
	}

	for _, p := range f.Path[1:] {
		linkPath := filepath.Join(volPath, p)
		if err := createHardlink(firstPath, linkPath); err != nil {
			return fmt.Errorf("hardlink %s -> %s: %w", linkPath, firstPath, err)
		}
	}
	return nil
}

// writeChunkedFile streams content directly to disk.
// Efficiently handles both tiny (100B) and huge (1GiB) chunks.
func writeChunkedFile(path string, chunks []Chunk) (err error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	for _, c := range chunks {
		if err := writeChunk(f, c); err != nil {
			return err
		}
	}
	return nil
}

// writeChunk writes a single chunk to the file using streaming.
func writeChunk(f *os.File, c Chunk) error {
	const maxBufSize = 1 << 20 // 1MiB max buffer

	size, err := humanize.ParseBytes(c.Size)
	if err != nil {
		return fmt.Errorf("parse chunk size %q: %w", c.Size, err)
	}

	// Use smaller buffer for small chunks
	bufSize := int(size)
	if bufSize > maxBufSize {
		bufSize = maxBufSize
	}

	// Create pattern-filled buffer
	buf := bytes.Repeat([]byte{byte(c.Pattern)}, bufSize)

	// Stream write
	remaining := int64(size)
	for remaining > 0 {
		toWrite := int64(len(buf))
		if remaining < toWrite {
			toWrite = remaining
		}
		if _, err := f.Write(buf[:toWrite]); err != nil {
			return err
		}
		remaining -= toWrite
	}
	return nil
}

// sowSymlinks creates symlinks in a volume.
func sowSymlinks(volPath string, symlinks []Symlink) error {
	for _, sym := range symlinks {
		linkPath := filepath.Join(volPath, sym.Path)
		if err := createSymlink(sym.Target, linkPath); err != nil {
			return fmt.Errorf("symlink %s -> %s: %w", linkPath, sym.Target, err)
		}
	}
	return nil
}

// -----------------------------------------------------------------------------
// Helper Functions
// -----------------------------------------------------------------------------

// createHardlink creates a hardlink, creating parent dirs.
func createHardlink(target, link string) error {
	if err := os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
		return err
	}
	return os.Link(target, link)
}

// createSymlink creates a symlink, creating parent dirs.
func createSymlink(target, link string) error {
	if err := os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
		return err
	}
	return os.Symlink(target, link)
}
