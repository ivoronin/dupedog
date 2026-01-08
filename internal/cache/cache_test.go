package cache

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ivoronin/dupedog/internal/types"
)

func TestCacheDisabled(t *testing.T) {
	c, err := Open("")
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer func() { _ = c.Close() }()

	fi := &types.FileInfo{Path: "/test/file", Size: 100, Ino: 1234, ModTime: time.Now()}
	hash := []byte("12345678901234567890123456789012") // 32 bytes

	// Store should be no-op when disabled
	c.Store(fi, 0, 100, hash)

	// Lookup should return nil when disabled
	result := c.Lookup(fi, 0, 100)
	if result != nil {
		t.Errorf("Lookup() on disabled cache returned %v, want nil", result)
	}
}

func TestCacheRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	cachePath := filepath.Join(tmpDir, "cache.db")

	// First run: store entries
	c1, err := Open(cachePath)
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}

	fi := &types.FileInfo{
		Path:    "/test/file.txt",
		Size:    1024,
		Ino:     12345,
		ModTime: time.Unix(1609459200, 0),
	}
	hash := []byte("abcdefghijklmnopqrstuvwxyz012345") // 32 bytes

	// Store various byte ranges
	c1.Store(fi, 0, 1024, hash)            // full file
	c1.Store(fi, 0, 512, hash)             // first half
	c1.Store(fi, 512, 512, hash)           // second half
	c1.Store(fi, 1<<30, 1<<30, hash)       // 1GB chunk at 1GB offset

	if err := c1.Close(); err != nil {
		t.Fatalf("Close() failed: %v", err)
	}

	// Second run: lookup entries
	c2, err := Open(cachePath)
	if err != nil {
		t.Fatalf("Open() second time failed: %v", err)
	}
	defer func() { _ = c2.Close() }()

	// All ranges should hit
	for _, tc := range []struct {
		start int64
		size  int64
	}{
		{0, 1024},
		{0, 512},
		{512, 512},
		{1 << 30, 1 << 30},
	} {
		result := c2.Lookup(fi, tc.start, tc.size)
		if result == nil {
			t.Errorf("Lookup(start=%d, size=%d) returned nil, want hash", tc.start, tc.size)
			continue
		}
		if !bytes.Equal(result, hash) {
			t.Errorf("Lookup(start=%d, size=%d) = %q, want %q", tc.start, tc.size, result, hash)
		}
	}
}

func TestCacheMissOnMtimeChange(t *testing.T) {
	tmpDir := t.TempDir()
	cachePath := filepath.Join(tmpDir, "cache.db")

	// Store with original mtime
	c1, _ := Open(cachePath)
	fi := &types.FileInfo{
		Path:    "/test/file.txt",
		Size:    1024,
		Ino:     12345,
		ModTime: time.Unix(1609459200, 0),
	}
	hash := []byte("abcdefghijklmnopqrstuvwxyz012345")
	c1.Store(fi, 0, 1024, hash)
	_ = c1.Close()

	// Lookup with different mtime
	c2, _ := Open(cachePath)
	defer func() { _ = c2.Close() }()

	fiModified := &types.FileInfo{
		Path:    fi.Path,
		Size:    fi.Size,
		Ino:     fi.Ino,
		ModTime: time.Unix(1609459201, 0), // 1 second later
	}

	result := c2.Lookup(fiModified, 0, 1024)
	if result != nil {
		t.Errorf("Lookup() with different mtime returned %v, want nil", result)
	}
}

func TestCacheMissOnSizeChange(t *testing.T) {
	tmpDir := t.TempDir()
	cachePath := filepath.Join(tmpDir, "cache.db")

	c1, _ := Open(cachePath)
	fi := &types.FileInfo{Path: "/test/file.txt", Size: 1024, Ino: 12345, ModTime: time.Now()}
	hash := []byte("abcdefghijklmnopqrstuvwxyz012345")
	c1.Store(fi, 0, 1024, hash)
	_ = c1.Close()

	c2, _ := Open(cachePath)
	defer func() { _ = c2.Close() }()

	fiDifferentSize := &types.FileInfo{Path: fi.Path, Size: 2048, Ino: fi.Ino, ModTime: fi.ModTime}
	result := c2.Lookup(fiDifferentSize, 0, 1024)
	if result != nil {
		t.Errorf("Lookup() with different file size returned %v, want nil", result)
	}
}

func TestCacheMissOnInodeChange(t *testing.T) {
	tmpDir := t.TempDir()
	cachePath := filepath.Join(tmpDir, "cache.db")

	c1, _ := Open(cachePath)
	fi := &types.FileInfo{Path: "/test/file.txt", Size: 1024, Ino: 12345, ModTime: time.Now()}
	hash := []byte("abcdefghijklmnopqrstuvwxyz012345")
	c1.Store(fi, 0, 1024, hash)
	_ = c1.Close()

	c2, _ := Open(cachePath)
	defer func() { _ = c2.Close() }()

	// Simulates: file deleted, new file created with same path (different inode)
	fiDifferentIno := &types.FileInfo{Path: fi.Path, Size: fi.Size, Ino: 99999, ModTime: fi.ModTime}
	result := c2.Lookup(fiDifferentIno, 0, 1024)
	if result != nil {
		t.Errorf("Lookup() with different inode returned %v, want nil", result)
	}
}

func TestCacheMissOnPathChange(t *testing.T) {
	tmpDir := t.TempDir()
	cachePath := filepath.Join(tmpDir, "cache.db")

	c1, _ := Open(cachePath)
	fi := &types.FileInfo{Path: "/test/original.txt", Size: 1024, Ino: 12345, ModTime: time.Now()}
	hash := []byte("abcdefghijklmnopqrstuvwxyz012345")
	c1.Store(fi, 0, 1024, hash)
	_ = c1.Close()

	c2, _ := Open(cachePath)
	defer func() { _ = c2.Close() }()

	fiDifferentPath := &types.FileInfo{Path: "/test/renamed.txt", Size: fi.Size, Ino: fi.Ino, ModTime: fi.ModTime}
	result := c2.Lookup(fiDifferentPath, 0, 1024)
	if result != nil {
		t.Errorf("Lookup() with different path returned %v, want nil", result)
	}
}

func TestCacheMissOnStartChange(t *testing.T) {
	tmpDir := t.TempDir()
	cachePath := filepath.Join(tmpDir, "cache.db")

	c1, _ := Open(cachePath)
	fi := &types.FileInfo{Path: "/test/file.txt", Size: 1024, Ino: 12345, ModTime: time.Now()}
	hash := []byte("abcdefghijklmnopqrstuvwxyz012345")
	c1.Store(fi, 0, 512, hash) // Store first 512 bytes
	_ = c1.Close()

	c2, _ := Open(cachePath)
	defer func() { _ = c2.Close() }()

	// Lookup with different start offset - should miss
	result := c2.Lookup(fi, 512, 512)
	if result != nil {
		t.Errorf("Lookup() with different start returned %v, want nil", result)
	}
}

func TestCacheMissOnRangeSizeChange(t *testing.T) {
	tmpDir := t.TempDir()
	cachePath := filepath.Join(tmpDir, "cache.db")

	c1, _ := Open(cachePath)
	fi := &types.FileInfo{Path: "/test/file.txt", Size: 1024, Ino: 12345, ModTime: time.Now()}
	hash := []byte("abcdefghijklmnopqrstuvwxyz012345")
	c1.Store(fi, 0, 512, hash) // Store range [0, 512)
	_ = c1.Close()

	c2, _ := Open(cachePath)
	defer func() { _ = c2.Close() }()

	// Lookup with same start but different size - should miss
	result := c2.Lookup(fi, 0, 1024)
	if result != nil {
		t.Errorf("Lookup() with different range size returned %v, want nil", result)
	}
}

func TestSelfCleaning(t *testing.T) {
	tmpDir := t.TempDir()
	cachePath := filepath.Join(tmpDir, "cache.db")

	// First run: store two entries
	c1, _ := Open(cachePath)
	fiA := &types.FileInfo{Path: "/a.txt", Size: 100, Ino: 1, ModTime: time.Now()}
	fiB := &types.FileInfo{Path: "/b.txt", Size: 200, Ino: 2, ModTime: time.Now()}
	hash := []byte("abcdefghijklmnopqrstuvwxyz012345")
	c1.Store(fiA, 0, 100, hash)
	c1.Store(fiB, 0, 200, hash)
	_ = c1.Close()

	// Second run: only lookup fiA (fiB becomes orphan)
	c2, _ := Open(cachePath)
	c2.Lookup(fiA, 0, 100) // Hit - will be copied to new DB
	// fiB is NOT looked up
	_ = c2.Close()

	// Third run: fiB should be gone (self-cleaned)
	c3, _ := Open(cachePath)
	defer func() { _ = c3.Close() }()

	// fiA should still exist
	if c3.Lookup(fiA, 0, 100) == nil {
		t.Error("fiA should exist after self-cleaning")
	}

	// fiB should be gone (not looked up in run 2)
	if c3.Lookup(fiB, 0, 200) != nil {
		t.Error("fiB should have been cleaned")
	}
}

func TestInvalidHashSize(t *testing.T) {
	tmpDir := t.TempDir()
	cachePath := filepath.Join(tmpDir, "cache.db")

	c, _ := Open(cachePath)
	defer func() { _ = c.Close() }()

	fi := &types.FileInfo{Path: "/test.txt", Size: 100, Ino: 1, ModTime: time.Now()}

	// Store with wrong hash size - should be ignored
	c.Store(fi, 0, 100, []byte("too short"))

	// Lookup should return nil
	result := c.Lookup(fi, 0, 100)
	if result != nil {
		t.Errorf("Lookup() after invalid Store returned %v, want nil", result)
	}
}

func TestMakeKeyDeterministic(t *testing.T) {
	fi := &types.FileInfo{
		Path:    "/test/file.txt",
		Size:    1024,
		Ino:     12345,
		ModTime: time.Unix(1609459200, 123456789),
	}

	key1 := makeKey(fi, 0, 512)
	key2 := makeKey(fi, 0, 512)

	if !bytes.Equal(key1, key2) {
		t.Error("makeKey() not deterministic")
	}
}

func TestCacheDirCreation(t *testing.T) {
	tmpDir := t.TempDir()
	nestedPath := filepath.Join(tmpDir, "a", "b", "c", "cache.db")

	c, err := Open(nestedPath)
	if err != nil {
		t.Fatalf("Open() failed with nested path: %v", err)
	}
	_ = c.Close()

	// Check directory was created
	if _, err := os.Stat(filepath.Dir(nestedPath)); os.IsNotExist(err) {
		t.Error("Cache directory was not created")
	}
}
