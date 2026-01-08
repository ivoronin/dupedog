// Package cache provides file-based caching for progressive hash verification.
package cache

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/ivoronin/dupedog/internal/types"
)

const (
	bucketName = "hashes"
	hashSize   = 32
)

// Cache provides persistent caching of file hashes using BoltDB.
// Implements self-cleaning: each run creates a new database, only used entries survive.
type Cache struct {
	readDB  *bolt.DB // Existing cache (read-only)
	writeDB *bolt.DB // New cache (write) - BoltDB locks this file
	path    string   // Final path (for atomic swap)
	enabled bool
}

// Open opens existing cache for reading and creates new cache for writing.
// BoltDB's built-in file locking on .new file prevents concurrent instances.
// Returns disabled cache if path is empty.
func Open(path string) (*Cache, error) {
	if path == "" {
		return &Cache{enabled: false}, nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}

	c := &Cache{path: path, enabled: true}
	var err error

	// Open existing cache for reading (if exists)
	if _, statErr := os.Stat(path); statErr == nil {
		c.readDB, err = bolt.Open(path, 0o600, &bolt.Options{
			ReadOnly: true,
			Timeout:  1 * time.Second,
		})
		if err != nil {
			// Can't open existing - continue without read cache
			c.readDB = nil
		}
	}

	// Create new cache for writing - BoltDB locks this file
	newPath := path + ".new"
	c.writeDB, err = bolt.Open(newPath, 0o600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("create new cache (locked by another instance?): %w", err)
	}

	// Create bucket in new cache
	if err := c.writeDB.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	}); err != nil {
		_ = c.Close()
		return nil, err
	}

	return c, nil
}

// Close closes both databases and atomically replaces old with new.
// Only replaces if write database closed successfully to avoid data loss.
func (c *Cache) Close() error {
	var errs []error
	if c.readDB != nil {
		if err := c.readDB.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if c.writeDB != nil {
		if err := c.writeDB.Close(); err != nil {
			errs = append(errs, err)
		} else {
			// Atomic replace: rename new → old (only if close succeeded)
			if err := os.Rename(c.path+".new", c.path); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

const keyVersion byte = 1 // Increment when key format changes

// makeKey builds deterministic byte key for BoltDB lookup.
// Key = ver(1) + path + NUL + fileSize(8) + ino(8) + mtime(8) + start(8) + size(8)
func makeKey(fi *types.FileInfo, start, size int64) []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte(keyVersion)
	buf.WriteString(fi.Path)
	buf.WriteByte(0) // NUL separator
	_ = binary.Write(buf, binary.BigEndian, fi.Size)
	_ = binary.Write(buf, binary.BigEndian, fi.Ino)
	_ = binary.Write(buf, binary.BigEndian, fi.ModTime.UnixNano())
	_ = binary.Write(buf, binary.BigEndian, start)
	_ = binary.Write(buf, binary.BigEndian, size)
	return buf.Bytes()
}

// Lookup retrieves a cached hash for a byte range.
// Key = (path, fileSize, ino, mtime, start, size) - any change = cache miss.
// On HIT: copies entry to writeDB (self-cleaning).
// Returns (nil, nil) if not found, (nil, err) on read error.
func (c *Cache) Lookup(fi *types.FileInfo, start, size int64) ([]byte, error) {
	if !c.enabled || c.readDB == nil {
		return nil, nil
	}

	key := makeKey(fi, start, size)
	var hash []byte

	err := c.readDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return nil
		}
		data := b.Get(key)
		if len(data) == hashSize {
			hash = make([]byte, hashSize)
			copy(hash, data)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("cache lookup: %w", err)
	}

	if hash == nil {
		return nil, nil
	}

	// Self-cleaning: copy valid entry to new database
	_ = c.Store(fi, start, size, hash)

	return hash, nil
}

// Store saves a hash for a byte range to the new database.
func (c *Cache) Store(fi *types.FileInfo, start, size int64, hash []byte) error {
	if !c.enabled || c.writeDB == nil || len(hash) != hashSize {
		return nil
	}

	err := c.writeDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		return b.Put(makeKey(fi, start, size), hash)
	})
	if err != nil {
		return fmt.Errorf("cache store: %w", err)
	}
	return nil
}
