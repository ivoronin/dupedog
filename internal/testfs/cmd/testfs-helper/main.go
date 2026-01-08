//go:build linux

// testfs-helper is a binary helper for E2E tests that runs inside containers.
//
// It provides two modes for filesystem operations:
//
//	testfs-helper sow   - Create filesystem from JSON spec (stdin)
//	testfs-helper reap  - Capture filesystem state as JSON (stdout)
//
// This is a thin wrapper around the testfs package functions.
package main

import (
	"fmt"
	"os"

	"github.com/ivoronin/dupedog/internal/testfs"
)

func main() {
	if len(os.Args) < 2 {
		fatalf("usage: testfs-helper <sow|reap> [paths...]")
	}

	switch os.Args[1] {
	case "sow":
		cmdSow()
	case "reap":
		if len(os.Args) < 3 {
			fatalf("usage: testfs-helper reap <path> [path...]")
		}
		cmdReap(os.Args[2:])
	default:
		fatalf("unknown command: %s (use 'sow' or 'reap')", os.Args[1])
	}
}

// cmdSow reads a FileTree JSON from stdin and creates the filesystem.
func cmdSow() {
	// Root is "/" since we're in a container with actual tmpfs mounts
	if err := testfs.SowFromReader(os.Stdin, "/"); err != nil {
		fatalf("sow: %v", err)
	}
}

// cmdReap scans paths and outputs filesystem state as JSON.
func cmdReap(paths []string) {
	if err := testfs.ReapToWriter(os.Stdout, paths); err != nil {
		fatalf("reap: %v", err)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "testfs-helper: "+format+"\n", args...)
	os.Exit(1)
}
