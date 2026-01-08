package main

import (
	"os"

	"github.com/spf13/cobra"
)

var (
	version = "dev"
	commit  = "none"
)

func main() {
	os.Exit(run())
}

func run() int {
	root := &cobra.Command{
		Use:     "dupedog",
		Short:   "Find and deduplicate files",
		Version: version + " (" + commit + ")",
	}

	root.AddCommand(newDedupeCmd())

	if err := root.Execute(); err != nil {
		return 1
	}
	return 0
}
