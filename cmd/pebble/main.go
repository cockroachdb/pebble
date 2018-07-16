package main

import (
	"os"
	"time"

	"github.com/spf13/cobra"
)

var (
	concurrency int
	duration    time.Duration
)

var rootCmd = &cobra.Command{
	Use:   "pebble [command] (flags)",
	Short: "pebble benchmarking/introspection tool",
	Long:  ``,
}

func main() {
	cobra.EnableCommandSorting = false
	rootCmd.AddCommand(
		syncCmd,
	)

	syncCmd.Flags().IntVarP(
		&concurrency, "concurrency", "c", 1, "number of concurrent workers")
	syncCmd.Flags().DurationVarP(
		&duration, "duration", "d", 10*time.Second, "the duration to run (0, run forever)")

	if err := rootCmd.Execute(); err != nil {
		// Cobra has already printed the error message.
		os.Exit(1)
	}
}
