package main

import (
	"os"
	"time"

	"github.com/spf13/cobra"
)

var (
	concurrency int
	duration    time.Duration
	wipe        bool
)

var rootCmd = &cobra.Command{
	Use:   "pebble [command] (flags)",
	Short: "pebble benchmarking/introspection tool",
	Long:  ``,
}

func main() {
	cobra.EnableCommandSorting = false
	rootCmd.AddCommand(
		scanCmd,
		syncCmd,
	)

	for _, cmd := range []*cobra.Command{scanCmd, syncCmd} {
		cmd.Flags().IntVarP(
			&concurrency, "concurrency", "c", 1, "number of concurrent workers")
		cmd.Flags().DurationVarP(
			&duration, "duration", "d", 10*time.Second, "the duration to run (0, run forever)")
		cmd.Flags().BoolVarP(
			&wipe, "wipe", "w", false, "wipe the database before starting")
	}

	scanCmd.Flags().BoolVarP(
		&scanReverse, "reverse", "r", false, "reverse scan")
	scanCmd.Flags().IntVar(
		&scanRows, "rows", scanRows, "number of rows to scan in each operation")
	scanCmd.Flags().IntVar(
		&scanValueSize, "value", scanValueSize, "size of values to scan")

	if err := rootCmd.Execute(); err != nil {
		// Cobra has already printed the error message.
		os.Exit(1)
	}
}
