// One flow, built once, run whichever way you choose at startup.
//
//	go run .                  inline: goroutines, in this process
//	go run . -mode temporal   durable: the same cues as Temporal activities
//	go run . -mode both       run each in turn and compare
//
// The flow itself is in ./flow and is identical either way. What changes is the
// Runner chosen in main, which is the shape a real service would take: inline
// for tests and local development, Temporal in production, decided by config
// rather than by editing the flow.
//
// The README in the examples directory says which of them answers which
// question, if this is not the one you wanted.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/lordtatty/deck/examples/eitherway/flow"
)

func newRunner(mode string) Runner {
	switch mode {
	case "inline":
		return Inline{}
	case "temporal":
		return NewTemporal()
	default:
		log.Fatalf("unknown mode %q: want inline, temporal, or both", mode)
		return nil
	}
}

// execute returns its error rather than calling log.Fatal, because Fatal skips
// deferred calls — and one of them shuts down the Temporal dev server this
// example started.
func execute(mode string, in flow.Input) (flow.State, error) {
	runner := newRunner(mode)
	defer runner.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	start := time.Now()
	state, err := runner.Run(ctx, in)
	if err != nil {
		return state, fmt.Errorf("%s: %w", mode, err)
	}

	fmt.Printf("%-10s %-46s %v\n", mode, state.Indexed, time.Since(start).Round(10*time.Millisecond))
	return state, nil
}

func mustExecute(mode string, in flow.Input) flow.State {
	state, err := execute(mode, in)
	if err != nil {
		log.Fatal(err)
	}
	return state
}

// main returns the exit code from run so that run's deferred shutdowns still
// happen: os.Exit would skip them and leave a dev server running.
func main() {
	os.Exit(run())
}

func run() int {
	mode := flag.String("mode", "inline", "inline, temporal, or both")
	flag.Parse()

	in := flow.Input{URL: "report.html"}

	fmt.Println("fetch, then summarise / classify / keywords together, then index.")
	fmt.Println()
	fmt.Println("Five cues, but only three declare work with deck.Do — the other two")
	fmt.Println("just read state, so they run inline and cost nothing. Under Temporal")
	fmt.Println("that is three activities, not five.")
	fmt.Println()
	fmt.Println("300ms + 400ms of parallel work = about 700ms if summarise and classify")
	fmt.Println("really do overlap; 1.1s if they do not.")
	if *mode != "inline" {
		fmt.Println("(starting a Temporal dev server — downloaded once, then cached)")
	}
	fmt.Println()
	fmt.Printf("%-10s %-46s %s\n", "MODE", "RESULT", "TOOK")

	switch *mode {
	case "both":
		inline := mustExecute("inline", in)
		durable := mustExecute("temporal", in)

		// Disagreeing is a failure, not a remark: this comparison is the point
		// of the example, and what makes running it in CI worth anything.
		fmt.Println()
		if inline.Indexed != durable.Indexed {
			fmt.Fprintf(os.Stderr, "MISMATCH\n  inline:   %s\n  temporal: %s\n", inline.Indexed, durable.Indexed)
			return 1
		}
		fmt.Println("Identical results. The flow was compiled once and never")
		fmt.Println("asked which world it was running in.")
	default:
		state := mustExecute(*mode, in)
		fmt.Println()
		fmt.Println("keywords: " + strings.Join(state.Keywords, ", "))
	}
	return 0
}
