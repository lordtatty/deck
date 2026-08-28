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
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
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

func execute(mode string, in flow.Input) flow.State {
	runner := newRunner(mode)
	defer runner.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	start := time.Now()
	state, err := runner.Run(ctx, in)
	if err != nil {
		log.Fatalf("%s: %v", mode, err)
	}

	fmt.Printf("%-10s %-46s %v\n", mode, state.Indexed, time.Since(start).Round(10*time.Millisecond))
	return state
}

func main() {
	mode := flag.String("mode", "inline", "inline, temporal, or both")
	flag.Parse()

	in := flow.Input{URL: "report.html"}

	fmt.Println("fetch, then summarise and keywords together, then index.")
	fmt.Println("300ms + 400ms of parallel work = about 700ms if the two middle")
	fmt.Println("cues really do overlap; 1.1s if they do not.")
	if *mode != "inline" {
		fmt.Println("(starting a Temporal dev server — downloaded once, then cached)")
	}
	fmt.Println()
	fmt.Printf("%-10s %-46s %s\n", "MODE", "RESULT", "TOOK")

	switch *mode {
	case "both":
		inline := execute("inline", in)
		durable := execute("temporal", in)

		fmt.Println()
		if inline.Indexed == durable.Indexed {
			fmt.Println("Identical results. The flow was compiled once and never")
			fmt.Println("asked which world it was running in.")
		} else {
			fmt.Println("MISMATCH:")
			fmt.Println("  inline:   " + inline.Indexed)
			fmt.Println("  temporal: " + durable.Indexed)
		}
	default:
		state := execute(*mode, in)
		fmt.Println()
		fmt.Println("keywords: " + strings.Join(state.Keywords, ", "))
	}
}
