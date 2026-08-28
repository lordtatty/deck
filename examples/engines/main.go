package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/lordtatty/deck"
)

// Cues can do their work themselves, or they can declare it and let the Deck's
// Engine perform it. This example shows the difference the Engine makes to
// exactly the same set of cues.
//
// The Engine is where work runs. Leave it nil and you get goroutines and the
// wall clock, which is what every Deck did before Engines existed. Set it and
// the same cues run somewhere else — one at a time for a reproducible test, or
// inside a durable workflow engine (see the temporal example).

type ReportInput struct {
	Title string
}

type ReportState struct {
	Sections []string
	Summary  string
}

// fetchSection is an ordinary Go function. Nothing about it knows what a Deck
// is, and that is the point: this is the shape of work a cue can hand off.
func fetchSection(ctx context.Context, name string) (string, error) {
	delay := map[string]time.Duration{
		"summary": 300 * time.Millisecond,
		"charts":  200 * time.Millisecond,
		"tables":  100 * time.Millisecond,
	}[name]

	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return "", fmt.Errorf("fetching %s: %w", name, ctx.Err())
	}
	return name, nil
}

// cues builds four cues, and they are deliberately not all the same shape.
//
// Not every cue should declare work. The choice is about what the cue actually
// does:
//
//   - deck.Do  — the work leaves this process, or is slow, or is not
//     deterministic: a network call, a database query, an LLM request. The
//     Engine decides where it runs, and under a durable engine it becomes a
//     retryable, recorded unit of work.
//
//   - deck.Complete — the cue only computes from state it already has. Handing
//     that to an Engine buys nothing; under Temporal it would cost a round trip
//     to the server and an entry in the workflow history for a string join.
func cues() []deck.Cue[ReportInput, ReportState] {
	var out []deck.Cue[ReportInput, ReportState]

	// Three fetches. Each one goes somewhere slow, so each declares its work.
	for _, n := range []string{"summary", "charts", "tables"} {
		name := n
		out = append(out, deck.Cue[ReportInput, ReportState]{
			Name: name,
			Run: func(_ ReportInput, _ ReportState) (deck.Mutation[ReportState], error) {
				// Do says what should happen and what to do with the result.
				// It does not block: the Engine starts the work, and the Deck
				// carries on triggering other cues meanwhile.
				return deck.Do(fetchSection, name, func(section string) deck.Mutation[ReportState] {
					return deck.Complete(func(s *ReportState) {
						s.Sections = append(s.Sections, section)
					})
				}), nil
			},
		})
	}

	// The assembly step. It waits for all three, then does nothing but read
	// what they gathered — so it stays inline, whatever the Engine is.
	out = append(out, deck.Cue[ReportInput, ReportState]{
		Name: "assemble",
		When: func(_ ReportInput, _ ReportState, r deck.Result) bool {
			return r.Completed("summary") && r.Completed("charts") && r.Completed("tables")
		},
		Run: func(in ReportInput, s ReportState) (deck.Mutation[ReportState], error) {
			summary := fmt.Sprintf("%s: %d sections", in.Title, len(s.Sections))
			return deck.Complete(func(s *ReportState) { s.Summary = summary }), nil
		},
	})

	return out
}

func run(label string, engine deck.Engine) {
	d, err := deck.New(cues()...)
	if err != nil {
		log.Fatal(err)
	}
	d.Engine = engine // nil means goroutines and the wall clock

	var state ReportState
	start := time.Now()
	result, err := d.Run(context.Background(), ReportInput{Title: "Q3"}, &state)
	if err != nil {
		log.Fatal(err)
	}

	order := make([]string, 0, len(result.CompletedCues))
	for _, c := range result.CompletedCues {
		order = append(order, c.Name)
	}
	fmt.Printf("%-22s %-42s %v\n",
		label,
		strings.Join(order, " -> "),
		time.Since(start).Round(10*time.Millisecond),
	)
}

func main() {
	fmt.Println("Three cues fetch something slow and declare their work with deck.Do.")
	fmt.Println("A fourth only reads what they gathered, so it uses deck.Complete and")
	fmt.Println("runs inline — no Engine involved, and no cost under a durable engine.")
	fmt.Println()
	fmt.Printf("%-22s %-42s %s\n", "ENGINE", "ORDER COMPLETED", "TOOK")

	// The default. Work runs concurrently, so the whole run takes about as long
	// as the slowest cue, and results arrive shortest-first.
	run("nil (goroutines)", nil)

	// Serial runs each cue inline, one at a time, in the order they were
	// registered. Slower, but the same every single time — which is what you
	// want when a test is asserting on order.
	run("deck.Serial()", deck.Serial())
	run("deck.Serial()", deck.Serial())

	fmt.Println()
	fmt.Println("Same cues both times. Only the Engine changed.")
	fmt.Println("Note assemble is always last: it waits for the other three, and")
	fmt.Println("costs nothing to run because it does no work of its own.")
}
