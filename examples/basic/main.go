package main

import (
	"context"
	"fmt"
	"log"

	"github.com/lordtatty/deck"
)

// A simple pipeline: Prepare data, then Process it, then Summarise.
// Each cue triggers the next via state changes.

type PipelineState struct {
	Items     []string
	Processed []string
	Summary   string
}

func main() {
	cues := []deck.Cue[PipelineState]{
		{
			Name: "Prepare",
			When: func(s PipelineState, r deck.Result) bool {
				return len(s.Items) == 0
			},
			Run: func(s PipelineState) (deck.Mutation[PipelineState], error) {
				fmt.Println("[Prepare] Fetching items...")
				return deck.Complete(func(s *PipelineState) {
					s.Items = []string{"alpha", "bravo", "charlie"}
				}), nil
			},
		},
		{
			Name: "Process",
			When: func(s PipelineState, r deck.Result) bool {
				return len(s.Items) > 0 && len(s.Processed) == 0
			},
			Run: func(s PipelineState) (deck.Mutation[PipelineState], error) {
				fmt.Println("[Process] Processing items...")
				var results []string
				for _, item := range s.Items {
					results = append(results, fmt.Sprintf("%s_done", item))
				}
				return deck.Complete(func(s *PipelineState) {
					s.Processed = results
				}), nil
			},
		},
		{
			Name: "Summarise",
			When: func(s PipelineState, r deck.Result) bool {
				return len(s.Processed) > 0 && s.Summary == ""
			},
			Run: func(s PipelineState) (deck.Mutation[PipelineState], error) {
				fmt.Println("[Summarise] Creating summary...")
				return deck.Complete(func(s *PipelineState) {
					s.Summary = fmt.Sprintf("Processed %d items", len(s.Processed))
				}), nil
			},
		},
	}

	d, err := deck.New(cues...)
	if err != nil {
		log.Fatal(err)
	}

	state := &PipelineState{}
	result, err := d.Run(context.Background(), state)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println()
	fmt.Println("=== Result ===")
	fmt.Printf("Items:     %v\n", state.Items)
	fmt.Printf("Processed: %v\n", state.Processed)
	fmt.Printf("Summary:   %s\n", state.Summary)
	fmt.Printf("Completed: %d cues\n", len(result.CompletedCues))
	for _, c := range result.CompletedCues {
		fmt.Printf("  - %s (%s)\n", c.Name, c.Duration())
	}
}
