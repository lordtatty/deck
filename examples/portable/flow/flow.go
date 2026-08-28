// Package flow is the interesting half of this example: a deck flow that does
// not know, or care, where it runs.
//
// Look at the imports. There is no Temporal here, and no branching on which
// environment we are in. The same cues run on goroutines in a CLI and as
// activities inside a durable workflow.
package flow

import (
	"context"
	"fmt"
	"time"

	"github.com/lordtatty/deck"
)

type Input struct {
	CustomerID string `json:"customer_id"`
}

type State struct {
	Profile string   `json:"profile"`
	Orders  []string `json:"orders"`
	Report  string   `json:"report"`
}

// FetchProfile and FetchOrders are ordinary Go functions taking a context and
// one argument. That is also exactly the shape of a Temporal activity, which is
// why the same function serves both worlds with no adapter in between.
func FetchProfile(ctx context.Context, customerID string) (string, error) {
	if err := pause(ctx, 400*time.Millisecond); err != nil {
		return "", err
	}
	return "profile(" + customerID + ")", nil
}

func FetchOrders(ctx context.Context, customerID string) ([]string, error) {
	if err := pause(ctx, 400*time.Millisecond); err != nil {
		return nil, err
	}
	return []string{"order-1", "order-2"}, nil
}

func pause(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return fmt.Errorf("cancelled: %w", ctx.Err())
	}
}

// Cues returns the flow. Profile and orders have nothing to do with each other,
// so they go out together; the report waits for both.
//
// Note that only two of the three cues declare work. Do is for work that leaves
// the process — a lookup, a call, anything slow. The report cue only reads state
// the others filled in, so it uses Complete and runs inline.
func Cues() []deck.Cue[Input, State] {
	return []deck.Cue[Input, State]{
		{
			Name: "profile",
			// No When, so it triggers immediately.
			Run: func(in Input, _ State) (deck.Mutation[State], error) {
				// Do declares the work instead of performing it. It returns
				// straight away, so the Deck can trigger "orders" while this is
				// still running. That is where the parallelism comes from.
				return deck.Do(FetchProfile, in.CustomerID, func(p string) deck.Mutation[State] {
					return deck.Complete(func(s *State) { s.Profile = p })
				}), nil
			},
		},
		{
			Name: "orders",
			Run: func(in Input, _ State) (deck.Mutation[State], error) {
				return deck.Do(FetchOrders, in.CustomerID, func(o []string) deck.Mutation[State] {
					return deck.Complete(func(s *State) { s.Orders = o })
				}), nil
			},
		},
		{
			Name: "report",
			// Waits for both. Result carries what has completed so far.
			When: func(_ Input, _ State, r deck.Result) bool {
				return r.Completed("profile") && r.Completed("orders")
			},
			// No Do here: this is a string built from state we already have.
			// Wrapping it in Do would make it a Temporal activity, costing a
			// round trip and a history entry to do a Sprintf.
			Run: func(_ Input, s State) (deck.Mutation[State], error) {
				report := fmt.Sprintf("%s has %d orders", s.Profile, len(s.Orders))
				return deck.Complete(func(s *State) { s.Report = report }), nil
			},
		},
	}
}
