// Package flow is a deck flow that does not know where it runs. It imports
// deck and nothing else — no Temporal, no environment branching — and the same
// cues drive both the local and the workflow test.
package flow

import (
	"context"
	"fmt"
	"time"

	"github.com/lordtatty/deck"
)

// WorkDuration is how long each unit of work takes. Two units running at once
// finish in roughly this; one after the other takes twice it.
const WorkDuration = 200 * time.Millisecond

// In is the run's input.
type In struct {
	UserID string `json:"user_id"`
}

// Timed is a result plus when the work actually ran, so a test can see whether
// two units of work overlapped.
type Timed struct {
	Value string    `json:"value"`
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// State is what the flow builds up.
type State struct {
	User   Timed  `json:"user"`
	Orders Timed  `json:"orders"`
	Report string `json:"report"`
}

// FetchUser and FetchOrders are ordinary Go functions. Locally they are called
// directly; under Temporal they are registered as activities. Same function.
func FetchUser(ctx context.Context, userID string) (Timed, error) {
	return work(ctx, "user:"+userID)
}

func FetchOrders(ctx context.Context, userID string) (Timed, error) {
	return work(ctx, "orders:"+userID)
}

func work(ctx context.Context, value string) (Timed, error) {
	start := time.Now()
	select {
	case <-time.After(WorkDuration):
	case <-ctx.Done():
		return Timed{}, fmt.Errorf("work cancelled: %w", ctx.Err())
	}
	return Timed{Value: value, Start: start, End: time.Now()}, nil
}

// Cues returns the flow: two independent fetches, and a report that waits for
// both. The report cue does no work of its own — it only reads state.
func Cues() []deck.Cue[In, State] {
	return []deck.Cue[In, State]{
		{
			Name: "user",
			Run: func(in In, _ State) (deck.Mutation[State], error) {
				return deck.Do(FetchUser, in.UserID, func(t Timed) deck.Mutation[State] {
					return deck.Complete(func(s *State) { s.User = t })
				}), nil
			},
		},
		{
			Name: "orders",
			Run: func(in In, _ State) (deck.Mutation[State], error) {
				return deck.Do(FetchOrders, in.UserID, func(t Timed) deck.Mutation[State] {
					return deck.Complete(func(s *State) { s.Orders = t })
				}), nil
			},
		},
		{
			Name: "report",
			When: func(_ In, _ State, r deck.Result) bool {
				return r.Completed("user") && r.Completed("orders")
			},
			Run: func(_ In, s State) (deck.Mutation[State], error) {
				report := fmt.Sprintf("%s + %s", s.User.Value, s.Orders.Value)
				return deck.Complete(func(s *State) { s.Report = report }), nil
			},
		},
	}
}

// Overlapped reports whether the two independent fetches ran at the same time.
func Overlapped(s State) bool {
	return s.User.Start.Before(s.Orders.End) && s.Orders.Start.Before(s.User.End)
}
