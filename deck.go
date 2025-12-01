package deck

import (
	"context"
	"fmt"
)

// Deck manages a set of agents (Cues) that operate on a shared state.
type Deck[S any] struct {
	cues []Cue[S]
}

// Cue represents an agent that runs when a condition is met.
type Cue[S any] struct {
	// Name is the unique identifier for the agent.
	Name string
	// When returns true if the agent should run.
	When func(*S) bool
	// Run executes the agent's logic.
	// It returns a mutation function that safely updates the state, or an error.
	Run func(*S) (func(*S), error)
}

// New creates a new Deck with the given cues.
// It returns an error if any cues have duplicate names or empty names.
func New[S any](cues ...Cue[S]) (*Deck[S], error) {
	seen := make(map[string]bool)
	for _, c := range cues {
		if c.Name == "" {
			return nil, fmt.Errorf("cue name cannot be empty")
		}
		if seen[c.Name] {
			return nil, fmt.Errorf("duplicate cue name: %s", c.Name)
		}
		seen[c.Name] = true
	}
	return &Deck[S]{
		cues: cues,
	}, nil
}

// Result contains information about the Deck execution.
type Result struct {
	CompletedCues []string
}

// Run starts the Deck loop. It continues until the context is cancelled.
func (d *Deck[S]) Run(ctx context.Context, state *S) (Result, error) {
	return newRunner(d, ctx, state).run()
}

// runner encapsulates the state of a single Deck execution.
type runner[S any] struct {
	deck        *Deck[S]
	ctx         context.Context
	state       *S
	pending     []Cue[S]
	done        chan cueResult[S]
	activeCount int
	completed   []string
}

type cueResult[S any] struct {
	cue      Cue[S]
	mutation func(*S)
	err      error
}

func newRunner[S any](d *Deck[S], ctx context.Context, state *S) *runner[S] {
	pending := make([]Cue[S], len(d.cues))
	copy(pending, d.cues)

	return &runner[S]{
		deck:    d,
		ctx:     ctx,
		state:   state,
		pending: pending,
		done:    make(chan cueResult[S]),
	}
}

func (r *runner[S]) run() (Result, error) {
	for {
		hits, misses := r.check()
		r.pending = misses
		r.trigger(hits)

		if r.isStable() {
			return Result{CompletedCues: r.completed}, nil
		}

		if err := r.wait(); err != nil {
			return Result{CompletedCues: r.completed}, err
		}
	}
}

func (r *runner[S]) check() ([]Cue[S], []Cue[S]) {
	var triggered []Cue[S]
	nextPending := r.pending[:0]
	for _, c := range r.pending {
		if c.When(r.state) {
			triggered = append(triggered, c)
		} else {
			nextPending = append(nextPending, c)
		}
	}
	return triggered, nextPending
}

func (r *runner[S]) trigger(cues []Cue[S]) {
	for _, c := range cues {
		r.activeCount++
		go func(cue Cue[S]) {
			mutation, err := cue.Run(r.state)
			r.done <- cueResult[S]{cue: cue, mutation: mutation, err: err}
		}(c)
	}
}

func (r *runner[S]) isStable() bool {
	return r.activeCount == 0
}

func (r *runner[S]) wait() error {
	select {
	case <-r.ctx.Done():
		return r.ctx.Err()
	case result := <-r.done:
		r.activeCount--
		if result.mutation != nil {
			result.mutation(r.state)
		}
		r.completed = append(r.completed, result.cue.Name)
		return nil
	}
}
