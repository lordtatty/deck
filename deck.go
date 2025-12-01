package deck

import (
	"context"
)

// Deck manages a set of agents (Cues) that operate on a shared state.
type Deck[S any] struct {
	state *S
	cues  []Cue[S]
}

// Cue represents an agent that runs when a condition is met.
type Cue[S any] struct {
	// When returns true if the agent should run.
	When func(*S) bool
	// Run executes the agent's logic.
	Run func(*S) error
}

// New creates a new Deck with the given initial state and cues.
func New[S any](state *S, cues ...Cue[S]) *Deck[S] {
	return &Deck[S]{
		state: state,
		cues:  cues,
	}
}

// Run starts the Deck loop. It continues until the context is cancelled.
func (d *Deck[S]) Run(ctx context.Context) error {
	return newRunner(d, ctx).run()
}

// runner encapsulates the state of a single Deck execution.
type runner[S any] struct {
	deck        *Deck[S]
	ctx         context.Context
	pending     []Cue[S]
	done        chan Cue[S]
	activeCount int
}

func newRunner[S any](d *Deck[S], ctx context.Context) *runner[S] {
	pending := make([]Cue[S], len(d.cues))
	copy(pending, d.cues)

	return &runner[S]{
		deck:    d,
		ctx:     ctx,
		pending: pending,
		done:    make(chan Cue[S]),
	}
}

func (r *runner[S]) run() error {
	for {
		hits, misses := r.check()
		r.pending = misses
		r.trigger(hits)

		if r.isStable() {
			return nil
		}

		if err := r.wait(); err != nil {
			return err
		}
	}
}

func (r *runner[S]) check() ([]Cue[S], []Cue[S]) {
	var triggered []Cue[S]
	nextPending := r.pending[:0]
	for _, c := range r.pending {
		if c.When(r.deck.state) {
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
			_ = cue.Run(r.deck.state)
			r.done <- cue
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
	case <-r.done:
		r.activeCount--
		return nil
	}
}
