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

// New creates a new Deck with the given initial state.
func New[S any](state *S) *Deck[S] {
	return &Deck[S]{
		state: state,
	}
}

// AddCue adds a new agent to the Deck.
func (d *Deck[S]) AddCue(c Cue[S]) {
	d.cues = append(d.cues, c)
}

// Run starts the Deck loop. It continues until the context is cancelled.
func (d *Deck[S]) Run(ctx context.Context) error {
	// pending contains cues that are waiting to be checked/run
	pending := make([]Cue[S], len(d.cues))
	copy(pending, d.cues)

	// triggered contains cues that have matched and are ready to run
	var triggered []Cue[S]

	// done receives cues that have completed execution
	done := make(chan Cue[S])

	activeCount := 0

	for {
		// Check Phase: Move matching cues from pending to triggered
		// We iterate backwards so we can remove items from pending easily
		// Note: This is a simple implementation; for order preservation we might want a different approach,
		// but since we re-evaluate all pending, order in pending doesn't strictly matter for correctness
		// of "eventually running".
		// However, to match the previous behavior (check in order), we should iterate forward and rebuild pending.
		nextPending := pending[:0]
		for _, c := range pending {
			if c.When(d.state) {
				triggered = append(triggered, c)
			} else {
				nextPending = append(nextPending, c)
			}
		}
		pending = nextPending

		// Trigger Phase: Launch goroutines for triggered cues
		for _, c := range triggered {
			activeCount++
			go func(cue Cue[S]) {
				// We ignore errors for now as per previous implementation,
				// or we could log them. The signature returns error but we can't easily propagate it
				// without cancelling everything. For now, we just run.
				_ = cue.Run(d.state)
				done <- cue
			}(c)
		}
		triggered = triggered[:0] // Clear triggered

		// Wait Phase
		if activeCount == 0 {
			// If nothing is active and nothing is pending (that matched), we are stable.
			// But we still have cues in pending that didn't match.
			// If we return here, we exit.
			// The requirement is "run until stable".
			// If activeCount is 0, it means no cues are running.
			// Since we just checked all pending cues and moved matches to triggered,
			// if triggered is empty (which it is now), then no progress can be made.
			return nil
		}

		// Wait for something to complete or context cancel
		select {
		case <-ctx.Done():
			return ctx.Err()
		case c := <-done:
			activeCount--
			// Return the completed cue to pending so it can be checked again later
			pending = append(pending, c)
		}
	}
}
