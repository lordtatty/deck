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
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			// Check if any cue needs to run
			anyRun := false
			for _, c := range d.cues {
				if c.When(d.state) {
					if err := c.Run(d.state); err != nil {
						return err
					}
					anyRun = true
				}
			}

			// If no cue ran, we are done for this pass
			if !anyRun {
				return nil
			}
		}
	}
}
