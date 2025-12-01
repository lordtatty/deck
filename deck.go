package deck

import (
	"context"
	"fmt"
	"time"
)

// Deck manages a set of agents (Cues) that operate on a shared state.
type Deck[S any] struct {
	cues []Cue[S]
}

// Cue represents a single unit of work in the Deck.
type Cue[S any] struct {
	// Name is a unique identifier for the cue.
	Name string
	// When determines if the cue should run based on the current state and execution history.
	When func(S, Result) bool
	// Run performs the work associated with the cue.
	// It returns a mutation function that updates the state, or an error.
	Run func(S) (func(*S), error)
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

// CompletedCue contains information about a successfully executed cue.
type CompletedCue struct {
	Name      string
	StartTime time.Time
	EndTime   time.Time
}

// Duration returns the time taken for the cue to execute.
func (c CompletedCue) Duration() time.Duration {
	return c.EndTime.Sub(c.StartTime)
}

// Result contains information about the Deck execution.
type Result struct {
	// CompletedCues is a list of cues that executed successfully.
	CompletedCues []CompletedCue
}

// Completed returns true if a cue with the given name has successfully executed.
func (r Result) Completed(name string) bool {
	for _, c := range r.CompletedCues {
		if c.Name == name {
			return true
		}
	}
	return false
}

// Run starts the Deck loop. It continues until the context is cancelled.
func (d *Deck[S]) Run(ctx context.Context, state *S) (Result, error) {
	// Create a local copy of the state to isolate execution
	localState := *state
	runner := newRunner(d, ctx, &localState)
	result, err := runner.run()
	if err == nil {
		// Copy the final state back to the caller's pointer
		*state = localState
	}
	return result, err
}

// runner encapsulates the state of a single Deck execution.
type runner[S any] struct {
	deck        *Deck[S]
	ctx         context.Context
	state       *S
	pending     []Cue[S]
	done        chan cueResult[S]
	activeCount int
	completed   []CompletedCue
}

type cueResult[S any] struct {
	cue       Cue[S]
	mutation  func(*S)
	err       error
	startTime time.Time
	endTime   time.Time
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

	// Construct current result for When check
	currentResult := Result{CompletedCues: r.completed}

	for _, c := range r.pending {
		// Pass state by value (dereferenced)
		// If When is nil, default to true (always run)
		if c.When == nil || c.When(*r.state, currentResult) {
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
		// Snapshot state for concurrent execution
		currentState := *r.state
		go func(cue Cue[S], state S) {
			startTime := time.Now()
			mutation, err := cue.Run(state)
			endTime := time.Now()
			r.done <- cueResult[S]{
				cue:       cue,
				mutation:  mutation,
				err:       err,
				startTime: startTime,
				endTime:   endTime,
			}
		}(c, currentState)
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
		r.completed = append(r.completed, CompletedCue{
			Name:      result.cue.Name,
			StartTime: result.startTime,
			EndTime:   result.endTime,
		})
		return nil
	}
}
