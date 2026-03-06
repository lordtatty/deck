package deck

import (
	"context"
	"encoding/json"
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
	// It returns a Mutation that updates the state, or an error.
	// Use Complete() for normal mutations, or Suspended() to signal
	// that the Deck should suspend after applying the mutation.
	Run func(S) (Mutation[S], error)
}

// New creates a new Deck with the given cues.
// It returns an error if any cues have duplicate names or empty names.
func New[S any](cues ...Cue[S]) (*Deck[S], error) {
	seen := make(map[string]bool)
	for _, c := range cues {
		if c.Name == "" {
			return nil, fmt.Errorf("cue name cannot be empty")
		}
		if c.Run == nil {
			return nil, fmt.Errorf("cue run cannot be nil: %s", c.Name)
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

// Mutation represents the result of a Cue's Run function.
// It can be either a regular mutation (func(*S)) or a suspended mutation
// created via Suspended().
type Mutation[S any] interface {
	apply(*S)
	isSuspended() bool
}

type regularMutation[S any] struct {
	mutate func(*S)
}

func (m *regularMutation[S]) apply(s *S) { m.mutate(s) }
func (m *regularMutation[S]) isSuspended() bool { return false }

type suspendedMutation[S any] struct {
	mutate func(*S)
}

func (m *suspendedMutation[S]) apply(s *S) { m.mutate(s) }
func (m *suspendedMutation[S]) isSuspended() bool { return true }

// Complete wraps a plain mutation function as a Mutation, indicating
// that the cue has completed successfully.
func Complete[S any](fn func(*S)) Mutation[S] {
	return &regularMutation[S]{mutate: fn}
}

// Suspended wraps a mutation function to indicate that the Deck should
// suspend after applying this mutation. The mutation is applied to state,
// but the cue is not marked as completed, allowing it to fire again on
// the next Run.
func Suspended[S any](fn func(*S)) Mutation[S] {
	return &suspendedMutation[S]{mutate: fn}
}

// CompletedCue contains information about a successfully executed cue.
type CompletedCue struct {
	Name      string    `json:"name"`
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
}

// Duration returns the time taken for the cue to execute.
func (c CompletedCue) Duration() time.Duration {
	return c.EndTime.Sub(c.StartTime)
}

// Result contains information about the Deck execution.
type Result struct {
	// CompletedCues is a list of cues that executed successfully.
	CompletedCues []CompletedCue `json:"completed_cues"`
	// Suspended is true if the Deck was suspended by a cue.
	Suspended bool `json:"suspended"`
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

// snapshot bundles state and result for serialization between suspend/resume cycles.
type snapshot[S any] struct {
	State  S      `json:"state"`
	Result Result `json:"result"`
}

// Export serializes the current state and result into a portable byte slice
// that can be stored externally and later passed to Import.
func (d *Deck[S]) Export(state *S, result Result) ([]byte, error) {
	snap := snapshot[S]{State: *state, Result: result}
	return json.Marshal(snap)
}

// Import deserializes a previously exported snapshot, returning the state
// and previous result. Use the returned values with Resume to continue
// execution.
func (d *Deck[S]) Import(data []byte) (*S, Result, error) {
	var snap snapshot[S]
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, Result{}, fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}
	return &snap.State, snap.Result, nil
}

// Run starts the Deck loop. It continues until the context is cancelled.
func (d *Deck[S]) Run(ctx context.Context, state *S) (Result, error) {
	return d.Resume(ctx, state, Result{})
}

// Resume continues a previously suspended Deck execution. It skips cues
// that were already completed in the previous result and pre-populates
// the execution history so that When predicates can inspect it.
func (d *Deck[S]) Resume(ctx context.Context, state *S, prev Result) (Result, error) {
	localState := *state
	runner := newRunner(d, ctx, &localState)

	// Pre-populate completed cues from previous result
	if len(prev.CompletedCues) > 0 {
		runner.completed = append(runner.completed, prev.CompletedCues...)

		// Remove already-completed cues from pending
		alreadyDone := make(map[string]bool, len(prev.CompletedCues))
		for _, c := range prev.CompletedCues {
			alreadyDone[c.Name] = true
		}
		filtered := runner.pending[:0]
		for _, c := range runner.pending {
			if !alreadyDone[c.Name] {
				filtered = append(filtered, c)
			}
		}
		runner.pending = filtered
	}

	result, err := runner.run()
	if err == nil {
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
	suspended   bool
}

type cueResult[S any] struct {
	cue       Cue[S]
	mutation  Mutation[S]
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
			return Result{CompletedCues: r.completed, Suspended: r.suspended}, nil
		}

		if err := r.wait(); err != nil {
			return Result{CompletedCues: r.completed, Suspended: r.suspended}, err
		}

		if r.suspended {
			if err := r.drain(); err != nil {
				return Result{CompletedCues: r.completed, Suspended: r.suspended}, err
			}
			return Result{CompletedCues: r.completed, Suspended: true}, nil
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
			result.mutation.apply(r.state)
			if result.mutation.isSuspended() {
				r.suspended = true
			} else {
				r.completed = append(r.completed, CompletedCue{
					Name:      result.cue.Name,
					StartTime: result.startTime,
					EndTime:   result.endTime,
				})
			}
		}
		return nil
	}
}

// drain waits for all remaining active cues to complete.
func (r *runner[S]) drain() error {
	for r.activeCount > 0 {
		if err := r.wait(); err != nil {
			return err
		}
	}
	return nil
}
