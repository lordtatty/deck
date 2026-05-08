// Package deck orchestrates concurrent, state-driven units of work called
// Cues.
//
// A Deck holds a set of Cues. Each Cue has a When predicate that decides if
// it should run and a Run function that does the work. Calling Run on the
// Deck evaluates all pending cues' When predicates against the current input
// and state; triggered cues run concurrently. As each completes, its mutation
// is applied to state. The cycle repeats until no cues trigger.
//
// # Input vs State
//
// Deck and Cue are generic on two type parameters, I and S:
//
//   - I (input) is read-only data that defines this run. It is passed by
//     value to every When and Run. Cues must not mutate it. Input is NOT
//     included in Export snapshots; on resume, the caller provides fresh
//     input.
//
//   - S (state) is mutable progress. Cues mutate state by returning a
//     Mutation[S] from Run. State IS persisted by Export and restored by
//     Import.
//
// Use I for things that come in (a user message, request parameters, request
// context). Use S for things you build up during the run (results, IDs,
// computed values).
//
// Reference fields inside I (slices, maps, pointers) are passed by reference,
// not deep-copied. Cues must not mutate them either. Prefer value-typed
// fields where practical; if you include a reference type, treat its
// contents as read-only.
//
// Do not put functions, clients, or other behavior in I. Inject those via
// closures over Cue.Run.
//
// # Suspend and resume
//
// A Cue can return Suspended instead of Complete to signal that it has
// kicked off long-running async work. The Deck applies the mutation, drains
// other running cues, and returns with Result.Suspended set. Use Export to
// serialize state, persist it, and later use Import + Run (with fresh input)
// to resume.
package deck

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Deck is an immutable, stateless configuration of cues. Create one with New
// and execute it with Run. The same Deck can be used for many runs.
//
// I is the immutable input type; S is the mutable state type. See the package
// documentation for the full I/S contract.
type Deck[I, S any] struct {
	cues []Cue[I, S]
}

// Cue is a single unit of work. The Deck evaluates each pending cue's When
// predicate against the current input, state, and result history; triggered
// cues run concurrently. Each cue executes at most once per Run call.
type Cue[I, S any] struct {
	// Name uniquely identifies the cue within a Deck. Required and non-empty.
	Name string
	// When decides whether this cue should run, given the current input,
	// state, and result history. All three are passed by copy (read-only).
	// If nil, the cue always triggers on its first evaluation.
	When func(I, S, Result) bool
	// Run performs the cue's work. It receives input and a snapshot of
	// state, both by value, and returns a Mutation describing how to update
	// state — or an error.
	//
	// Use Complete for normal mutations. Use Suspended to apply the
	// mutation and signal the Deck to stop after the current cycle drains —
	// typical for cues that kick off long-running async work.
	Run func(I, S) (Mutation[S], error)
}

// New constructs a Deck from the given cues. Returns an error if any cue has
// an empty name, a nil Run, or a duplicate name.
func New[I, S any](cues ...Cue[I, S]) (*Deck[I, S], error) {
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
	return &Deck[I, S]{
		cues: cues,
	}, nil
}

// Mutation describes how a Cue's Run wants to update state. Construct one
// with Complete (normal completion) or Suspended (apply the mutation and
// stop the Deck).
type Mutation[S any] interface {
	apply(*S)
	isSuspended() bool
}

type regularMutation[S any] struct {
	mutate func(*S)
}

// These methods satisfy the Mutation[S] interface and are dispatched via the
// interface in runner.wait. golangci-lint's `unused` analyzer doesn't trace
// generic interface dispatch, so it flags them — they are not actually unused.
func (m *regularMutation[S]) apply(s *S)        { m.mutate(s) }  //nolint:unused
func (m *regularMutation[S]) isSuspended() bool { return false } //nolint:unused

type suspendedMutation[S any] struct {
	mutate func(*S)
}

func (m *suspendedMutation[S]) apply(s *S)        { m.mutate(s) } //nolint:unused
func (m *suspendedMutation[S]) isSuspended() bool { return true } //nolint:unused

// Complete wraps a state-update function as a Mutation. The cue is recorded
// in Result.CompletedCues and will not fire again in this run.
func Complete[S any](fn func(*S)) Mutation[S] {
	return &regularMutation[S]{mutate: fn}
}

// Suspended wraps a state-update function as a Mutation that also signals
// the Deck to stop after applying it. The cue is NOT recorded as completed,
// so it will re-evaluate on the next Run if its When predicate matches.
//
// Typical use: a cue submits a long-running async job and stores the
// returned ID in state via the mutation. The Deck suspends; the caller
// persists state via Export and resumes later — with fresh input — via
// Import + Run.
func Suspended[S any](fn func(*S)) Mutation[S] {
	return &suspendedMutation[S]{mutate: fn}
}

// CompletedCue records a cue that finished successfully, with the wall-clock
// times its Run started and ended.
type CompletedCue struct {
	Name      string    `json:"name"`
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
}

// Duration is EndTime minus StartTime.
func (c CompletedCue) Duration() time.Duration {
	return c.EndTime.Sub(c.StartTime)
}

// Result describes the outcome of a Run call. CompletedCues lists every cue
// that finished successfully (including cues completed in a prior, resumed
// run). Suspended is true if a cue returned a Suspended mutation.
type Result struct {
	CompletedCues []CompletedCue `json:"completed_cues"`
	Suspended     bool           `json:"suspended"`
}

// Completed reports whether a cue with the given name has finished
// successfully, including any prior resumed runs.
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

// Export serializes state and the run's Result into a portable byte slice.
// Persist the bytes (in Redis, a database, etc.) and pass them to Import
// later to resume.
//
// Input is intentionally NOT included in the snapshot. The caller provides
// fresh input on resume; that is part of the I/S contract documented at the
// package level.
func (d *Deck[I, S]) Export(state *S, result Result) ([]byte, error) {
	snap := snapshot[S]{State: *state, Result: result}
	data, err := json.Marshal(snap)
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot: %w", err)
	}
	return data, nil
}

// Import deserializes a snapshot produced by Export, returning the state and
// the prior Result. Pass these to Run, along with fresh input, to continue
// execution from where the previous run suspended.
func (d *Deck[I, S]) Import(data []byte) (*S, Result, error) {
	var snap snapshot[S]
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, Result{}, fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}
	return &snap.State, snap.Result, nil
}

// Run executes the Deck with the given input and state. It returns when no
// more cues trigger, when a cue returns a Suspended mutation, or when the
// context is cancelled.
//
// Input is passed by value to every When and Run; the library does not
// mutate it and cues must not. State is mutated by cues' returned Mutations
// and updated in-place on successful return.
//
// To resume a previously suspended execution, pass the Result from Import as
// the optional prev argument. The caller provides input fresh on resume —
// it is not in the snapshot.
func (d *Deck[I, S]) Run(ctx context.Context, input I, state *S, prev ...Result) (Result, error) {
	var p Result
	if len(prev) > 0 {
		p = prev[0]
	}
	localState := *state
	runner := newRunner(d, ctx, input, &localState)

	// Pre-populate completed cues from previous result
	if len(p.CompletedCues) > 0 {
		runner.completed = append(runner.completed, p.CompletedCues...)

		// Remove already-completed cues from pending
		alreadyDone := make(map[string]bool, len(p.CompletedCues))
		for _, c := range p.CompletedCues {
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
type runner[I, S any] struct {
	deck        *Deck[I, S]
	ctx         context.Context
	input       I
	state       *S
	pending     []Cue[I, S]
	done        chan cueResult[S]
	activeCount int
	completed   []CompletedCue
	suspended   bool
}

type cueResult[S any] struct {
	cueName   string
	mutation  Mutation[S]
	err       error
	startTime time.Time
	endTime   time.Time
}

func newRunner[I, S any](d *Deck[I, S], ctx context.Context, input I, state *S) *runner[I, S] {
	pending := make([]Cue[I, S], len(d.cues))
	copy(pending, d.cues)

	return &runner[I, S]{
		deck:    d,
		ctx:     ctx,
		input:   input,
		state:   state,
		pending: pending,
		done:    make(chan cueResult[S]),
	}
}

func (r *runner[I, S]) run() (Result, error) {
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

func (r *runner[I, S]) check() ([]Cue[I, S], []Cue[I, S]) {
	var triggered []Cue[I, S]
	nextPending := r.pending[:0]

	// Construct current result for When check
	currentResult := Result{CompletedCues: r.completed}

	for _, c := range r.pending {
		// Pass input and state by value (dereferenced)
		// If When is nil, default to true (always run)
		if c.When == nil || c.When(r.input, *r.state, currentResult) {
			triggered = append(triggered, c)
		} else {
			nextPending = append(nextPending, c)
		}
	}
	return triggered, nextPending
}

func (r *runner[I, S]) trigger(cues []Cue[I, S]) {
	for _, c := range cues {
		r.activeCount++
		// Snapshot input and state for concurrent execution
		currentInput := r.input
		currentState := *r.state
		go func(cue Cue[I, S], input I, state S) {
			startTime := time.Now()
			mutation, err := cue.Run(input, state)
			endTime := time.Now()
			r.done <- cueResult[S]{
				cueName:   cue.Name,
				mutation:  mutation,
				err:       err,
				startTime: startTime,
				endTime:   endTime,
			}
		}(c, currentInput, currentState)
	}
}

func (r *runner[I, S]) isStable() bool {
	return r.activeCount == 0
}

func (r *runner[I, S]) wait() error {
	select {
	case <-r.ctx.Done():
		return fmt.Errorf("deck run cancelled: %w", r.ctx.Err())
	case result := <-r.done:
		r.activeCount--
		if result.mutation != nil {
			result.mutation.apply(r.state)
			if result.mutation.isSuspended() {
				r.suspended = true
			} else {
				r.completed = append(r.completed, CompletedCue{
					Name:      result.cueName,
					StartTime: result.startTime,
					EndTime:   result.endTime,
				})
			}
		}
		return nil
	}
}

// drain waits for all remaining active cues to complete.
func (r *runner[I, S]) drain() error {
	for r.activeCount > 0 {
		if err := r.wait(); err != nil {
			return err
		}
	}
	return nil
}
