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

// Deck is a stateless configuration of cues. Create one with New and execute
// it with Run. The same Deck can be used for many runs.
//
// I is the immutable input type; S is the mutable state type. See the package
// documentation for the full I/S contract.
//
// Now, Spawn and Await are optional hooks for running where ordinary Go
// concurrency is not allowed, such as a durable-execution workflow. Set them
// before calling Run; each run takes its own copy of them as it starts.
type Deck[I, S any] struct {
	cues []Cue[I, S]

	// Now stamps a cue's start and end times. Nil means time.Now. Inject a
	// deterministic clock where wall-clock reads are forbidden.
	//
	// Now is called from inside each cue, so unless Spawn is serial it must
	// be safe for concurrent use. An engine clock that is constant within a
	// step (Temporal's workflow.Now) yields a zero Duration for cues that
	// start and end in that step.
	Now func() time.Time

	// Spawn starts a triggered cue and must run the given function exactly
	// once, now or later. Nil means a goroutine per cue.
	//
	// A serial Spawn — one that calls the function inline — gives
	// deterministic, single-threaded execution. A Spawn that defers to an
	// engine's scheduler (Temporal's workflow.Go) runs cues concurrently and
	// must be paired with Await; where the engine hands the scheduled
	// function its own context, the adapter must pass that on to the cue.
	Spawn func(func())

	// Await yields to the caller's scheduler until cond returns true, and
	// aborts the run if it returns an error. Nil means Run blocks on a plain
	// channel receive. Set it exactly when Spawn defers to an engine's
	// scheduler, where blocking natively would stall every cue the engine
	// has yet to run. cond is side-effect free and may be called any number
	// of times.
	Await func(cond func() bool) error
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
	//
	// If Run returns a non-nil error, the Deck stops, drains other active
	// cues, and surfaces the error from Deck.Run wrapped with the cue's name.
	// The returned mutation is discarded and the cue is not recorded in
	// Result.CompletedCues. If multiple concurrent cues error, the first
	// observed error wins.
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
// interface in runner.absorb. golangci-lint's `unused` analyzer doesn't trace
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

// CompletedCue records a cue that finished successfully and when its Run
// started and ended, as read from Deck.Now.
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
// more cues trigger, when a cue returns a Suspended mutation, when a cue
// returns an error, or when the context is cancelled.
//
// Cancellation is checked between cycles in every execution mode; only the
// default mode can additionally abandon cues still in flight.
//
// Input is passed by value to every When and Run; the library does not
// mutate it and cues must not. State is mutated by cues' returned Mutations
// and updated in-place on successful return. On error (cue error or context
// cancellation), the caller's *state is not overwritten with the run's
// working copy. Note: reference-typed fields in S (slices, maps, pointers)
// share storage with the working copy, so mutations cues made through those
// references remain visible regardless of error status.
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
	ctx         context.Context
	input       I
	state       *S
	pending     []Cue[I, S]
	done        chan cueResult[S]
	activeCount int
	completed   []CompletedCue
	suspended   bool
	runErr      error // first error returned by any cue's Run (subsequent errors are dropped)

	// Hooks resolved at Run start, so a cue that outlives its run never
	// reads the Deck's fields.
	clock func() time.Time
	spawn func(func())
	await func(func() bool) error
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

	clock := d.Now
	if clock == nil {
		clock = time.Now
	}
	spawn := d.Spawn
	if spawn == nil {
		spawn = func(fn func()) { go fn() }
	}

	// Buffered so a serial Spawn can deliver inline before any receiver runs.
	// Each cue runs at most once per Run, so len(cues) is always enough.
	return &runner[I, S]{
		ctx:     ctx,
		input:   input,
		state:   state,
		pending: pending,
		done:    make(chan cueResult[S], len(d.cues)),
		clock:   clock,
		spawn:   spawn,
		await:   d.Await,
	}
}

func (r *runner[I, S]) run() (Result, error) {
	for {
		// A non-blocking read, so cancellation is honoured between cycles in
		// every execution mode.
		if err := r.ctx.Err(); err != nil {
			return Result{CompletedCues: r.completed, Suspended: r.suspended}, fmt.Errorf("deck run cancelled: %w", err)
		}

		hits, misses := r.check()
		r.pending = misses
		r.trigger(hits)

		if r.isStable() {
			return Result{CompletedCues: r.completed, Suspended: r.suspended}, r.runErr
		}

		if err := r.wait(); err != nil {
			return Result{CompletedCues: r.completed, Suspended: r.suspended}, err
		}

		// Either condition stops the run: drain remaining active cues so
		// their goroutines can exit cleanly, then decide what to return.
		// Error takes precedence over suspended — the drain itself may have
		// captured an error from a sibling cue, so we re-check r.runErr
		// AFTER drain rather than only before.
		if r.suspended || r.runErr != nil {
			drainErr := r.drain()
			switch {
			case r.runErr != nil && drainErr != nil:
				return Result{CompletedCues: r.completed}, fmt.Errorf("%w; drain aborted: %w", r.runErr, drainErr)
			case r.runErr != nil:
				return Result{CompletedCues: r.completed}, r.runErr
			case drainErr != nil:
				return Result{CompletedCues: r.completed, Suspended: r.suspended}, drainErr
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
		cue, input, state := c, r.input, *r.state
		r.spawn(func() {
			startTime := r.clock()
			mutation, err := cue.Run(input, state)
			endTime := r.clock()
			r.done <- cueResult[S]{
				cueName:   cue.Name,
				mutation:  mutation,
				err:       err,
				startTime: startTime,
				endTime:   endTime,
			}
		})
	}
}

func (r *runner[I, S]) isStable() bool {
	return r.activeCount == 0
}

// wait absorbs one cue result, or returns an error if the run was cancelled
// or stalled first. Of its three paths only the final select blocks, so a
// Deck with a serial Spawn or an Await never performs a blocking operation.
func (r *runner[I, S]) wait() error {
	// Take a buffered result first. Under a serial Spawn every result is
	// already buffered, so that mode never reaches the ctx branch; run checks
	// ctx between cycles instead.
	select {
	case result := <-r.done:
		return r.absorb(result)
	default:
	}

	// Yield to the engine's scheduler rather than block. The receive after it
	// is non-blocking too: if the scheduler returned with nothing ready, no
	// context could rescue a blocked receive.
	if r.await != nil {
		if err := r.await(func() bool { return len(r.done) > 0 }); err != nil {
			return fmt.Errorf("deck run aborted by Await: %w", err)
		}
		select {
		case result := <-r.done:
			return r.absorb(result)
		default:
			return fmt.Errorf("deck run stalled: Await returned before a cue result was ready")
		}
	}

	select {
	case <-r.ctx.Done():
		return fmt.Errorf("deck run cancelled: %w", r.ctx.Err())
	case result := <-r.done:
		return r.absorb(result)
	}
}

// absorb applies one finished cue's outcome. A cue's error takes precedence
// over its mutation: if Run returned an error, the cue is treated as failed —
// its mutation is discarded and it is not recorded in CompletedCues.
func (r *runner[I, S]) absorb(result cueResult[S]) error {
	r.activeCount--
	if result.err != nil {
		if r.runErr == nil {
			r.runErr = fmt.Errorf("cue %s: %w", result.cueName, result.err)
		}
		return nil
	}
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

// drain waits for all remaining active cues to complete.
func (r *runner[I, S]) drain() error {
	for r.activeCount > 0 {
		if err := r.wait(); err != nil {
			return err
		}
	}
	return nil
}
