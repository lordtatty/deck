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
// # Where cues run
//
// A Deck's Engine decides where its cues run. Leave Deck.Engine nil and you
// get Goroutines: one goroutine per cue and the wall clock. Set it to Serial
// to run cues one at a time in registration order, which makes a run
// reproducible, or to an engine of your own — see the deck/temporal module,
// which runs a Deck inside a Temporal workflow.
//
// # Declaring work
//
// A Cue's Run may do its work directly and return Complete. Where the work
// leaves the process, is slow, or is not deterministic, it should instead
// describe the work with Do and let the Engine perform it:
//
//	Run: func(in Input, s State) (deck.Mutation[State], error) {
//		return deck.Do(FetchUser, in.UserID, func(u User) deck.Mutation[State] {
//			return deck.Complete(func(s *State) { s.User = u })
//		}), nil
//	}
//
// Do returns straight away, so the Deck carries on triggering other cues while
// the work runs. A cue written this way does not know where its work happens,
// which is what lets the same cues run on goroutines in one process and as
// durable activities in another.
//
// Most flows mix the two: cues that fetch or call something use Do, and a cue
// that merely assembles what they gathered uses Complete.
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
	"errors"
	"fmt"
	"runtime/debug"
	"time"
)

// Deck is a stateless configuration of cues. Create one with New and execute
// it with Run. The same Deck can be used for many runs.
//
// I is the immutable input type; S is the mutable state type. See the package
// documentation for the full I/S contract.
//
// Engine decides where cues run. Leave it nil for goroutines and the wall
// clock; set it to run somewhere ordinary Go concurrency is not allowed.
type Deck[I, S any] struct {
	cues []Cue[I, S]

	// Engine is where this Deck's cues run. Nil means Goroutines.
	//
	// Assign it only on a Deck you own outright. Where one Deck is shared
	// between runs that need different engines — a package-level Deck used by
	// every workflow on a worker, say — use WithEngine instead: assigning here
	// would race with every other run in flight.
	Engine Engine
}

// WithEngine returns a copy of d that runs on e, leaving d untouched. The two
// share their cues, which are read-only once New has returned.
//
// It exists so that a Deck built once can serve many runs at once, each with
// its own engine:
//
//	var flow, _ = deck.New(cues...)
//
//	func MyWorkflow(ctx workflow.Context) error {
//		d := flow.WithEngine(temporal.New(ctx))
//		...
//	}
//
// Assigning to flow.Engine there would look equivalent and would race with
// every other workflow the worker is running.
func (d *Deck[I, S]) WithEngine(e Engine) *Deck[I, S] {
	c := *d
	c.Engine = e
	return &c
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
	//
	// A panic is treated the same way, and reported with the panic value and a
	// stack trace. Deck recovers it because an Engine may be running Run on a
	// goroutine of its own, where a panic would end the process and the caller
	// would have no way to intervene. Work declared with Do, and the handler
	// given to it, are covered too.
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

// Do declares work for the Deck's Engine to perform, rather than performing it
// in the cue. fn is an ordinary Go function; then turns its result into a state
// update once the work has finished.
//
// Use Do where the work leaves the process, is slow, or is not deterministic.
// A cue that only computes from state it already holds should return Complete
// instead: it will run inline, and under a durable engine it costs nothing,
// where a Do would cost a scheduled unit of work and an entry in the run's
// history.
//
// Under the default Engine, fn is simply called on its own goroutine. Under a
// durable engine it may run elsewhere — as a Temporal activity, say — so fn and
// arg must carry everything the work needs, and the result must survive being
// serialised. The cue is not recorded as completed until then has run.
func Do[S, A, R any](
	fn func(context.Context, A) (R, error),
	arg A,
	then func(R) Mutation[S],
) Mutation[S] {
	var result R
	return &workMutation[S]{
		work: Work{
			Func:   fn,
			Arg:    arg,
			Result: &result,
			Local: func(ctx context.Context) (err error) {
				// As in trigger: fn is user code, and an Engine may be running
				// it on a goroutine of its own.
				defer func() {
					if p := recover(); p != nil {
						err = panicError(p)
					}
				}()
				out, callErr := fn(ctx, arg)
				if callErr != nil {
					return callErr
				}
				result = out
				return nil
			},
		},
		collect: func() Mutation[S] { return then(result) },
	}
}

// workMutation is what Do returns. It is not a state change: the runner
// recognises it, has the Engine perform the work, and applies whatever collect
// produces once the result arrives.
type workMutation[S any] struct {
	work    Work
	collect func() Mutation[S]
}

// Never reached: the runner intercepts workMutation before applying anything.
func (m *workMutation[S]) apply(*S)          {}               //nolint:unused
func (m *workMutation[S]) isSuspended() bool { return false } //nolint:unused

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
// Cancellation is checked between cycles, and again whenever the Engine
// yields. Only the default Engine can additionally abandon cues in flight.
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
	ctx       context.Context
	engine    Engine
	input     I
	state     *S
	pending   []Cue[I, S]
	inflight  []*attempt[S]
	completed []CompletedCue
	suspended bool
	runErr    error // first error returned by any cue's Run (subsequent errors are dropped)
}

// attempt is one cue execution in flight. The Engine's Future says when the
// other fields may be read.
type attempt[S any] struct {
	name       string
	future     Future
	start, end time.Time
	mutation   Mutation[S]
	err        error

	// collect is set on the second phase of a Do cue: the cue's Run has
	// returned and its declared work is now in flight.
	collect func() Mutation[S]
}

func newRunner[I, S any](d *Deck[I, S], ctx context.Context, input I, state *S) *runner[I, S] {
	pending := make([]Cue[I, S], len(d.cues))
	copy(pending, d.cues)

	engine := d.Engine
	if engine == nil {
		engine = Goroutines()
	}

	return &runner[I, S]{
		ctx:     ctx,
		engine:  engine,
		input:   input,
		state:   state,
		pending: pending,
	}
}

func (r *runner[I, S]) run() (Result, error) {
	for {
		// A non-blocking read, so cancellation is honoured between cycles
		// whatever the Engine does.
		if err := r.ctx.Err(); err != nil {
			return r.result(), fmt.Errorf("deck run cancelled: %w", err)
		}

		hits, misses := r.check()
		r.pending = misses
		r.trigger(hits)

		if r.isStable() {
			return r.result(), r.runErr
		}

		if err := r.wait(); err != nil {
			return r.result(), err
		}

		// Either condition stops the run: drain the cues still in flight so
		// they finish cleanly, then decide what to return. Error takes
		// precedence over suspended — the drain itself may have captured an
		// error from a sibling cue, so we re-check r.runErr AFTER draining.
		if r.suspended || r.runErr != nil {
			drainErr := r.drain()
			switch {
			case r.runErr != nil && drainErr != nil:
				return Result{CompletedCues: r.completed}, fmt.Errorf("%w; drain aborted: %w", r.runErr, drainErr)
			case r.runErr != nil:
				return Result{CompletedCues: r.completed}, r.runErr
			case drainErr != nil:
				return r.result(), drainErr
			}
			return Result{CompletedCues: r.completed, Suspended: true}, nil
		}
	}
}

func (r *runner[I, S]) result() Result {
	return Result{CompletedCues: r.completed, Suspended: r.suspended}
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
		// Snapshot input and state for concurrent execution
		cue, input, state := c, r.input, *r.state
		a := &attempt[S]{name: cue.Name, start: r.engine.Now()}
		a.future = r.engine.Spawn(func() {
			// A cue is user code and may panic. Under the default Engine it
			// runs on a goroutine deck created, where a panic would kill the
			// process and the caller could do nothing about it. Turning it into
			// this cue's error means every Engine behaves the same way.
			defer func() {
				a.end = r.engine.Now()
				if p := recover(); p != nil {
					a.mutation, a.err = nil, panicError(p)
				}
			}()
			a.mutation, a.err = cue.Run(input, state)
		})
		r.inflight = append(r.inflight, a)
	}
}

func (r *runner[I, S]) isStable() bool {
	return len(r.inflight) == 0
}

// wait yields to the Engine until a cue has finished, then absorbs exactly one.
func (r *runner[I, S]) wait() error {
	if err := r.engine.Await(r.ctx, r.futures()); err != nil {
		return fmt.Errorf("deck run aborted: %w", err)
	}
	for i, a := range r.inflight {
		if a.future.IsReady() {
			r.inflight = append(r.inflight[:i], r.inflight[i+1:]...)
			r.absorb(a)
			return nil
		}
	}
	return errors.New("deck run stalled: the engine returned before any cue had finished")
}

// futures lists what the run is currently waiting on, for the Engine to wait
// on in whatever way suits it.
func (r *runner[I, S]) futures() []Future {
	fs := make([]Future, 0, len(r.inflight))
	for _, a := range r.inflight {
		fs = append(fs, a.future)
	}
	return fs
}

// absorb applies one finished cue's outcome. A cue's error takes precedence
// over its mutation: if Run returned an error, the cue is treated as failed —
// its mutation is discarded and it is not recorded in CompletedCues.
func (r *runner[I, S]) absorb(a *attempt[S]) {
	if a.collect != nil {
		// The work this cue declared has finished.
		a.end = r.engine.Now()
		if err := a.future.Get(); err != nil {
			a.err = err
		} else {
			a.mutation, a.err = r.collectResult(a)
		}
	}
	if a.err != nil {
		if r.runErr == nil {
			r.runErr = fmt.Errorf("cue %s: %w", a.name, a.err)
		}
		return
	}
	if a.mutation == nil {
		return
	}
	if w, ok := a.mutation.(*workMutation[S]); ok {
		// The cue declared work rather than a state change: start it and keep
		// the cue in flight until its result arrives.
		work := w.work
		work.CueName = a.name
		r.inflight = append(r.inflight, &attempt[S]{
			name:    a.name,
			start:   a.start,
			future:  r.engine.Execute(r.ctx, work),
			collect: w.collect,
		})
		return
	}
	a.mutation.apply(r.state)
	if a.mutation.isSuspended() {
		r.suspended = true
		return
	}
	r.completed = append(r.completed, CompletedCue{
		Name:      a.name,
		StartTime: a.start,
		EndTime:   a.end,
	})
}

// collectResult runs the handler a cue gave to Do. It is user code running on
// the Deck's own goroutine, so a panic here would surface from Run rather than
// from the cue that caused it.
func (r *runner[I, S]) collectResult(a *attempt[S]) (m Mutation[S], err error) {
	defer func() {
		if p := recover(); p != nil {
			m, err = nil, panicError(p)
		}
	}()
	return a.collect(), nil
}

// panicError turns a recovered panic into an error. The stack is included
// because a panic reported without one is harder to chase than the crash it
// replaced.
func panicError(p any) error {
	return fmt.Errorf("panic: %v\n\n%s", p, debug.Stack())
}

// drain waits for every cue still in flight to finish.
func (r *runner[I, S]) drain() error {
	for len(r.inflight) > 0 {
		if err := r.wait(); err != nil {
			return err
		}
	}
	return nil
}
