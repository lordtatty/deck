package deck

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Engine is where a Deck's cues run. It supplies the clock, starts work, and
// yields while work is in flight. A Deck with no Engine uses Goroutines.
//
// Implement it to run a Deck somewhere ordinary Go concurrency is not allowed:
// inside a durable-execution engine's workflow, or in a test that needs a fixed
// execution order.
type Engine interface {
	// Now stamps cue start and end times.
	Now() time.Time

	// Spawn runs fn exactly once, now or later, and returns a handle reporting
	// when it has finished. Spawn itself must not block. fn is the Deck's own
	// code, so it always runs in this process.
	Spawn(fn func()) Future

	// Execute performs work a cue declared with Do, and returns a handle
	// reporting when it has finished. Execute itself must not block. Unlike
	// Spawn, the work may run elsewhere entirely.
	Execute(ctx context.Context, w Work) Future

	// Await yields until at least one of fs is ready. Returning an error stops
	// the run: a cancelled context, or the engine's own cancellation.
	//
	// It is given the handles rather than a condition to evaluate, because
	// waiting on a named set of outstanding work is the primitive engines
	// tend to offer. Use AnyReady if it is easier to poll.
	Await(ctx context.Context, fs []Future) error
}

// AnyReady reports whether any of fs has finished. Engines implementing
// Await on top of a polling or condition-based primitive will want it.
func AnyReady(fs []Future) bool {
	for _, f := range fs {
		if f.IsReady() {
			return true
		}
	}
	return false
}

// Work is a unit of work a cue declared with Do. Every field is here so that
// an Engine can choose how to perform it, rather than deck deciding for it.
type Work struct {
	// CueName is the cue that declared this work. An Engine can use it to
	// treat one cue's work differently from another's — a longer timeout, a
	// different queue — without the cue itself knowing anything about the
	// engine.
	CueName string

	// Func and Arg identify the work. An Engine that runs work elsewhere
	// schedules it by these — Temporal, for one, resolves Func to the activity
	// registered under its name.
	Func any
	Arg  any

	// Result points at the value a result decodes into.
	Result any

	// Local performs the work in this process, filling Result. Engines that
	// run work here call it; engines that run it elsewhere ignore it.
	Local func(ctx context.Context) error
}

// Future reports on work an Engine has started.
type Future interface {
	// IsReady reports whether the work has finished. When true, Get returns
	// without blocking.
	IsReady() bool

	// Get returns the work's error. A Deck only calls it once IsReady is true.
	Get() error
}

// Goroutines returns the default Engine: a goroutine per cue, the wall clock,
// and an Await that blocks until a cue finishes or ctx is cancelled.
//
// An Engine may be shared between concurrent runs.
func Goroutines() Engine {
	return &goEngine{wake: make(chan struct{}, 1)}
}

type goEngine struct {
	// wake carries at most one pending nudge; Await re-checks ready on every
	// wake, so a coalesced nudge cannot lose a completion.
	wake chan struct{}
}

func (e *goEngine) Now() time.Time { return time.Now() }

func (e *goEngine) Spawn(fn func()) Future {
	return e.start(func() error { fn(); return nil })
}

func (e *goEngine) Execute(ctx context.Context, w Work) Future {
	return e.start(func() error { return w.Local(ctx) })
}

func (e *goEngine) start(fn func() error) Future {
	f := &goFuture{}
	go func() {
		err := fn()
		f.settle(err)
		select {
		case e.wake <- struct{}{}:
		default:
		}
	}()
	return f
}

func (e *goEngine) Await(ctx context.Context, fs []Future) error {
	for !AnyReady(fs) {
		select {
		case <-e.wake:
		case <-ctx.Done():
			// Passed through unwrapped: the Deck adds the run's own context
			// when it reports this.
			return ctx.Err() //nolint:wrapcheck
		}
	}
	return nil
}

// goFuture's mutex is what publishes the work's writes to the reader: settle
// unlocks after the work is done, IsReady locks before the Deck reads it.
type goFuture struct {
	mu    sync.Mutex
	ready bool
	err   error
}

func (f *goFuture) settle(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err, f.ready = err, true
}

func (f *goFuture) IsReady() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ready
}

func (f *goFuture) Get() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.err
}

// Serial returns an Engine that runs each cue inline, to completion, in the
// order cues were triggered. Nothing runs concurrently and nothing ever waits,
// so a run is reproducible.
func Serial() Engine { return serialEngine{} }

type serialEngine struct{}

func (serialEngine) Now() time.Time { return time.Now() }

func (serialEngine) Spawn(fn func()) Future {
	fn()
	return ReadyFuture{}
}

func (serialEngine) Execute(ctx context.Context, w Work) Future {
	return ReadyFuture{Err: w.Local(ctx)}
}

func (serialEngine) Await(_ context.Context, fs []Future) error {
	if AnyReady(fs) {
		return nil
	}
	return errors.New("serial engine has nothing left to run")
}

// ReadyFuture is a Future that has already finished. Engines that complete work
// before returning from Go can use it as their handle.
type ReadyFuture struct{ Err error }

func (f ReadyFuture) IsReady() bool { return true }
func (f ReadyFuture) Get() error    { return f.Err }

// WithClock returns e with now in place of its clock, leaving the rest of the
// engine alone. Use it to make timestamps predictable in tests.
func WithClock(e Engine, now func() time.Time) Engine {
	return clockEngine{Engine: e, now: now}
}

type clockEngine struct {
	Engine
	now func() time.Time
}

func (c clockEngine) Now() time.Time { return c.now() }
