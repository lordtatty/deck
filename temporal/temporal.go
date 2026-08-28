// Package temporal runs a deck Deck inside a Temporal workflow.
//
// A workflow must be deterministic: no wall clock, no goroutines, no native
// channels or selects. This package supplies a deck.Engine that satisfies all
// of that, so the cues themselves need no knowledge of Temporal:
//
//	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
//		StartToCloseTimeout: time.Minute,
//	})
//
//	d, err := deck.New(cues...)
//	if err != nil {
//		return err
//	}
//	d.Engine = temporal.New(ctx)
//	_, err = d.Run(context.Background(), input, &state)
//
// Work a cue declares with deck.Do becomes an activity. Register the same
// functions with your worker, and Temporal resolves them by name.
//
// Two things to know:
//
//   - Cancellation reaches the Deck through the workflow context this engine
//     holds, not through the context passed to Run. Pass context.Background().
//
//   - A cue's Run executes inline on the workflow coroutine, so it must not
//     block. Declare work with deck.Do instead; a cue that blocks will trip
//     Temporal's deadlock detector.
//
// Where one cue needs different activity settings from the rest, name it in the
// wiring rather than in the flow:
//
//	d.Engine = temporal.New(ctx,
//		temporal.ForCue("summarise", workflow.ActivityOptions{
//			StartToCloseTimeout: 10 * time.Minute,
//		}),
//	)
package temporal

import (
	"context"
	"fmt"
	"time"

	"github.com/lordtatty/deck"
	"go.temporal.io/sdk/workflow"
)

// New returns an Engine that runs a Deck inside the workflow ctx belongs to.
//
// Activity settings come from ctx, so apply workflow.WithActivityOptions before
// calling this — deck does not choose them for you. Use ForCue where one cue
// needs different settings from the rest.
func New(ctx workflow.Context, opts ...Option) deck.Engine {
	e := &engine{ctx: ctx}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Option configures the Engine returned by New.
type Option func(*engine)

// ForCue sets the activity options used for the work one cue declares, in place
// of those on the workflow context.
//
// Activity options are Temporal's, so a cue that set its own would have to
// import Temporal — and would stop running anywhere else. Naming the cue here
// keeps the flow portable and puts the timeout where the deployment decisions
// already live.
func ForCue(name string, o workflow.ActivityOptions) Option {
	return func(e *engine) {
		if e.perCue == nil {
			e.perCue = make(map[string]workflow.ActivityOptions)
		}
		e.perCue[name] = o
	}
}

type engine struct {
	ctx    workflow.Context
	perCue map[string]workflow.ActivityOptions
}

// Now is the workflow's clock. It reports the time the current workflow task
// started, so cues completing within one task can report a zero Duration.
func (e *engine) Now() time.Time { return workflow.Now(e.ctx) }

// Spawn runs the Deck's own code — a cue's Run — inline on the workflow
// coroutine. That is deterministic, and it means a cue holding this workflow's
// context is holding the right one.
func (e *engine) Spawn(fn func()) deck.Future {
	fn()
	return deck.ReadyFuture{}
}

// Execute turns declared work into an activity. ExecuteActivity does not block,
// so every cue triggered in a cycle has its activity in flight before the Deck
// waits for any of them.
func (e *engine) Execute(_ context.Context, w deck.Work) deck.Future {
	ctx := e.ctx
	// A map lookup, never a range: iteration order would not survive replay.
	if o, ok := e.perCue[w.CueName]; ok {
		ctx = workflow.WithActivityOptions(ctx, o)
	}
	return &future{
		ctx:    ctx,
		future: workflow.ExecuteActivity(ctx, w.Func, w.Arg),
		result: w.Result,
	}
}

// Await yields the workflow coroutine until one of the activities started
// above has finished, letting them all make progress meanwhile.
func (e *engine) Await(_ context.Context, fs []deck.Future) error {
	if err := workflow.Await(e.ctx, func() bool { return deck.AnyReady(fs) }); err != nil {
		return fmt.Errorf("workflow await: %w", err)
	}
	return nil
}

type future struct {
	ctx    workflow.Context
	future workflow.Future
	result any
}

func (f *future) IsReady() bool { return f.future.IsReady() }

// Get decodes the activity's result. The Deck only calls it once IsReady is
// true, and Temporal guarantees Get does not block in that case.
func (f *future) Get() error {
	if err := f.future.Get(f.ctx, f.result); err != nil {
		return fmt.Errorf("activity: %w", err)
	}
	return nil
}
