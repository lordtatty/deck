package temporal_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lordtatty/deck"
	decktemporal "github.com/lordtatty/deck/temporal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// Someone cancels the order. The workflow is cancelled while the Deck is
// waiting on activities that have not finished, and it has to stop promptly
// rather than sit there until they do.
//
// Two things are checked. The Deck must come back at once — it is parked in
// workflow.Await, which Temporal unblocks on cancellation — and the run must be
// recorded as Cancelled rather than Failed, since that is what anyone reading
// the history goes by.
//
// On the second: what matters is that the error the Deck returns originates in
// the cancellation. Probing established the boundary — a workflow returning a
// wholly unrelated error after being cancelled is reported as Failed, while the
// Deck's error is reported as Cancelled. How the SDK decides is its business
// (it substitutes its own error, so the Deck's wrapping style makes no
// difference either way); the contrast is what this pins.

const cancelQueue = "deck-cancellation"

type cancelState struct {
	Done []string `json:"done"`
}

// SlowLeg takes long enough that a cancellation always lands mid-flight. It
// does not heartbeat, so Temporal cannot interrupt it — the workflow has to
// come back without it.
func SlowLeg(ctx context.Context, name string) (string, error) {
	select {
	case <-time.After(5 * time.Second):
	case <-ctx.Done():
		return "", fmt.Errorf("leg %s cancelled: %w", name, ctx.Err())
	}
	return name, nil
}

func CancellableWorkflow(ctx workflow.Context) (cancelState, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})

	var cues []deck.Cue[struct{}, cancelState]
	for _, n := range []string{"a", "b"} {
		name := n
		cues = append(cues, deck.Cue[struct{}, cancelState]{
			Name: name,
			Run: func(_ struct{}, _ cancelState) (deck.Mutation[cancelState], error) {
				return deck.Do(SlowLeg, name, func(v string) deck.Mutation[cancelState] {
					return deck.Complete(func(s *cancelState) { s.Done = append(s.Done, v) })
				}), nil
			},
		})
	}

	d, err := deck.New(cues...)
	if err != nil {
		return cancelState{}, fmt.Errorf("build deck: %w", err)
	}
	d.Engine = decktemporal.New(ctx)

	var s cancelState
	if _, err := d.Run(context.Background(), struct{}{}, &s); err != nil {
		return s, fmt.Errorf("run deck: %w", err)
	}
	return s, nil
}

func TestCancellingTheWorkflowStopsTheDeckPromptly(t *testing.T) {
	c := startDevServer(t)
	startWorker(t, c, cancelQueue, func(w worker.Worker) {
		w.RegisterWorkflow(CancellableWorkflow)
		w.RegisterActivity(SlowLeg)
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// Given a flow whose activities will take five seconds
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        fmt.Sprintf("cancel-%d", time.Now().UnixNano()),
		TaskQueue: cancelQueue,
	}, CancellableWorkflow)
	require.NoError(t, err)

	// When it is cancelled while the Deck is waiting on them
	time.Sleep(700 * time.Millisecond)
	started := time.Now()
	require.NoError(t, c.CancelWorkflow(ctx, run.GetID(), run.GetRunID()))

	var out cancelState
	err = run.Get(ctx, &out)
	elapsed := time.Since(started)

	// Then it comes back at once rather than waiting out activities it cannot
	// interrupt — they do not heartbeat, so Temporal cannot stop them early
	t.Logf("returned after %v with: %v", elapsed.Round(10*time.Millisecond), err)
	require.Error(t, err)
	assert.Less(t, elapsed, 3*time.Second,
		"a cancelled workflow should not wait out its in-flight activities")
	assert.Empty(t, out.Done, "no cue should have completed")

	// And Temporal records it as a cancellation, not a failure. A workflow that
	// returned an unrelated error here would be reported as Failed instead —
	// that contrast is what makes this assertion worth having.
	assert.True(t, isCanceled(err),
		"a cancelled run should be recorded as Cancelled, not Failed")

	// Worth knowing for anyone reaching for the obvious check: what comes back
	// is Temporal's CanceledError, not Go's context.Canceled. Cancellation
	// inside a workflow arrives through the workflow context, which is not a
	// context.Context at all.
	assert.NotErrorIs(t, err, context.Canceled,
		"cancellation here is Temporal's, not the standard library's")
}

func isCanceled(err error) bool {
	var canceled *temporal.CanceledError
	return errors.As(err, &canceled)
}
