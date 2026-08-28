package temporal_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lordtatty/deck"
	decktemporal "github.com/lordtatty/deck/temporal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// Trace records what happened and when, so a test can tell parallel from
// sequential rather than taking anyone's word for it.
type Trace struct {
	mu     sync.Mutex
	events []string
	t0     time.Time
}

func (tr *Trace) add(format string, a ...any) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.events = append(tr.events, fmt.Sprintf("%4dms %s",
		time.Since(tr.t0).Milliseconds(), fmt.Sprintf(format, a...)))
}

var trace = &Trace{}

type parState struct{ Done []string }

// SlowStep is the activity. It takes 200ms and reports when it ran.
func SlowStep(ctx context.Context, name string) (string, error) {
	trace.add("activity %s START", name)
	select {
	case <-time.After(200 * time.Millisecond):
	case <-ctx.Done():
		return "", ctx.Err() //nolint:wrapcheck
	}
	trace.add("activity %s END", name)
	return name, nil
}

func parallelCues() []deck.Cue[struct{}, parState] {
	var cues []deck.Cue[struct{}, parState]
	for _, n := range []string{"A", "B", "C", "D"} {
		name := n
		cues = append(cues, deck.Cue[struct{}, parState]{
			Name: name,
			Run: func(_ struct{}, _ parState) (deck.Mutation[parState], error) {
				trace.add("cue %s Run (declares work)", name)
				return deck.Do(SlowStep, name, func(v string) deck.Mutation[parState] {
					trace.add("cue %s collected", name)
					return deck.Complete(func(s *parState) { s.Done = append(s.Done, v) })
				}), nil
			},
		})
	}
	return cues
}

func ParallelWorkflow(ctx workflow.Context) (parState, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
	})
	d, err := deck.New(parallelCues()...)
	if err != nil {
		return parState{}, fmt.Errorf("build deck: %w", err)
	}
	d.Engine = decktemporal.New(ctx)

	var s parState
	if _, err := d.Run(context.Background(), struct{}{}, &s); err != nil {
		return s, fmt.Errorf("run deck: %w", err)
	}
	return s, nil
}

// Parallelism, shown rather than asserted: the logged timeline is the point of
// this test as much as the elapsed-time bound. It separates the two things that
// are easy to confuse — the Deck decides one cue at a time (all four Runs land
// at 0ms), while the work itself goes out together (all four activities start
// at once and finish together). Sequential execution would take four times as
// long and read completely differently.
func TestFourCuesRunTheirWorkInParallel(t *testing.T) {
	trace = &Trace{t0: time.Now()}

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivity(SlowStep)

	start := time.Now()
	env.ExecuteWorkflow(ParallelWorkflow)
	elapsed := time.Since(start)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var s parState
	require.NoError(t, env.GetWorkflowResult(&s))

	for _, e := range trace.events {
		t.Log(e)
	}
	t.Logf("TOTAL %v  (4 x 200ms work: ~200ms if parallel, ~800ms if sequential)", elapsed.Round(10*time.Millisecond))

	assert.ElementsMatch(t, []string{"A", "B", "C", "D"}, s.Done)
	assert.Less(t, elapsed, 500*time.Millisecond, "four 200ms activities should overlap, not queue up")
}
