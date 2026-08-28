package temporal_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lordtatty/deck"
	decktemporal "github.com/lordtatty/deck/temporal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// What a cue costs in a workflow depends entirely on what its Run returns.
// deck.Do schedules an activity; deck.Complete does not.

const countQueue = "deck-activity-count"

type countState struct {
	Values []string `json:"values"`
	Report string   `json:"report"`
}

func Echo(ctx context.Context, name string) (string, error) {
	return "v:" + name, nil
}

// echoCues returns one cue per name, each declaring an Echo activity.
func echoCues(names ...string) []deck.Cue[struct{}, countState] {
	cues := make([]deck.Cue[struct{}, countState], 0, len(names))
	for _, n := range names {
		name := n
		cues = append(cues, deck.Cue[struct{}, countState]{
			Name: name,
			Run: func(_ struct{}, _ countState) (deck.Mutation[countState], error) {
				return deck.Do(Echo, name, func(v string) deck.Mutation[countState] {
					return deck.Complete(func(s *countState) { s.Values = append(s.Values, v) })
				}), nil
			},
		})
	}
	return cues
}

// runCountDeck is the workflow-side wiring every test in this file shares.
func runCountDeck(ctx workflow.Context, cues ...deck.Cue[struct{}, countState]) (countState, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})
	d, err := deck.New(cues...)
	if err != nil {
		return countState{}, fmt.Errorf("build deck: %w", err)
	}
	d.Engine = decktemporal.New(ctx)

	var s countState
	if _, err := d.Run(context.Background(), struct{}{}, &s); err != nil {
		return s, fmt.Errorf("run deck: %w", err)
	}
	return s, nil
}

// CountingWorkflow has four cues. Three declare work with Do; the fourth only
// reads state, so it returns Complete and never leaves the workflow.
func CountingWorkflow(ctx workflow.Context) (countState, error) {
	cues := echoCues("a", "b", "c")
	cues = append(cues, deck.Cue[struct{}, countState]{
		Name: "report",
		When: func(_ struct{}, _ countState, r deck.Result) bool {
			return r.Completed("a") && r.Completed("b") && r.Completed("c")
		},
		// No Do: this cue only assembles what the others fetched, so it costs
		// the workflow nothing.
		Run: func(_ struct{}, s countState) (deck.Mutation[countState], error) {
			report := fmt.Sprintf("%d values", len(s.Values))
			return deck.Complete(func(s *countState) { s.Report = report }), nil
		},
	})
	return runCountDeck(ctx, cues...)
}

func TestOneActivityPerDoAndNoneForComplete(t *testing.T) {
	c := startDevServer(t)
	startWorker(t, c, countQueue, func(w worker.Worker) {
		w.RegisterWorkflow(CountingWorkflow)
		w.RegisterActivity(Echo)
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// Given four cues, three of which declare work with Do
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        fmt.Sprintf("count-%d", time.Now().UnixNano()),
		TaskQueue: countQueue,
	}, CountingWorkflow)
	require.NoError(t, err)

	var out countState
	require.NoError(t, run.Get(ctx, &out))
	require.Equal(t, "3 values", out.Report)

	// When the recorded history is counted
	scheduled := scheduledActivities(historyOf(t, c, run.GetID(), run.GetRunID()))

	// Then there is exactly one activity per Do, and none for the cue that only
	// read state — four cues, three activities
	t.Logf("4 cues, 3 of them using Do -> %d activities scheduled", scheduled)
	assert.Equal(t, 3, scheduled)
}

// PanickingCueWorkflow checks the Temporal half of the panic story: a cue that
// panics should fail the workflow as a named cue error, the same shape a caller
// sees under every other Engine.
func PanickingCueWorkflow(ctx workflow.Context) (countState, error) {
	return runCountDeck(ctx, deck.Cue[struct{}, countState]{
		Name: "Boom",
		Run: func(_ struct{}, _ countState) (deck.Mutation[countState], error) {
			panic("cue exploded")
		},
	})
}

func TestAPanickingCueFailsTheWorkflowAsACueError(t *testing.T) {
	// Given a cue that panics inside a workflow
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(PanickingCueWorkflow)

	// When the workflow finishes
	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()

	// Then it failed with the cue named, rather than as a bare panic — the same
	// shape a caller sees from any other Engine
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cue Boom")
	assert.Contains(t, err.Error(), "cue exploded")
	t.Logf("workflow error: %v", firstLine(err.Error()))
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// A wide fan-out is where a workflow meets limits deck does not have: every Do
// is an activity, and every activity is several entries in the workflow's
// history. Fifty at once is a realistic ceiling for a real flow, and worth
// knowing works — the core handles thousands, but the constraint here is
// Temporal's, not the runner's.
func TestAWideFanOutCompletesInOneWorkflow(t *testing.T) {
	c := startDevServer(t)
	startWorker(t, c, wideQueue, func(w worker.Worker) {
		w.RegisterWorkflow(WideFanOutWorkflow)
		w.RegisterActivity(Echo)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Given fifty independent cues, each declaring its own activity
	start := time.Now()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        fmt.Sprintf("wide-%d", time.Now().UnixNano()),
		TaskQueue: wideQueue,
	}, WideFanOutWorkflow)
	require.NoError(t, err)

	var out countState
	require.NoError(t, run.Get(ctx, &out))
	elapsed := time.Since(start)

	// Then all fifty completed
	assert.Len(t, out.Values, wideCount)

	// And the history stayed proportionate: a handful of events per activity,
	// nothing quadratic
	hist := historyOf(t, c, run.GetID(), run.GetRunID())
	scheduled := scheduledActivities(hist)
	t.Logf("%d cues -> %d activities, %d history events, %v",
		wideCount, scheduled, len(hist.Events), elapsed.Round(10*time.Millisecond))
	assert.Equal(t, wideCount, scheduled)
	assert.Less(t, len(hist.Events), wideCount*10,
		"history should grow in proportion to the activities, not faster")
}

const (
	wideQueue = "deck-wide-fanout"
	wideCount = 50
)

func WideFanOutWorkflow(ctx workflow.Context) (countState, error) {
	names := make([]string, wideCount)
	for i := range names {
		names[i] = fmt.Sprintf("cue%d", i)
	}
	return runCountDeck(ctx, echoCues(names...)...)
}
