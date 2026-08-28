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
	enumspb "go.temporal.io/api/enums/v1"
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

// CountingWorkflow has four cues. Three declare work with Do; the fourth only
// reads state, so it returns Complete and never leaves the workflow.
func CountingWorkflow(ctx workflow.Context) (countState, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})

	var cues []deck.Cue[struct{}, countState]
	for _, n := range []string{"a", "b", "c"} {
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

func TestOneActivityPerDoAndNoneForComplete(t *testing.T) {
	c := startDevServer(t)

	w := worker.New(c, countQueue, worker.Options{})
	w.RegisterWorkflow(CountingWorkflow)
	w.RegisterActivity(Echo)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

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
	scheduled := 0
	for _, e := range historyOf(t, c, run.GetID(), run.GetRunID()).Events {
		if e.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED {
			scheduled++
		}
	}

	// Then there is exactly one activity per Do, and none for the cue that only
	// read state — four cues, three activities
	t.Logf("4 cues, 3 of them using Do -> %d activities scheduled", scheduled)
	assert.Equal(t, 3, scheduled)
}

// PanickingCueWorkflow is the Temporal half of the panic story. Before deck
// recovered panics the SDK caught this one and reported a workflow panic; now
// it arrives as an ordinary cue error, the same as under every other Engine.
func PanickingCueWorkflow(ctx workflow.Context) (countState, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})
	cue := deck.Cue[struct{}, countState]{
		Name: "Boom",
		Run: func(_ struct{}, _ countState) (deck.Mutation[countState], error) {
			panic("cue exploded")
		},
	}
	d, err := deck.New(cue)
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
