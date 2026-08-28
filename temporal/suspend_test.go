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
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// Suspend, Export and Import predate the Engine and still work under one. These
// tests pin that, so the README can say so without hedging — and so anyone
// weighing them against Temporal's own durability can see what actually happens.

type suspState struct {
	JobID  string `json:"job_id"`
	Result string `json:"result"`
}

type suspOutcome struct {
	SuspendedFirst bool      `json:"suspended_first"`
	SnapshotBytes  int       `json:"snapshot_bytes"`
	Final          suspState `json:"final"`
	Completed      []string  `json:"completed"`
}

func SubmitJob(ctx context.Context, topic string) (string, error) {
	return "job-for-" + topic, nil
}

func FetchResult(ctx context.Context, jobID string) (string, error) {
	return "result of " + jobID, nil
}

func suspendCues() []deck.Cue[string, suspState] {
	return []deck.Cue[string, suspState]{
		{
			// Submits a job, then suspends carrying the ID it was given. The
			// submission is real work, so it is declared; the suspension is
			// what the result does to state.
			Name: "submit",
			When: func(_ string, s suspState, _ deck.Result) bool { return s.JobID == "" },
			Run: func(topic string, _ suspState) (deck.Mutation[suspState], error) {
				return deck.Do(SubmitJob, topic, func(id string) deck.Mutation[suspState] {
					return deck.Suspended(func(s *suspState) { s.JobID = id })
				}), nil
			},
		},
		{
			Name: "collect",
			When: func(_ string, s suspState, _ deck.Result) bool {
				return s.JobID != "" && s.Result == ""
			},
			Run: func(_ string, s suspState) (deck.Mutation[suspState], error) {
				return deck.Do(FetchResult, s.JobID, func(r string) deck.Mutation[suspState] {
					return deck.Complete(func(s *suspState) { s.Result = r })
				}), nil
			},
		},
	}
}

// SuspendResumeWorkflow runs the whole suspend → Export → Import → resume cycle
// inside one workflow, which is the most demanding version: every step has to
// behave under the Temporal engine.
func SuspendResumeWorkflow(ctx workflow.Context, topic string) (suspOutcome, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})

	newDeck := func() (*deck.Deck[string, suspState], error) {
		d, err := deck.New(suspendCues()...)
		if err != nil {
			return nil, fmt.Errorf("build deck: %w", err)
		}
		d.Engine = decktemporal.New(ctx)
		return d, nil
	}

	d, err := newDeck()
	if err != nil {
		return suspOutcome{}, err
	}

	// First run: submit, then suspend.
	var state suspState
	first, err := d.Run(context.Background(), topic, &state)
	if err != nil {
		return suspOutcome{}, fmt.Errorf("first run: %w", err)
	}

	// Serialise and restore, exactly as a caller outside a workflow would.
	snapshot, err := d.Export(&state, first)
	if err != nil {
		return suspOutcome{}, fmt.Errorf("export: %w", err)
	}
	restored, prev, err := d.Import(snapshot)
	if err != nil {
		return suspOutcome{}, fmt.Errorf("import: %w", err)
	}

	// Second run: collect.
	second, err := d.Run(context.Background(), topic, restored, prev)
	if err != nil {
		return suspOutcome{}, fmt.Errorf("second run: %w", err)
	}

	names := make([]string, 0, len(second.CompletedCues))
	for _, c := range second.CompletedCues {
		names = append(names, c.Name)
	}
	return suspOutcome{
		SuspendedFirst: first.Suspended,
		SnapshotBytes:  len(snapshot),
		Final:          *restored,
		Completed:      names,
	}, nil
}

func TestSuspendAndResumeWorkUnderTheTemporalEngine(t *testing.T) {
	// Given a flow that submits a job, suspends, and later collects the result
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivity(SubmitJob)
	env.RegisterActivity(FetchResult)

	// When the whole suspend / export / import / resume cycle runs in a workflow
	env.ExecuteWorkflow(SuspendResumeWorkflow, "widgets")
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var out suspOutcome
	require.NoError(t, env.GetWorkflowResult(&out))

	// Then it behaves exactly as it does without Temporal
	assert.True(t, out.SuspendedFirst, "first run should have suspended")
	assert.Positive(t, out.SnapshotBytes, "Export should produce a snapshot")
	assert.Equal(t, "job-for-widgets", out.Final.JobID)
	assert.Equal(t, "result of job-for-widgets", out.Final.Result)

	// And the suspending cue is not replayed after resume — only collect runs
	assert.Equal(t, []string{"collect"}, out.Completed)
	t.Logf("suspended=%v snapshot=%d bytes final=%+v completed=%v",
		out.SuspendedFirst, out.SnapshotBytes, out.Final, out.Completed)
}

// The test above does the whole cycle inside one workflow, which proves the
// pieces work under the Engine but not the shape anyone would actually use.
// This is that shape: one workflow suspends and hands back a snapshot, the
// snapshot survives outside Temporal entirely, and a second, separate workflow
// execution picks it up and finishes the flow.
//
// It is the pattern deck's README steers Temporal users away from — Temporal is
// already keeping the flow alive, so ending the workflow to persist state
// yourself gives that up. But the README also says it still works, and this is
// what makes that claim checkable rather than reassuring.

func SuspendOnlyWorkflow(ctx workflow.Context, topic string) ([]byte, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})
	d, err := deck.New(suspendCues()...)
	if err != nil {
		return nil, fmt.Errorf("build deck: %w", err)
	}
	d.Engine = decktemporal.New(ctx)

	var state suspState
	result, err := d.Run(context.Background(), topic, &state)
	if err != nil {
		return nil, fmt.Errorf("run: %w", err)
	}
	if !result.Suspended {
		return nil, errors.New("expected the flow to suspend")
	}
	snapshot, err := d.Export(&state, result)
	if err != nil {
		return nil, fmt.Errorf("export: %w", err)
	}
	return snapshot, nil
}

func ResumeFromSnapshotWorkflow(ctx workflow.Context, snapshot []byte, topic string) (suspState, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})
	d, err := deck.New(suspendCues()...)
	if err != nil {
		return suspState{}, fmt.Errorf("build deck: %w", err)
	}
	d.Engine = decktemporal.New(ctx)

	restored, prev, err := d.Import(snapshot)
	if err != nil {
		return suspState{}, fmt.Errorf("import: %w", err)
	}
	if _, err := d.Run(context.Background(), topic, restored, prev); err != nil {
		return *restored, fmt.Errorf("resume: %w", err)
	}
	return *restored, nil
}

func TestAFlowResumesInASeparateWorkflowExecution(t *testing.T) {
	newEnv := func() *testsuite.TestWorkflowEnvironment {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		env.RegisterActivity(SubmitJob)
		env.RegisterActivity(FetchResult)
		return env
	}

	// Given a first workflow that suspends and hands back a snapshot
	first := newEnv()
	first.ExecuteWorkflow(SuspendOnlyWorkflow, "widgets")
	require.True(t, first.IsWorkflowCompleted())
	require.NoError(t, first.GetWorkflowError())

	var snapshot []byte
	require.NoError(t, first.GetWorkflowResult(&snapshot))
	require.NotEmpty(t, snapshot)
	t.Logf("snapshot handed out of workflow 1: %s", snapshot)

	// When a second, entirely separate execution resumes from it
	second := newEnv()
	second.ExecuteWorkflow(ResumeFromSnapshotWorkflow, snapshot, "widgets")
	require.True(t, second.IsWorkflowCompleted())
	require.NoError(t, second.GetWorkflowError())

	var final suspState
	require.NoError(t, second.GetWorkflowResult(&final))

	// Then the flow finished, with the second execution picking up exactly
	// where the first left off rather than starting again
	assert.Equal(t, "job-for-widgets", final.JobID)
	assert.Equal(t, "result of job-for-widgets", final.Result)
}
