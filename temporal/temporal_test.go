package temporal_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lordtatty/deck"
	decktemporal "github.com/lordtatty/deck/temporal"
	"github.com/lordtatty/deck/temporal/internal/flow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// ReportWorkflow runs the flow inside a Temporal workflow. Attaching the engine
// is the only Temporal-specific line, and it is visible at the call site.
func ReportWorkflow(ctx workflow.Context, in flow.In) (flow.State, error) {
	// Activity settings are the workflow's to choose; deck does not invent them.
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
	})

	d, err := deck.New(flow.Cues()...)
	if err != nil {
		return flow.State{}, fmt.Errorf("build deck: %w", err)
	}
	d.Engine = decktemporal.New(ctx)

	var state flow.State
	_, err = d.Run(context.Background(), in, &state)
	if err != nil {
		return state, fmt.Errorf("run deck: %w", err)
	}
	return state, nil
}

// The test the whole design was built against, written before any of it
// existed. If one set of cues cannot run unchanged in both worlds then the
// Engine abstraction has not earned its place, so this is the one to look at
// first — and the one to be most suspicious of if it ever needs loosening.
//
// It asserts on overlap rather than only on the result, because a Deck that ran
// the two lookups one after the other would reach exactly the same answer.
func TestFlowRunsTheSameLocallyAndInTemporal(t *testing.T) {
	in := flow.In{UserID: "u1"}

	// When the flow runs locally, on goroutines
	d, err := deck.New(flow.Cues()...)
	require.NoError(t, err)
	var local flow.State
	_, err = d.Run(context.Background(), in, &local)
	require.NoError(t, err)

	// And the same cues run inside a Temporal workflow
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivity(flow.FetchUser)
	env.RegisterActivity(flow.FetchOrders)
	env.ExecuteWorkflow(ReportWorkflow, in)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var remote flow.State
	require.NoError(t, env.GetWorkflowResult(&remote))

	// Then both reach the same result
	assert.Equal(t, "user:u1 + orders:u1", local.Report)
	assert.Equal(t, local.Report, remote.Report)

	// And in both, the two independent units of work overlapped
	assert.True(t, flow.Overlapped(local), "local: fetches should have overlapped")
	assert.True(t, flow.Overlapped(remote), "temporal: activities should have overlapped")
}
