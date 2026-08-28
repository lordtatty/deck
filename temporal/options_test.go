package temporal_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lordtatty/deck"
	decktemporal "github.com/lordtatty/deck/temporal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// One flow, two units of work with very different appetites: the sort of thing
// a single set of activity options cannot serve.

type optState struct {
	Values []string `json:"values"`
}

func SizedStep(ctx context.Context, ms int) (string, error) {
	select {
	case <-time.After(time.Duration(ms) * time.Millisecond):
	case <-ctx.Done():
		return "", fmt.Errorf("sized step cancelled: %w", ctx.Err())
	}
	return fmt.Sprintf("%dms", ms), nil
}

func optCues() []deck.Cue[struct{}, optState] {
	mk := func(name string, ms int) deck.Cue[struct{}, optState] {
		return deck.Cue[struct{}, optState]{
			Name: name,
			Run: func(_ struct{}, _ optState) (deck.Mutation[optState], error) {
				return deck.Do(SizedStep, ms, func(v string) deck.Mutation[optState] {
					return deck.Complete(func(s *optState) { s.Values = append(s.Values, v) })
				}), nil
			},
		}
	}
	return []deck.Cue[struct{}, optState]{mk("quick", 50), mk("slow", 700)}
}

func once(d time.Duration) workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: d,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	}
}

// OptionsWorkflow runs the same cues either way. The flow itself is unchanged
// between the two: only the wiring differs.
func OptionsWorkflow(ctx workflow.Context, perCue bool) ([]string, error) {
	ctx = workflow.WithActivityOptions(ctx, once(300*time.Millisecond))

	var opts []decktemporal.Option
	if perCue {
		opts = append(opts, decktemporal.ForCue("slow", once(5*time.Second)))
	}

	d, err := deck.New(optCues()...)
	if err != nil {
		return nil, fmt.Errorf("build deck: %w", err)
	}
	d.Engine = decktemporal.New(ctx, opts...)

	var s optState
	if _, err := d.Run(context.Background(), struct{}{}, &s); err != nil {
		return nil, fmt.Errorf("run deck: %w", err)
	}
	return s.Values, nil
}

func runOptions(t *testing.T, perCue bool) ([]string, error) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivity(SizedStep)
	env.ExecuteWorkflow(OptionsWorkflow, perCue)
	require.True(t, env.IsWorkflowCompleted())
	if err := env.GetWorkflowError(); err != nil {
		return nil, fmt.Errorf("workflow: %w", err)
	}
	var out []string
	require.NoError(t, env.GetWorkflowResult(&out))
	return out, nil
}

// This pair is a before-and-after on one unchanged flow, and the failing half
// is as important as the passing one: it establishes that the shared timeout
// really is too short, so the test below is demonstrating ForCue working rather
// than a timeout that was never going to fire.
func TestOneSetOfOptionsCannotServeEveryCue(t *testing.T) {
	// Given a 300ms timeout shared by every cue, when one cue's work takes 700ms
	_, err := runOptions(t, false)

	// Then that cue times out and takes the run with it
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cue slow")
	t.Logf("shared options: %v", err)
}

func TestForCueGivesOneCueItsOwnOptions(t *testing.T) {
	// Given the same flow, wired with options for the cue that needs them
	values, err := runOptions(t, true)

	// Then both cues complete — the flow was not touched, only the wiring
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"50ms", "700ms"}, values)
}
