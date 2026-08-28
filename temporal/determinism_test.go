package temporal_test

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/lordtatty/deck"
	decktemporal "github.com/lordtatty/deck/temporal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// These tests run against a real Temporal server, because replay is the only
// thing that actually proves determinism: the in-process test environment does
// not produce a replayable history.

const detQueue = "deck-determinism"

type detState struct {
	Done []string `json:"done"`
}

// JitteredStep sleeps for a random time before returning. It is activity code,
// not workflow code, so it is allowed to be non-deterministic — and the jitter
// is the point: it makes parallel work finish in a different order each run.
func JitteredStep(ctx context.Context, name string) (string, error) {
	d := time.Duration(50+rand.Intn(250)) * time.Millisecond //nolint:gosec // jitter, not crypto
	select {
	case <-time.After(d):
	case <-ctx.Done():
		return "", fmt.Errorf("step %s cancelled: %w", name, ctx.Err())
	}
	return name, nil
}

func detCues(names ...string) []deck.Cue[struct{}, detState] {
	cues := make([]deck.Cue[struct{}, detState], 0, len(names))
	for _, n := range names {
		name := n
		cues = append(cues, deck.Cue[struct{}, detState]{
			Name: name,
			Run: func(_ struct{}, _ detState) (deck.Mutation[detState], error) {
				return deck.Do(JitteredStep, name, func(v string) deck.Mutation[detState] {
					return deck.Complete(func(s *detState) { s.Done = append(s.Done, v) })
				}), nil
			},
		})
	}
	return cues
}

func runDetDeck(ctx workflow.Context, names ...string) ([]string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
	})
	d, err := deck.New(detCues(names...)...)
	if err != nil {
		return nil, fmt.Errorf("build deck: %w", err)
	}
	d.Engine = decktemporal.New(ctx)

	var s detState
	if _, err := d.Run(context.Background(), struct{}{}, &s); err != nil {
		return nil, fmt.Errorf("run deck: %w", err)
	}
	return s.Done, nil
}

// DeterminismWorkflow runs four cues whose work goes out in parallel. Done
// comes back in whatever order the activities finished.
func DeterminismWorkflow(ctx workflow.Context) ([]string, error) {
	return runDetDeck(ctx, "A", "B", "C", "D")
}

// DivergentWorkflow is the same flow with a fifth cue. Replaying
// DeterminismWorkflow's history against it must fail — that is what proves the
// replayer would catch a real divergence, rather than passing regardless.
func DivergentWorkflow(ctx workflow.Context) ([]string, error) {
	return runDetDeck(ctx, "A", "B", "C", "D", "E")
}

func startDevServer(t *testing.T) client.Client {
	t.Helper()
	if testing.Short() {
		t.Skip("needs a Temporal dev server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	srv, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{})
	require.NoError(t, err, "starting dev server")
	t.Cleanup(func() { _ = srv.Stop() })
	return srv.Client()
}

// startWorker runs a worker on queue for the rest of the test, with whatever
// register puts on it.
func startWorker(t *testing.T, c client.Client, queue string, register func(worker.Worker)) {
	t.Helper()
	w := worker.New(c, queue, worker.Options{})
	register(w)
	require.NoError(t, w.Start(), "starting worker")
	t.Cleanup(w.Stop)
}

func startDetWorker(t *testing.T, c client.Client) {
	t.Helper()
	startWorker(t, c, detQueue, func(w worker.Worker) {
		w.RegisterWorkflow(DeterminismWorkflow)
		w.RegisterActivity(JitteredStep)
	})
}

func historyOf(t *testing.T, c client.Client, workflowID, runID string) *historypb.History {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	iter := c.GetWorkflowHistory(ctx, workflowID, runID, false,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	var hist historypb.History
	for iter.HasNext() {
		event, err := iter.Next()
		require.NoError(t, err)
		hist.Events = append(hist.Events, event)
	}
	require.NotEmpty(t, hist.Events, "history should not be empty")
	return &hist
}

// scheduledActivities counts the activities a history records being started —
// one per deck.Do that reached the engine.
func scheduledActivities(hist *historypb.History) int {
	n := 0
	for _, e := range hist.Events {
		if e.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED {
			n++
		}
	}
	return n
}

// TestParallelWorkReplaysDeterministically is the headline claim: work that
// really did run in parallel, and finished in an order nobody chose, replays
// down exactly the same path.
func TestParallelWorkReplaysDeterministically(t *testing.T) {
	c := startDevServer(t)
	startDetWorker(t, c)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const runs = 3
	orders := make([]string, 0, runs)

	for i := range runs {
		// When four cues run their work in parallel
		start := time.Now()
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID:        fmt.Sprintf("determinism-%d-%d", time.Now().UnixNano(), i),
			TaskQueue: detQueue,
		}, DeterminismWorkflow)
		require.NoError(t, err)

		var done []string
		require.NoError(t, run.Get(ctx, &done))
		elapsed := time.Since(start)

		// Then all four finished, and they overlapped rather than queueing:
		// four steps of up to 300ms each would take over a second in series.
		assert.ElementsMatch(t, []string{"A", "B", "C", "D"}, done)
		assert.Less(t, elapsed, time.Second, "work should have run in parallel")

		order := strings.Join(done, ",")
		orders = append(orders, order)
		t.Logf("run %d: completion order %-12s in %v", i, order, elapsed.Round(10*time.Millisecond))

		// And replaying that exact history takes the same path
		replayer := worker.NewWorkflowReplayer()
		replayer.RegisterWorkflow(DeterminismWorkflow)
		require.NoError(t, replayer.ReplayWorkflowHistory(nil, historyOf(t, c, run.GetID(), run.GetRunID())),
			"run %d replayed differently than it ran", i)
	}

	t.Logf("orders observed across %d runs: %v", runs, orders)
}

// TestReplayCatchesDivergence is the negative control. Without it, the test
// above would pass just as happily if the replayer checked nothing.
func TestReplayCatchesDivergence(t *testing.T) {
	c := startDevServer(t)
	startDetWorker(t, c)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Given a history recorded from the four-cue flow
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        fmt.Sprintf("divergence-%d", time.Now().UnixNano()),
		TaskQueue: detQueue,
	}, DeterminismWorkflow)
	require.NoError(t, err)
	var done []string
	require.NoError(t, run.Get(ctx, &done))
	hist := historyOf(t, c, run.GetID(), run.GetRunID())

	// When it is replayed against a flow with a fifth cue, under the same name
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(DivergentWorkflow, workflow.RegisterOptions{
		Name: "DeterminismWorkflow",
	})
	err = replayer.ReplayWorkflowHistory(nil, hist)

	// Then the replayer rejects it — so a clean replay above means something
	require.Error(t, err, "replayer should reject a flow that issues different commands")
	t.Logf("divergence detected: %v", err)
}
