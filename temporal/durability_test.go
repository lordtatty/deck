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
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// These tests kill the worker that started a flow and bring up a fresh one, to
// check the promise that makes Temporal worth the trouble: a flow outlives the
// process running it.

const durQueue = "deck-durability"

// Step records which unit of work ran and when, so a test can tell whether it
// ran before or after the worker was replaced.
type Step struct {
	Name    string    `json:"name"`
	Started time.Time `json:"started"`
}

type durState struct {
	Steps []Step `json:"steps"`
}

// DurableStep takes long enough that a worker can be stopped mid-flow.
func DurableStep(ctx context.Context, name string) (Step, error) {
	started := time.Now()
	select {
	case <-time.After(600 * time.Millisecond):
	case <-ctx.Done():
		return Step{}, fmt.Errorf("step %s cancelled: %w", name, ctx.Err())
	}
	return Step{Name: name, Started: started}, nil
}

func durCue(name string, needs string) deck.Cue[struct{}, durState] {
	c := deck.Cue[struct{}, durState]{
		Name: name,
		Run: func(_ struct{}, _ durState) (deck.Mutation[durState], error) {
			return deck.Do(DurableStep, name, func(s Step) deck.Mutation[durState] {
				return deck.Complete(func(st *durState) { st.Steps = append(st.Steps, s) })
			}), nil
		},
	}
	if needs != "" {
		c.When = func(_ struct{}, _ durState, r deck.Result) bool { return r.Completed(needs) }
	}
	return c
}

func runDurDeck(ctx workflow.Context, cues ...deck.Cue[struct{}, durState]) (durState, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})
	d, err := deck.New(cues...)
	if err != nil {
		return durState{}, fmt.Errorf("build deck: %w", err)
	}
	d.Engine = decktemporal.New(ctx)

	var st durState
	if _, err := d.Run(context.Background(), struct{}{}, &st); err != nil {
		return st, fmt.Errorf("run deck: %w", err)
	}
	return st, nil
}

// DurableChainWorkflow is four cues in a chain: each waits for the one before,
// so there is always work left to do.
func DurableChainWorkflow(ctx workflow.Context) (durState, error) {
	return runDurDeck(ctx,
		durCue("one", ""),
		durCue("two", "one"),
		durCue("three", "two"),
		durCue("four", "three"),
	)
}

// DurableParallelWorkflow runs a wave of four cues at once, joins them, then
// runs a second wave. Stopping the worker mid-join means the replacement has to
// rebuild the state of the whole first wave and then start fresh parallel work
// of its own.
func DurableParallelWorkflow(ctx workflow.Context) (durState, error) {
	join := durCue("join", "")
	join.When = func(_ struct{}, _ durState, r deck.Result) bool {
		return r.Completed("a") && r.Completed("b") && r.Completed("c") && r.Completed("d")
	}
	return runDurDeck(ctx,
		durCue("a", ""), durCue("b", ""), durCue("c", ""), durCue("d", ""),
		join,
		durCue("x", "join"), durCue("y", "join"),
	)
}

// newDurWorker starts a worker on the durability queue. The short sticky
// timeout is what stops a dead worker's cached workflow from sitting idle for
// the default ten seconds before another worker may pick it up.
func newDurWorker(t *testing.T, c client.Client, identity string) worker.Worker {
	t.Helper()
	w := worker.New(c, durQueue, worker.Options{
		Identity:                     identity,
		StickyScheduleToStartTimeout: time.Second,
	})
	w.RegisterWorkflow(DurableChainWorkflow)
	w.RegisterWorkflow(DurableParallelWorkflow)
	w.RegisterActivity(DurableStep)
	require.NoError(t, w.Start(), "starting %s", identity)
	return w
}

// runAcrossAWorkerRestart starts wf on one worker, replaces that worker
// mid-flow, and returns the flow's result plus the moment the first worker was
// gone.
func runAcrossAWorkerRestart(t *testing.T, wf any, name string) (durState, time.Time) {
	t.Helper()
	c := startDevServer(t)

	first := newDurWorker(t, c, "worker-1")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        fmt.Sprintf("%s-%d", name, time.Now().UnixNano()),
		TaskQueue: durQueue,
	}, wf)
	require.NoError(t, err)

	// Let the flow get under way, then take the worker away from it.
	time.Sleep(800 * time.Millisecond)
	first.Stop()
	stopped := time.Now()
	t.Logf("worker-1 stopped %v into the flow", 800*time.Millisecond)

	// A fresh process picks the flow up. It has none of the first worker's
	// memory: everything it knows, it rebuilds from history.
	second := newDurWorker(t, c, "worker-2")
	t.Cleanup(second.Stop)

	var out durState
	require.NoError(t, run.Get(ctx, &out), "flow did not survive the restart")
	return out, stopped
}

func namesOf(st durState) []string {
	names := make([]string, 0, len(st.Steps))
	for _, s := range st.Steps {
		names = append(names, s.Name)
	}
	return names
}

// startedAfter counts the steps that began only once the first worker was gone,
// proving the replacement really did the remaining work.
func startedAfter(st durState, cut time.Time) []string {
	var late []string
	for _, s := range st.Steps {
		if s.Started.After(cut) {
			late = append(late, s.Name)
		}
	}
	return late
}

// The promise that justifies the whole exercise: kill the process running a
// flow and the flow carries on somewhere else. worker-2 shares no memory with
// worker-1, so everything it knows about which cues have finished it rebuilt
// from history alone.
//
// The assertion to keep is the last one. Checking only that the flow completed
// would pass even if worker-1 had quietly finished everything before stopping;
// asserting that steps *began* after it was gone is what proves the
// replacement did real work.
func TestChainedFlowSurvivesAWorkerRestart(t *testing.T) {
	// Given a chain of four cues, when the worker is replaced part way through
	out, stopped := runAcrossAWorkerRestart(t, DurableChainWorkflow, "chain")

	// Then the flow still finished, in order
	assert.Equal(t, []string{"one", "two", "three", "four"}, namesOf(out))

	// And the work that remained was done by the replacement
	late := startedAfter(out, stopped)
	t.Logf("steps run after worker-1 was gone: %v", late)
	assert.NotEmpty(t, late, "no step ran after the restart, so nothing was proven")
}

// The harder version of the above. A chain only ever has one cue in flight, so
// it says little about rebuilding state. This one restarts mid-join, so the
// replacement has to reconstruct the whole first wave's completions before it
// can decide anything — and then start fresh parallel work of its own.
func TestParallelFlowSurvivesAWorkerRestart(t *testing.T) {
	// Given four cues in flight at once and a join behind them, when the worker
	// is replaced part way through
	out, stopped := runAcrossAWorkerRestart(t, DurableParallelWorkflow, "parallel")

	// Then every cue completed, with the join between the two waves
	names := namesOf(out)
	assert.ElementsMatch(t, []string{"a", "b", "c", "d", "join", "x", "y"}, names)
	joinAt := indexOf(names, "join")
	assert.Greater(t, joinAt, indexOf(names, "d"), "join should follow the first wave")
	assert.Less(t, joinAt, indexOf(names, "x"), "the second wave should follow the join")

	// And the replacement started fresh parallel work of its own, having
	// rebuilt the first wave's state from history alone
	late := startedAfter(out, stopped)
	t.Logf("steps run after worker-1 was gone: %v", late)
	assert.Subset(t, late, []string{"x", "y"}, "the second wave should have run on the replacement")
}

func indexOf(names []string, want string) int {
	for i, n := range names {
		if n == want {
			return i
		}
	}
	return -1
}
