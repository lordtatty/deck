package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/lordtatty/deck"
	"github.com/lordtatty/deck/examples/eitherway/flow"
	decktemporal "github.com/lordtatty/deck/temporal"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// flowDeck is built once, at startup, and shared by every run. Cues are
// read-only after New, so this is safe — but the Engine differs per run, which
// is what WithEngine is for. Assigning to flowDeck.Engine instead would race
// between concurrent runs.
var flowDeck = func() *deck.Deck[flow.Input, flow.State] {
	d, err := deck.New(flow.Cues()...)
	if err != nil {
		log.Fatalf("building deck: %v", err)
	}
	return d
}()

// Runner runs the flow. Which implementation you get is a startup decision;
// everything after that point is written against this interface and does not
// care.
type Runner interface {
	Run(ctx context.Context, in flow.Input) (flow.State, error)
	Close()
}

// ---------------------------------------------------------------------------
// Inline: cues on goroutines, in this process
// ---------------------------------------------------------------------------

type Inline struct{}

func (Inline) Close() {}

func (Inline) Run(ctx context.Context, in flow.Input) (flow.State, error) {
	// No Engine, so the default: a goroutine per cue and the wall clock.
	var state flow.State
	if _, err := flowDeck.Run(ctx, in, &state); err != nil {
		return state, fmt.Errorf("running flow: %w", err)
	}
	return state, nil
}

// ---------------------------------------------------------------------------
// Temporal: the same cues, durable
// ---------------------------------------------------------------------------

const taskQueue = "eitherway"

// IndexWorkflow is the same flow again. deck.New takes the identical cues; only
// the Engine differs from Inline.Run above.
func IndexWorkflow(ctx workflow.Context, in flow.Input) (flow.State, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
	})

	// WithEngine, not flowDeck.Engine = ...: a worker runs many workflows at
	// once, and they would all be writing the same field.
	d := flowDeck.WithEngine(decktemporal.New(ctx))

	// Cancellation arrives through the Engine's workflow context, so the
	// context here is only a placeholder.
	var state flow.State
	if _, err := d.Run(context.Background(), in, &state); err != nil {
		return state, fmt.Errorf("running flow: %w", err)
	}
	return state, nil
}

type Temporal struct {
	client client.Client
	worker worker.Worker
	stop   func()
}

// NewTemporal wires up everything Temporal needs. In a real service the client
// and worker usually live in separate processes; they are together here so the
// example is one command.
func NewTemporal() *Temporal {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	srv, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ClientOptions: &client.Options{Logger: quietLogger{}},
		LogLevel:      "error",
		Stdout:        io.Discard,
		Stderr:        io.Discard,
	})
	if err != nil {
		log.Fatalf("starting dev server: %v", err)
	}

	c := srv.Client()
	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterWorkflow(IndexWorkflow)

	// The flow's own functions, registered exactly as written — there is no
	// Temporal-specific copy of them anywhere. Only the three the cues declared
	// with Do appear here: keywords and index never leave the workflow, so
	// there is nothing to register for them.
	w.RegisterActivity(flow.Fetch)
	w.RegisterActivity(flow.Summarise)
	w.RegisterActivity(flow.Classify)

	if err := w.Start(); err != nil {
		log.Fatalf("starting worker: %v", err)
	}
	return &Temporal{client: c, worker: w, stop: func() { _ = srv.Stop() }}
}

func (t *Temporal) Close() {
	t.worker.Stop()
	t.stop()
}

func (t *Temporal) Run(ctx context.Context, in flow.Input) (flow.State, error) {
	run, err := t.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        fmt.Sprintf("index-%d", time.Now().UnixNano()),
		TaskQueue: taskQueue,
	}, IndexWorkflow, in)
	if err != nil {
		return flow.State{}, fmt.Errorf("starting workflow: %w", err)
	}

	var state flow.State
	if err := run.Get(ctx, &state); err != nil {
		return state, fmt.Errorf("workflow failed: %w", err)
	}
	return state, nil
}

// quietLogger keeps the SDK's chatter out of the example's output. Swap it for
// your own logger in a real service.
type quietLogger struct{}

func (quietLogger) Debug(string, ...any) {}
func (quietLogger) Info(string, ...any)  {}
func (quietLogger) Warn(string, ...any)  {}
func (quietLogger) Error(msg string, _ ...any) {
	fmt.Println("temporal:", msg)
}
