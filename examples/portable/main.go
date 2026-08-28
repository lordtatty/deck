// The same deck flow, run twice: once as ordinary Go, once inside a Temporal
// workflow where it is durable — if the process dies mid-flow, it picks up
// where it left off on another machine.
//
// The cues are in ./flow and are identical in both runs. Only the Engine
// changes.
//
//	go run .
//
// The first run downloads a Temporal dev server (a one-off, cached by the SDK).
// Nothing else to install.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/lordtatty/deck"
	"github.com/lordtatty/deck/examples/portable/flow"
	decktemporal "github.com/lordtatty/deck/temporal"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const taskQueue = "deck-example"

// ---------------------------------------------------------------------------
// Plain Go
// ---------------------------------------------------------------------------

func runLocally(in flow.Input) flow.State {
	d, err := deck.New(flow.Cues()...)
	if err != nil {
		log.Fatalf("building deck: %v", err)
	}
	// No Engine set, so cues run on goroutines against the wall clock.

	var state flow.State
	if _, err := d.Run(context.Background(), in, &state); err != nil {
		log.Fatalf("running locally: %v", err)
	}
	return state
}

// ---------------------------------------------------------------------------
// Inside a Temporal workflow
// ---------------------------------------------------------------------------

// ReportWorkflow is the whole integration. Two lines of it are about Temporal;
// the rest is the same deck code you would write anywhere.
func ReportWorkflow(ctx workflow.Context, in flow.Input) (flow.State, error) {
	// Activity settings are yours to choose — deck will not invent them. Use
	// decktemporal.ForCue("name", opts) where one cue needs different settings
	// from the rest.
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
	})

	d, err := deck.New(flow.Cues()...)
	if err != nil {
		return flow.State{}, fmt.Errorf("building deck: %w", err)
	}

	// The one line that makes this durable. Now a cue's Run executes inline on
	// the workflow coroutine, and the work it declares becomes an activity.
	d.Engine = decktemporal.New(ctx)

	// Cancellation comes from the workflow context the Engine holds, so the
	// context passed here is just a placeholder.
	var state flow.State
	if _, err := d.Run(context.Background(), in, &state); err != nil {
		return state, fmt.Errorf("running deck: %w", err)
	}
	return state, nil
}

func runInTemporal(c client.Client, in flow.Input) flow.State {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        fmt.Sprintf("deck-example-%d", time.Now().UnixNano()),
		TaskQueue: taskQueue,
	}, ReportWorkflow, in)
	if err != nil {
		log.Fatalf("starting workflow: %v", err)
	}

	var state flow.State
	if err := run.Get(ctx, &state); err != nil {
		log.Fatalf("workflow failed: %v", err)
	}
	return state
}

func main() {
	in := flow.Input{CustomerID: "cust-42"}

	fmt.Println("The profile and orders lookups take 400ms each and do not")
	fmt.Println("depend on each other. In series that is 800ms; together, 400ms.")
	fmt.Println()

	start := time.Now()
	local := runLocally(in)
	fmt.Printf("plain Go   %-34s %v\n", local.Report, time.Since(start).Round(10*time.Millisecond))

	fmt.Println()
	fmt.Println("Starting a Temporal dev server (downloaded once, then cached)...")
	c, stop := devServer()
	defer stop()

	// A worker runs both the workflow and the functions the cues declared. Note
	// that FetchProfile and FetchOrders are registered exactly as written — the
	// flow package needed no Temporal-specific version of them.
	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterWorkflow(ReportWorkflow)
	w.RegisterActivity(flow.FetchProfile)
	w.RegisterActivity(flow.FetchOrders)
	if err := w.Start(); err != nil {
		log.Fatalf("starting worker: %v", err)
	}
	defer w.Stop()

	start = time.Now()
	durable := runInTemporal(c, in)
	fmt.Printf("Temporal   %-34s %v\n", durable.Report, time.Since(start).Round(10*time.Millisecond))

	fmt.Println()
	if local.Report == durable.Report {
		fmt.Println("Same cues, same answer, parallel in both — and the Temporal run")
		fmt.Println("would survive the process being killed halfway through.")
	}
}

// devServer starts a throwaway Temporal server so this example runs with no
// setup. In a real service you would dial your own instead:
//
//	c, err := client.Dial(client.Options{HostPort: "temporal:7233"})
func devServer() (client.Client, func()) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Quiet: this example is about deck, not about Temporal's logs.
	srv, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ClientOptions: &client.Options{Logger: quietLogger{}},
		LogLevel:      "error",
		Stdout:        io.Discard,
		Stderr:        io.Discard,
	})
	if err != nil {
		log.Fatalf("starting dev server: %v", err)
	}
	return srv.Client(), func() { _ = srv.Stop() }
}

// quietLogger keeps the SDK's chatter out of the example's output. Swap it for
// your own logger in a real service.
type quietLogger struct{}

func (quietLogger) Debug(string, ...any) {}
func (quietLogger) Info(string, ...any)  {}
func (quietLogger) Warn(string, ...any)  {}
func (quietLogger) Error(msg string, keyvals ...any) {
	log.Println(append([]any{"temporal:", msg}, keyvals...)...)
}
