// A deck flow built for Temporal, using the things you go to Temporal for:
// automatic retries of flaky work, different timeouts per unit of work, and a
// full history you can open in a browser afterwards.
//
// This one is Temporal on its own terms. The README in the examples directory
// says which of them answers which question.
//
//	go run .            run the flow and exit
//	go run . -ui        run it, then leave the Web UI up so you can read the history
//
// A dev server is started for you and thrown away at the end.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/lordtatty/deck"
	decktemporal "github.com/lordtatty/deck/temporal"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const taskQueue = "orders"

type Order struct {
	ID string `json:"id"`
}

type Fulfilment struct {
	Stock    string   `json:"stock"`
	Payment  string   `json:"payment"`
	Shipment string   `json:"shipment"`
	Outcome  string   `json:"outcome"`
	Timeline []string `json:"timeline"`
}

// cues describes the flow. Stock and payment are independent, so they go out
// together; shipping waits for both; the confirmation waits for shipping.
//
// Three cues declare work with deck.Do and one does not, which is the usual
// shape. Do is for work that leaves the process: it becomes a Temporal
// activity, so it is retried, timed out and recorded independently. That is
// worth having for a warehouse call or a payment, and not worth having for
// assembling a sentence — which is why "confirm" below uses Complete instead.
//
// Nothing here mentions retries or timeouts. Those are Temporal's business, and
// they are configured in the workflow below.
func cues() []deck.Cue[Order, Fulfilment] {
	return []deck.Cue[Order, Fulfilment]{
		{
			Name: "reserve",
			Run: func(o Order, _ Fulfilment) (deck.Mutation[Fulfilment], error) {
				return deck.Do(ReserveStock, o.ID, func(v string) deck.Mutation[Fulfilment] {
					return deck.Complete(func(f *Fulfilment) { f.Stock = v })
				}), nil
			},
		},
		{
			Name: "charge",
			Run: func(o Order, _ Fulfilment) (deck.Mutation[Fulfilment], error) {
				return deck.Do(ChargePayment, o.ID, func(v string) deck.Mutation[Fulfilment] {
					return deck.Complete(func(f *Fulfilment) { f.Payment = v })
				}), nil
			},
		},
		{
			Name: "ship",
			When: func(_ Order, _ Fulfilment, r deck.Result) bool {
				return r.Completed("reserve") && r.Completed("charge")
			},
			Run: func(o Order, _ Fulfilment) (deck.Mutation[Fulfilment], error) {
				return deck.Do(ShipOrder, o.ID, func(v string) deck.Mutation[Fulfilment] {
					return deck.Complete(func(f *Fulfilment) { f.Shipment = v })
				}), nil
			},
		},
		{
			// Complete, not Do: this only reads state the other cues filled
			// in. As an activity it would cost a round trip and a history
			// entry to run a Sprintf, and gain nothing — there is nothing here
			// that can fail, time out, or need retrying.
			Name: "confirm",
			When: func(_ Order, _ Fulfilment, r deck.Result) bool {
				return r.Completed("ship")
			},
			Run: func(o Order, f Fulfilment) (deck.Mutation[Fulfilment], error) {
				outcome := fmt.Sprintf("order %s complete (%s, %s, %s)",
					o.ID, f.Stock, f.Payment, f.Shipment)
				return deck.Complete(func(f *Fulfilment) { f.Outcome = outcome }), nil
			},
		},
	}
}

// FulfilOrder is where deck and Temporal meet. Everything Temporal-specific
// about this flow lives in this function.
func FulfilOrder(ctx workflow.Context, order Order) (Fulfilment, error) {
	// The house rules for every cue's work: half a second, and up to five
	// attempts a tenth of a second apart. ChargePayment leans on this — it
	// fails twice before it works, and nothing in the flow has to care.
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 500 * time.Millisecond,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 5,
			InitialInterval: 100 * time.Millisecond,
		},
	})

	d, err := deck.New(cues()...)
	if err != nil {
		return Fulfilment{}, fmt.Errorf("building deck: %w", err)
	}

	d.Engine = decktemporal.New(ctx,
		// Shipping takes 1.5s, so the house rule above would time it out and
		// then retry it forever. Activity options are Temporal's own type, so
		// naming the cue here keeps the flow itself free of Temporal — and puts
		// the timeout beside the other deployment decisions.
		decktemporal.ForCue("ship", workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
		}),
	)

	var f Fulfilment
	result, err := d.Run(context.Background(), order, &f)
	if err != nil {
		return f, fmt.Errorf("fulfilling order: %w", err)
	}

	// Result carries each cue that completed, in the order it completed, and
	// how long it took. Under Temporal those stamps come from workflow.Now, so
	// they mark workflow task boundaries rather than exact wall-clock time.
	for _, c := range result.CompletedCues {
		f.Timeline = append(f.Timeline, fmt.Sprintf("%-8s %v", c.Name, c.Duration()))
	}
	return f, nil
}

func main() {
	keepUI := flag.Bool("ui", false, "leave the Web UI running when the flow finishes")
	flag.Parse()

	fmt.Println("Fulfilling an order. Watch the payment step:")
	fmt.Println("it fails twice, and Temporal retries it without the flow knowing.")
	fmt.Println()

	srv, stop := devServer()
	defer stop()
	c := srv.Client()

	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterWorkflow(FulfilOrder)
	w.RegisterActivity(ReserveStock)
	w.RegisterActivity(ChargePayment)
	w.RegisterActivity(ShipOrder)
	if err := w.Start(); err != nil {
		log.Fatalf("starting worker: %v", err)
	}
	defer w.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        fmt.Sprintf("order-%d", time.Now().UnixNano()),
		TaskQueue: taskQueue,
	}, FulfilOrder, Order{ID: "A-1001"})
	if err != nil {
		log.Fatalf("starting workflow: %v", err)
	}

	var f Fulfilment
	if err := run.Get(ctx, &f); err != nil {
		log.Fatalf("workflow failed: %v", err)
	}

	fmt.Println()
	fmt.Println(f.Outcome)
	fmt.Println()
	fmt.Println("deck's Result, in completion order:")
	for _, line := range f.Timeline {
		fmt.Println("    " + line)
	}
	fmt.Println()
	fmt.Println("Three things happened there that you would otherwise have written yourself:")
	fmt.Println("  - the payment retried twice and then succeeded, with no retry code in the flow")
	fmt.Println("  - shipping got its own timeout via ForCue, while everything else kept the strict one")
	fmt.Println("  - every attempt is recorded, so the run can be replayed or resumed on another machine")

	if *keepUI {
		fmt.Printf("\nWeb UI: http://localhost:%s — workflow %s\n", uiPort, run.GetID())
		fmt.Println("Press Ctrl-C when you have finished looking.")
		select {}
	}
}

const uiPort = "8233"

// devServer starts a throwaway Temporal server so the example needs no setup.
// In a real service you would dial your own:
//
//	c, err := client.Dial(client.Options{HostPort: "temporal:7233"})
func devServer() (*testsuite.DevServer, func()) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	srv, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ClientOptions: &client.Options{Logger: quietLogger{}},
		EnableUI:      true,
		UIPort:        uiPort,
		LogLevel:      "error",
		Stdout:        io.Discard,
		Stderr:        io.Discard,
	})
	if err != nil {
		log.Fatalf("starting dev server: %v", err)
	}
	return srv, func() { _ = srv.Stop() }
}

// quietLogger keeps the SDK's chatter out of the example's output. Swap it for
// your own logger in a real service.
type quietLogger struct{}

func (quietLogger) Debug(string, ...any) {}
func (quietLogger) Info(string, ...any)  {}
func (quietLogger) Warn(string, ...any)  {}

// Error stays visible: the payment gateway is meant to fail twice here, and
// seeing Temporal notice is half the point. Send these to your own logger in a
// real service.
func (quietLogger) Error(msg string, _ ...any) {
	fmt.Printf("    %-9s %s\n", "temporal", msg)
}
