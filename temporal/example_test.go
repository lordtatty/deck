package temporal_test

import (
	"context"
	"fmt"
	"time"

	"github.com/lordtatty/deck"
	decktemporal "github.com/lordtatty/deck/temporal"
	"go.temporal.io/sdk/workflow"
)

// The wiring the package doc describes, kept compiling. A Deck is built once
// at startup; each workflow gives it an engine of its own with WithEngine.
var flowDeck = func() *deck.Deck[struct{}, countState] {
	d, err := deck.New(echoCues("a", "b")...)
	if err != nil {
		panic(err)
	}
	return d
}()

func ExampleNew() {
	myWorkflow := func(ctx workflow.Context) (countState, error) {
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute,
		})
		d := flowDeck.WithEngine(decktemporal.New(ctx))

		var state countState
		if _, err := d.Run(context.Background(), struct{}{}, &state); err != nil {
			return state, fmt.Errorf("run deck: %w", err)
		}
		return state, nil
	}
	_ = myWorkflow
	// A worker registers myWorkflow and the functions the cues declared with
	// deck.Do — here, Echo — and Temporal resolves them by name.
}
