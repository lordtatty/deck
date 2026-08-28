// Package flow holds the work this program does. It is written once, knows
// nothing about how it will be run, and imports deck and nothing else.
package flow

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/lordtatty/deck"
)

type Input struct {
	URL string `json:"url"`
}

type State struct {
	Body     string   `json:"body"`
	Summary  string   `json:"summary"`
	Category string   `json:"category"`
	Keywords []string `json:"keywords"`
	Indexed  string   `json:"indexed"`
}

// The units of work: fetching a page, asking a model to summarise it, and
// asking a classifier service what it is about. All three leave the process,
// which is what makes them worth declaring.
//
// They are ordinary functions taking a context and one argument — which is also
// the shape Temporal wants for an activity, so the same function serves both
// ways of running.

func Fetch(ctx context.Context, url string) (string, error) {
	if err := pause(ctx, 300*time.Millisecond); err != nil {
		return "", err
	}
	return "quick brown fox", nil
}

func Summarise(ctx context.Context, body string) (string, error) {
	if err := pause(ctx, 400*time.Millisecond); err != nil {
		return "", err
	}
	return "about " + body, nil
}

func Classify(ctx context.Context, body string) (string, error) {
	if err := pause(ctx, 400*time.Millisecond); err != nil {
		return "", err
	}
	return "wildlife", nil
}

func pause(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return fmt.Errorf("cancelled: %w", ctx.Err())
	}
}

// Cues is the flow:
//
//	fetch ──┬── summarise  (Do: a model, elsewhere)
//	        ├── classify   (Do: a service, elsewhere)
//	        └── keywords   (Complete: splitting a string, right here)
//	                             │
//	                           index  (Complete: assembling the three)
//
// Five cues, three of which declare work. That ratio is the normal shape, and
// the middle three make the distinction plain: all three trigger at the same
// moment, but only the two that go somewhere become units of work for the
// Engine. Splitting a string we already hold is not worth handing to anyone —
// under Temporal it would buy a round trip to the server and an entry in the
// workflow history to run strings.Fields.
func Cues() []deck.Cue[Input, State] {
	return []deck.Cue[Input, State]{
		{
			Name: "fetch",
			Run: func(in Input, _ State) (deck.Mutation[State], error) {
				return deck.Do(Fetch, in.URL, func(body string) deck.Mutation[State] {
					return deck.Complete(func(s *State) { s.Body = body })
				}), nil
			},
		},
		{
			Name: "summarise",
			When: func(_ Input, _ State, r deck.Result) bool { return r.Completed("fetch") },
			Run: func(_ Input, s State) (deck.Mutation[State], error) {
				return deck.Do(Summarise, s.Body, func(sum string) deck.Mutation[State] {
					return deck.Complete(func(s *State) { s.Summary = sum })
				}), nil
			},
		},
		{
			Name: "classify",
			When: func(_ Input, _ State, r deck.Result) bool { return r.Completed("fetch") },
			Run: func(_ Input, s State) (deck.Mutation[State], error) {
				return deck.Do(Classify, s.Body, func(cat string) deck.Mutation[State] {
					return deck.Complete(func(s *State) { s.Category = cat })
				}), nil
			},
		},
		{
			// Triggers at the same moment as summarise and classify, but there
			// is nothing here to send anywhere: the body is already in state,
			// and splitting it is instant. So it returns Complete and finishes
			// before the other two have started their work.
			Name: "keywords",
			When: func(_ Input, _ State, r deck.Result) bool { return r.Completed("fetch") },
			Run: func(_ Input, s State) (deck.Mutation[State], error) {
				words := strings.Fields(s.Body)
				return deck.Complete(func(s *State) { s.Keywords = words }), nil
			},
		},
		{
			// Likewise: assembling a sentence from state we hold.
			Name: "index",
			When: func(_ Input, _ State, r deck.Result) bool {
				return r.Completed("summarise") && r.Completed("classify") && r.Completed("keywords")
			},
			Run: func(_ Input, s State) (deck.Mutation[State], error) {
				indexed := fmt.Sprintf("[%s] %s (%d keywords)", s.Category, s.Summary, len(s.Keywords))
				return deck.Complete(func(s *State) { s.Indexed = indexed }), nil
			},
		},
	}
}
