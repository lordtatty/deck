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
	Keywords []string `json:"keywords"`
	Indexed  string   `json:"indexed"`
}

// The units of work. Ordinary functions taking a context and one argument —
// which is also the shape Temporal wants for an activity, so the same function
// serves both ways of running.

func Fetch(ctx context.Context, url string) (string, error) {
	if err := pause(ctx, 300*time.Millisecond); err != nil {
		return "", err
	}
	return "contents of " + url, nil
}

func Summarise(ctx context.Context, body string) (string, error) {
	if err := pause(ctx, 400*time.Millisecond); err != nil {
		return "", err
	}
	return "summary(" + body + ")", nil
}

func Keywords(ctx context.Context, body string) ([]string, error) {
	if err := pause(ctx, 400*time.Millisecond); err != nil {
		return nil, err
	}
	return strings.Fields(body), nil
}

func pause(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return fmt.Errorf("cancelled: %w", ctx.Err())
	}
}

// Cues is the flow: fetch, then summarise and pull keywords at the same time,
// then index once both are done.
//
//	fetch ──┬── summarise ──┐
//	        └── keywords  ──┴── index
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
			Name: "keywords",
			When: func(_ Input, _ State, r deck.Result) bool { return r.Completed("fetch") },
			Run: func(_ Input, s State) (deck.Mutation[State], error) {
				return deck.Do(Keywords, s.Body, func(kw []string) deck.Mutation[State] {
					return deck.Complete(func(s *State) { s.Keywords = kw })
				}), nil
			},
		},
		{
			Name: "index",
			When: func(_ Input, _ State, r deck.Result) bool {
				return r.Completed("summarise") && r.Completed("keywords")
			},
			Run: func(_ Input, s State) (deck.Mutation[State], error) {
				indexed := fmt.Sprintf("%s [%d keywords]", s.Summary, len(s.Keywords))
				return deck.Complete(func(s *State) { s.Indexed = indexed }), nil
			},
		},
	}
}
