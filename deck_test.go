package deck_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lordtatty/deck"
	"github.com/stretchr/testify/assert"
)

// TestState is now unsafe (no mutex) to demonstrate race conditions
type TestState struct {
	Count           int
	CompletionTimes map[string]time.Time
	Buffer          []rune
}

func (s *TestState) Inc() {
	s.Count++
}

func (s *TestState) GetCount() int {
	return s.Count
}

func (s *TestState) RecordCompletion(name string) {
	if s.CompletionTimes == nil {
		s.CompletionTimes = make(map[string]time.Time)
	}
	s.CompletionTimes[name] = time.Now()
}

func (s *TestState) GetCompletionTime(name string) (time.Time, bool) {
	t, ok := s.CompletionTimes[name]
	return t, ok
}

func (s *TestState) Append(r rune) {
	// Simulate slow write to increase race window
	time.Sleep(10 * time.Microsecond)
	s.Buffer = append(s.Buffer, r)
}

func TestDeck_Run_HappyPath(t *testing.T) {
	// Arrange
	state := &TestState{Count: 1}

	cue := deck.Cue[TestState]{
		Name: "HappyPath",
		When: func(s *TestState) bool {
			return s.GetCount() == 1
		},
		Run: func(s *TestState) (func(*TestState), error) {
			return func(s *TestState) {
				s.Inc()
			}, nil
		},
	}

	sut, err := deck.New(cue)
	assert.NoError(t, err)

	// Act
	// Run for a short duration to allow the loop to execute
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err = sut.Run(ctx, state)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 2, state.GetCount(), "Count should be incremented to 2")
}

func TestDeck_Run_ChainReaction(t *testing.T) {
	// Arrange
	state := &TestState{
		Count:           0,
		CompletionTimes: make(map[string]time.Time),
	}

	// Cue 1: 0 -> 1
	cue1 := deck.Cue[TestState]{
		Name: "cue1",
		When: func(s *TestState) bool {
			return s.GetCount() == 0
		},
		Run: func(s *TestState) (func(*TestState), error) {
			// Ensure some time passes so timestamps are distinct
			time.Sleep(1 * time.Millisecond)
			return func(s *TestState) {
				s.Inc()
				state.RecordCompletion("cue1")
			}, nil
		},
	}

	// Cue 2: 1 -> 2
	cue2 := deck.Cue[TestState]{
		Name: "cue2",
		When: func(s *TestState) bool {
			return s.GetCount() == 1
		},
		Run: func(s *TestState) (func(*TestState), error) {
			return func(s *TestState) {
				s.Inc()
				state.RecordCompletion("cue2")
			}, nil
		},
	}

	sut, err := deck.New(cue1, cue2)
	assert.NoError(t, err)

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = sut.Run(ctx, state)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 2, state.GetCount(), "Count should be incremented to 2 via chain reaction")

	assertExecutionOrder(t, state, "cue1", "cue2")
}

func TestDeck_Run_Cancellation(t *testing.T) {
	// Arrange
	state := &TestState{Count: 0}

	// Add a cue that sleeps for a long time
	cue := deck.Cue[TestState]{
		Name: "SleepyCue",
		When: func(s *TestState) bool {
			return true
		},
		Run: func(s *TestState) (func(*TestState), error) {
			time.Sleep(200 * time.Millisecond)
			return nil, nil
		},
	}

	sut, err := deck.New(cue)
	assert.NoError(t, err)

	// Act
	ctx, cancel := context.WithCancel(context.Background())

	// Start Run in a goroutine
	errChan := make(chan error)
	go func() {
		errChan <- sut.Run(ctx, state)
	}()

	// Cancel shortly after
	time.Sleep(10 * time.Millisecond)
	cancel()

	// Assert
	select {
	case err := <-errChan:
		assert.ErrorIs(t, err, context.Canceled, "Run should return context.Canceled error")
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "Run did not return after cancellation")
	}
}

func TestDeck_Run_SingleExecution(t *testing.T) {
	// Arrange
	state := &TestState{Count: 0}

	// Add a cue that is always true
	cue := deck.Cue[TestState]{
		Name: "OneShot",
		When: func(s *TestState) bool {
			return true
		},
		Run: func(s *TestState) (func(*TestState), error) {
			return func(s *TestState) {
				s.Inc()
			}, nil
		},
	}

	sut, err := deck.New(cue)
	assert.NoError(t, err)

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err = sut.Run(ctx, state)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 1, state.GetCount(), "Cue should run exactly once")
}

func TestDeck_Run_Concurrency_Race(t *testing.T) {
	// Arrange
	state := &TestState{Buffer: make([]rune, 0)}

	// Create 100 cues that all stream the alphabet concurrently
	count := 100
	alphabet := "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

	// Construct expected buffer: "ABC...Z" repeated count times
	expected := bytes.Repeat([]byte(alphabet), count)

	cues := make([]deck.Cue[TestState], count)
	for i := range count {
		cues[i] = deck.Cue[TestState]{
			Name: fmt.Sprintf("Cue-%d", i),
			When: func(s *TestState) bool {
				return true
			},
			Run: func(s *TestState) (func(*TestState), error) {
				// Return mutation function that streams the alphabet
				return func(s *TestState) {
					for _, r := range alphabet {
						s.Append(r)
					}
				}, nil
			},
		}
	}

	sut, err := deck.New(cues...)
	assert.NoError(t, err)

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = sut.Run(ctx, state)

	// Assert
	assert.NoError(t, err)
	// If race conditions occur, Buffer will be corrupted (missing chars, wrong order)
	assert.Equal(t, string(expected), string(state.Buffer), "Buffer content should match expected sequence if updates are safe")
}

func TestDeck_New_DuplicateNames(t *testing.T) {
	// Arrange
	cue1 := deck.Cue[TestState]{
		Name: "Duplicate",
		When: func(s *TestState) bool { return true },
		Run:  func(s *TestState) (func(*TestState), error) { return nil, nil },
	}
	cue2 := deck.Cue[TestState]{
		Name: "Duplicate",
		When: func(s *TestState) bool { return true },
		Run:  func(s *TestState) (func(*TestState), error) { return nil, nil },
	}

	// Act
	_, err := deck.New(cue1, cue2)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate cue name: Duplicate")
}

func TestDeck_New_EmptyName(t *testing.T) {
	// Arrange
	cue := deck.Cue[TestState]{
		Name: "",
		When: func(s *TestState) bool { return true },
		Run:  func(s *TestState) (func(*TestState), error) { return nil, nil },
	}

	// Act
	_, err := deck.New(cue)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cue name cannot be empty")
}

func assertExecutionOrder(t *testing.T, state *TestState, order ...string) {
	t.Helper()
	for i := 0; i < len(order)-1; i++ {
		currKey := order[i]
		nextKey := order[i+1]

		currTime, ok1 := state.GetCompletionTime(currKey)
		nextTime, ok2 := state.GetCompletionTime(nextKey)

		if assert.True(t, ok1, "Cue %s should have completed", currKey) &&
			assert.True(t, ok2, "Cue %s should have completed", nextKey) {
			assert.True(t, currTime.Before(nextTime), "Cue %s should complete before %s", currKey, nextKey)
		}
	}
}
