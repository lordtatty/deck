package deck_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lordtatty/deck"
	"github.com/stretchr/testify/assert"
)

type TestState struct {
	mu              sync.Mutex
	Count           int
	CompletionTimes map[string]time.Time
}

func (s *TestState) Inc() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Count++
}

func (s *TestState) GetCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Count
}

func (s *TestState) RecordCompletion(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.CompletionTimes == nil {
		s.CompletionTimes = make(map[string]time.Time)
	}
	s.CompletionTimes[name] = time.Now()
}

func (s *TestState) GetCompletionTime(name string) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.CompletionTimes[name]
	return t, ok
}

func TestDeck_Run_HappyPath(t *testing.T) {
	// Arrange
	state := &TestState{Count: 1}

	cue := deck.Cue[TestState]{
		When: func(s *TestState) bool {
			return s.GetCount() == 1
		},
		Run: func(s *TestState) error {
			s.Inc()
			return nil
		},
	}

	sut := deck.New(state, cue)

	// Act
	// Run for a short duration to allow the loop to execute
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := sut.Run(ctx)

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
		When: func(s *TestState) bool {
			return s.GetCount() == 0
		},
		Run: func(s *TestState) error {
			s.Inc()
			state.RecordCompletion("cue1")
			// Ensure some time passes so timestamps are distinct
			time.Sleep(1 * time.Millisecond)
			return nil
		},
	}

	// Cue 2: 1 -> 2
	cue2 := deck.Cue[TestState]{
		When: func(s *TestState) bool {
			return s.GetCount() == 1
		},
		Run: func(s *TestState) error {
			s.Inc()
			state.RecordCompletion("cue2")
			return nil
		},
	}

	sut := deck.New(state, cue1, cue2)

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := sut.Run(ctx)

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
		When: func(s *TestState) bool {
			return true
		},
		Run: func(s *TestState) error {
			time.Sleep(200 * time.Millisecond)
			return nil
		},
	}

	sut := deck.New(state, cue)

	// Act
	ctx, cancel := context.WithCancel(context.Background())

	// Start Run in a goroutine
	errChan := make(chan error)
	go func() {
		errChan <- sut.Run(ctx)
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
		When: func(s *TestState) bool {
			return true
		},
		Run: func(s *TestState) error {
			s.Inc()
			return nil
		},
	}

	sut := deck.New(state, cue)

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := sut.Run(ctx)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 1, state.GetCount(), "Cue should run exactly once")
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
