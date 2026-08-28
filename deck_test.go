package deck_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lordtatty/deck"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestState is now unsafe (no mutex) to demonstrate race conditions
type TestState struct {
	Count  int
	Buffer []rune
}

func (s *TestState) Inc() {
	s.Count++
}

func (s *TestState) GetCount() int {
	return s.Count
}

func (s *TestState) Append(r rune) {
	// Simulate slow write to increase race window
	time.Sleep(10 * time.Microsecond)
	s.Buffer = append(s.Buffer, r)
}

func TestDeck_Run_HappyPath(t *testing.T) {
	// Arrange
	state := &TestState{Count: 1}

	cue := deck.Cue[struct{}, TestState]{
		Name: "HappyPath",
		When: func(_ struct{}, s TestState, r deck.Result) bool {
			return s.GetCount() == 1
		},
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) {
				s.Inc()
			}), nil
		},
	}

	sut, err := deck.New(cue)
	require.NoError(t, err)

	// Act
	// Run for a short duration to allow the loop to execute
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	result, err := sut.Run(ctx, struct{}{}, state)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 2, state.GetCount(), "Count should be incremented to 2")
	assert.Equal(t, "HappyPath", result.CompletedCues[0].Name)
}

func TestDeck_Run_ChainReaction(t *testing.T) {
	// Arrange
	state := &TestState{
		Count: 0,
	}

	// Cue 1: 0 -> 1
	cue1 := deck.Cue[struct{}, TestState]{
		Name: "cue1",
		When: func(_ struct{}, s TestState, r deck.Result) bool {
			return s.GetCount() == 0
		},
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			// Ensure some time passes so timestamps are distinct
			time.Sleep(1 * time.Millisecond)
			return deck.Complete(func(s *TestState) {
				s.Inc()
			}), nil
		},
	}

	// Cue 2: 1 -> 2
	cue2 := deck.Cue[struct{}, TestState]{
		Name: "cue2",
		When: func(_ struct{}, s TestState, r deck.Result) bool {
			return s.GetCount() == 1
		},
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) {
				s.Inc()
			}), nil
		},
	}

	sut, err := deck.New(cue1, cue2)
	require.NoError(t, err)

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	result, err := sut.Run(ctx, struct{}{}, state)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 2, state.GetCount(), "Count should be incremented to 2 via chain reaction")

	assertExecutionOrder(t, result, "cue1", "cue2")

	// Verify names in result
	var names []string
	for _, c := range result.CompletedCues {
		names = append(names, c.Name)
	}
	assert.Equal(t, []string{"cue1", "cue2"}, names)
}

func TestDeck_Run_Cancellation(t *testing.T) {
	// Arrange
	state := &TestState{Count: 0}

	// Add a cue that sleeps for a long time
	cue := deck.Cue[struct{}, TestState]{
		Name: "SleepyCue",
		When: func(_ struct{}, s TestState, r deck.Result) bool {
			return true
		},
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			time.Sleep(200 * time.Millisecond)
			return nil, nil
		},
	}

	sut, err := deck.New(cue)
	require.NoError(t, err)

	// Act
	ctx, cancel := context.WithCancel(context.Background())

	// Start Run in a goroutine
	errChan := make(chan error)
	go func() {
		_, err := sut.Run(ctx, struct{}{}, state)
		errChan <- err
	}()

	// Cancel shortly after
	time.Sleep(10 * time.Millisecond)
	cancel()

	// Assert
	select {
	case err := <-errChan:
		require.ErrorIs(t, err, context.Canceled, "Run should return context.Canceled error")
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "Run did not return after cancellation")
	}
}

func TestDeck_Run_SingleExecution(t *testing.T) {
	// Arrange
	state := &TestState{Count: 0}

	// Add a cue that is always true
	cue := deck.Cue[struct{}, TestState]{
		Name: "OneShot",
		When: func(_ struct{}, s TestState, r deck.Result) bool {
			return true
		},
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) {
				s.Inc()
			}), nil
		},
	}

	sut, err := deck.New(cue)
	require.NoError(t, err)

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	result, err := sut.Run(ctx, struct{}{}, state)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 1, state.GetCount(), "Cue should run exactly once")
	assert.Equal(t, "OneShot", result.CompletedCues[0].Name)
}

func TestDeck_Run_Concurrency_Race(t *testing.T) {
	// Arrange
	state := &TestState{Buffer: make([]rune, 0)}

	// Create 100 cues that all stream the alphabet concurrently
	count := 100
	alphabet := "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

	// Construct expected buffer: "ABC...Z" repeated count times
	expected := bytes.Repeat([]byte(alphabet), count)

	cues := make([]deck.Cue[struct{}, TestState], count)
	for i := range count {
		cues[i] = deck.Cue[struct{}, TestState]{
			Name: fmt.Sprintf("Cue-%d", i),
			When: func(_ struct{}, s TestState, r deck.Result) bool {
				return true
			},
			Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
				// Return mutation function that streams the alphabet
				return deck.Complete(func(s *TestState) {
					for _, r := range alphabet {
						s.Append(r)
					}
				}), nil
			},
		}
	}

	sut, err := deck.New(cues...)
	require.NoError(t, err)

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := sut.Run(ctx, struct{}{}, state)

	// Assert
	require.NoError(t, err)
	// If race conditions occur, Buffer will be corrupted (missing chars, wrong order)
	assert.Equal(t, string(expected), string(state.Buffer), "Buffer content should match expected sequence if updates are safe")
	assert.Len(t, result.CompletedCues, count)
}

func TestDeck_New_DuplicateNames(t *testing.T) {
	// Arrange
	cue1 := deck.Cue[struct{}, TestState]{
		Name: "Duplicate",
		When: func(_ struct{}, s TestState, r deck.Result) bool { return true },
		Run:  func(_ struct{}, s TestState) (deck.Mutation[TestState], error) { return nil, nil },
	}
	cue2 := deck.Cue[struct{}, TestState]{
		Name: "Duplicate",
		When: func(_ struct{}, s TestState, r deck.Result) bool { return true },
		Run:  func(_ struct{}, s TestState) (deck.Mutation[TestState], error) { return nil, nil },
	}

	// Act
	_, err := deck.New(cue1, cue2)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate cue name: Duplicate")
}

func TestDeck_New_EmptyName(t *testing.T) {
	// Arrange
	cue := deck.Cue[struct{}, TestState]{
		Name: "",
		When: func(_ struct{}, s TestState, r deck.Result) bool { return true },
		Run:  func(_ struct{}, s TestState) (deck.Mutation[TestState], error) { return nil, nil },
	}

	// Act
	_, err := deck.New(cue)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cue name cannot be empty")
}

func TestDeck_New_NilRun(t *testing.T) {
	// Arrange
	cue := deck.Cue[struct{}, TestState]{
		Name: "NilRunCue",
		When: nil,
		Run:  nil,
	}

	// Act
	_, err := deck.New(cue)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cue run cannot be nil: NilRunCue")
}

func TestDeck_Run_ReturnsCompletedCues(t *testing.T) {
	// Arrange
	state := &TestState{Count: 0}

	cue1 := deck.Cue[struct{}, TestState]{
		Name: "CueA",
		When: func(_ struct{}, s TestState, r deck.Result) bool { return s.Count == 0 },
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			time.Sleep(10 * time.Millisecond) // Simulate work
			return deck.Complete(func(s *TestState) { s.Inc() }), nil
		},
	}

	cue2 := deck.Cue[struct{}, TestState]{
		Name: "CueB",
		When: func(_ struct{}, s TestState, r deck.Result) bool { return s.Count == 1 },
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			time.Sleep(20 * time.Millisecond) // Simulate work
			return deck.Complete(func(s *TestState) { s.Inc() }), nil
		},
	}

	sut, err := deck.New(cue1, cue2)
	require.NoError(t, err)

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	result, err := sut.Run(ctx, struct{}{}, state)

	// Assert
	require.NoError(t, err)

	// Verify names
	var names []string
	for _, c := range result.CompletedCues {
		names = append(names, c.Name)
	}
	assert.ElementsMatch(t, []string{"CueA", "CueB"}, names)

	// Verify timings
	for _, c := range result.CompletedCues {
		assert.False(t, c.StartTime.IsZero(), "StartTime should be set")
		assert.False(t, c.EndTime.IsZero(), "EndTime should be set")
		assert.True(t, c.EndTime.After(c.StartTime), "EndTime should be after StartTime")
		assert.Positive(t, c.Duration(), "Duration should be positive")

		if c.Name == "CueA" {
			assert.GreaterOrEqual(t, c.Duration(), 10*time.Millisecond, "CueA duration should be at least 10ms")
		}
		if c.Name == "CueB" {
			assert.GreaterOrEqual(t, c.Duration(), 20*time.Millisecond, "CueB duration should be at least 20ms")
		}
	}
}

func TestDeck_Run_TriggerOnHistory(t *testing.T) {
	// Arrange
	state := &TestState{Count: 0}

	cue1 := deck.Cue[struct{}, TestState]{
		Name: "CueA",
		When: func(_ struct{}, s TestState, r deck.Result) bool { return s.Count == 0 },
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) { s.Inc() }), nil
		},
	}

	cue2 := deck.Cue[struct{}, TestState]{
		Name: "CueB",
		When: func(_ struct{}, s TestState, r deck.Result) bool {
			// Trigger only if CueA has completed
			return r.Completed("CueA")
		},
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) { s.Inc() }), nil
		},
	}

	sut, err := deck.New(cue1, cue2)
	require.NoError(t, err)

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	result, err := sut.Run(ctx, struct{}{}, state)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 2, state.Count)
	assertExecutionOrder(t, result, "CueA", "CueB")
}

func TestDeck_StateImmutability(t *testing.T) {
	// Arrange
	state := &TestState{Count: 0}

	cue := deck.Cue[struct{}, TestState]{
		Name: "BadActor",
		When: func(_ struct{}, s TestState, r deck.Result) bool {
			// Attempt to modify state in When (should be a copy)
			s.Count = 999
			return true
		},
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			// Attempt to modify state in Run (should be a copy)
			s.Count = 888
			return deck.Complete(func(s *TestState) {
				// Only this mutation should affect the real state
				s.Inc()
			}), nil
		},
	}

	sut, err := deck.New(cue)
	require.NoError(t, err)

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err = sut.Run(ctx, struct{}{}, state)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 1, state.Count, "State should only be modified by the mutation function")
}

func TestDeck_Run_ExternalModification(t *testing.T) {
	// Arrange
	state := &TestState{Count: 0}

	started := make(chan struct{})
	continueChan := make(chan struct{})

	// Cue that signals start, waits, then increments
	cue := deck.Cue[struct{}, TestState]{
		Name: "CoordinatedCue",
		When: func(_ struct{}, s TestState, r deck.Result) bool {
			return s.Count == 0
		},
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			close(started)
			<-continueChan
			return deck.Complete(func(s *TestState) {
				s.Inc()
			}), nil
		},
	}

	sut, err := deck.New(cue)
	require.NoError(t, err)

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Start Run in background
	errChan := make(chan error)
	go func() {
		_, err := sut.Run(ctx, struct{}{}, state)
		errChan <- err
	}()

	// Wait for cue to start (implies Run has copied state)
	<-started

	// Modify state externally.
	// Since Run has already read state and is blocked on continueChan,
	// and won't write back until after continueChan is closed,
	// this write is safe if we ensure happens-before.
	state.Count = 100

	// Signal cue to continue
	close(continueChan)

	// Wait for Run to complete
	err = <-errChan
	require.NoError(t, err)

	// Assert
	// The cue logic (When: s.Count == 0) used the initial state.
	// The mutation (0 -> 1) is applied to the isolated state.
	// The final isolated state (Count: 1) is copied back, overwriting the 100.
	assert.Equal(t, 1, state.Count, "External modification should be overwritten by isolated run result")
}

func TestDeck_Run_NilWhen(t *testing.T) {
	// Arrange
	state := &TestState{Count: 0}

	cue := deck.Cue[struct{}, TestState]{
		Name: "AlwaysRun",
		When: nil, // Should default to true
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) {
				s.Inc()
			}), nil
		},
	}

	sut, err := deck.New(cue)
	require.NoError(t, err)

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	result, err := sut.Run(ctx, struct{}{}, state)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 1, state.Count, "Cue with nil When should run")
	assert.Equal(t, "AlwaysRun", result.CompletedCues[0].Name)
}

func TestDeck_Run_Suspend_MutationAppliedButNotCompleted(t *testing.T) {
	// Given a cue that returns a Suspended mutation
	state := &TestState{Count: 0}

	cue := deck.Cue[struct{}, TestState]{
		Name: "SuspendingCue",
		When: func(_ struct{}, s TestState, r deck.Result) bool {
			return s.Count == 0
		},
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			return deck.Suspended(func(s *TestState) {
				s.Count = 42
			}), nil
		},
	}

	sut, err := deck.New(cue)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// When the deck runs
	result, err := sut.Run(ctx, struct{}{}, state)

	// Then the mutation is applied
	require.NoError(t, err)
	assert.Equal(t, 42, state.Count, "Suspended mutation should still be applied to state")

	// And the cue is NOT in CompletedCues
	assert.False(t, result.Completed("SuspendingCue"), "Suspended cue should not appear in completed cues")

	// And the result indicates suspension
	assert.True(t, result.Suspended, "Result should indicate the deck was suspended")
}

func TestDeck_Run_Suspend_OtherCuesDrainBeforeReturning(t *testing.T) {
	// Given a cue that suspends and another cue that runs normally
	state := &TestState{Count: 0}

	suspendCue := deck.Cue[struct{}, TestState]{
		Name: "Suspender",
		When: func(_ struct{}, s TestState, r deck.Result) bool {
			return true
		},
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			return deck.Suspended(func(s *TestState) {
				s.Count += 10
			}), nil
		},
	}

	normalCue := deck.Cue[struct{}, TestState]{
		Name: "Normal",
		When: func(_ struct{}, s TestState, r deck.Result) bool {
			return true
		},
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			time.Sleep(20 * time.Millisecond) // takes a bit longer
			return deck.Complete(func(s *TestState) {
				s.Count += 1
			}), nil
		},
	}

	sut, err := deck.New(suspendCue, normalCue)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// When the deck runs
	result, err := sut.Run(ctx, struct{}{}, state)

	// Then both mutations are applied
	require.NoError(t, err)
	assert.Equal(t, 11, state.Count, "Both mutations should be applied")

	// And the normal cue completed
	assert.True(t, result.Completed("Normal"), "Normal cue should be completed")

	// And the suspending cue did not complete
	assert.False(t, result.Completed("Suspender"), "Suspender should not be completed")

	// And the result indicates suspension
	assert.True(t, result.Suspended)
}

func TestDeck_Resume_SkipsPreviouslyCompletedCues(t *testing.T) {
	// Given a deck where CueA has already completed
	state := &TestState{Count: 1}

	cueA := deck.Cue[struct{}, TestState]{
		Name: "CueA",
		When: func(_ struct{}, s TestState, r deck.Result) bool {
			return true // would fire if not already completed
		},
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) {
				s.Count += 100 // should NOT happen on resume
			}), nil
		},
	}

	cueB := deck.Cue[struct{}, TestState]{
		Name: "CueB",
		When: func(_ struct{}, s TestState, r deck.Result) bool {
			return r.Completed("CueA")
		},
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) {
				s.Count += 1
			}), nil
		},
	}

	sut, err := deck.New(cueA, cueB)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// When we resume with CueA already completed
	prev := deck.Result{
		CompletedCues: []deck.CompletedCue{{Name: "CueA"}},
	}
	result, err := sut.Run(ctx, struct{}{}, state, prev)

	// Then CueA does not re-run (count would be 101+ if it did)
	require.NoError(t, err)
	assert.Equal(t, 2, state.Count, "Only CueB should have run")

	// And both cues appear in completed
	assert.True(t, result.Completed("CueA"), "CueA should still be in completed")
	assert.True(t, result.Completed("CueB"), "CueB should be in completed")
}

func TestDeck_Resume_SuspendAndResumeTwoCuePattern(t *testing.T) {
	// Given two cues: one to submit a batch, one to check the result
	type BatchState struct {
		BatchID string
		Result  string
	}

	submitCue := deck.Cue[struct{}, BatchState]{
		Name: "SubmitBatch",
		When: func(_ struct{}, s BatchState, r deck.Result) bool {
			return s.BatchID == ""
		},
		Run: func(_ struct{}, s BatchState) (deck.Mutation[BatchState], error) {
			return deck.Suspended(func(s *BatchState) {
				s.BatchID = "batch-123"
			}), nil
		},
	}

	checkCue := deck.Cue[struct{}, BatchState]{
		Name: "CheckBatch",
		When: func(_ struct{}, s BatchState, r deck.Result) bool {
			return s.BatchID != "" && s.Result == ""
		},
		Run: func(_ struct{}, s BatchState) (deck.Mutation[BatchState], error) {
			return deck.Complete(func(s *BatchState) {
				s.Result = "done"
			}), nil
		},
	}

	sut, err := deck.New(submitCue, checkCue)
	require.NoError(t, err)

	ctx := context.Background()

	// First run: SubmitBatch fires and suspends
	state := &BatchState{}
	result1, err := sut.Run(ctx, struct{}{}, state)
	require.NoError(t, err)
	assert.True(t, result1.Suspended)
	assert.Equal(t, "batch-123", state.BatchID)
	assert.Empty(t, state.Result)
	// SubmitBatch should not be in completed (it suspended)
	assert.False(t, result1.Completed("SubmitBatch"))

	// Resume: SubmitBatch won't fire (BatchID != ""), CheckBatch fires
	result2, err := sut.Run(ctx, struct{}{}, state, result1)
	require.NoError(t, err)
	assert.False(t, result2.Suspended)
	assert.Equal(t, "done", state.Result)
	assert.True(t, result2.Completed("CheckBatch"))
}

func TestDeck_Resume_FullLifecycleWithMultipleCues(t *testing.T) {
	// Given a deck with three cues: one completes normally, one suspends, one depends on the suspended work
	type WorkState struct {
		SetupDone bool
		BatchID   string
		Result    string
	}

	setupCue := deck.Cue[struct{}, WorkState]{
		Name: "Setup",
		When: func(_ struct{}, s WorkState, r deck.Result) bool {
			return !s.SetupDone
		},
		Run: func(_ struct{}, s WorkState) (deck.Mutation[WorkState], error) {
			return deck.Complete(func(s *WorkState) {
				s.SetupDone = true
			}), nil
		},
	}

	batchCue := deck.Cue[struct{}, WorkState]{
		Name: "SubmitBatch",
		When: func(_ struct{}, s WorkState, r deck.Result) bool {
			return s.SetupDone && s.BatchID == ""
		},
		Run: func(_ struct{}, s WorkState) (deck.Mutation[WorkState], error) {
			return deck.Suspended(func(s *WorkState) {
				s.BatchID = "batch-456"
			}), nil
		},
	}

	collectCue := deck.Cue[struct{}, WorkState]{
		Name: "CollectResult",
		When: func(_ struct{}, s WorkState, r deck.Result) bool {
			return s.BatchID != "" && s.Result == ""
		},
		Run: func(_ struct{}, s WorkState) (deck.Mutation[WorkState], error) {
			return deck.Complete(func(s *WorkState) {
				s.Result = "collected"
			}), nil
		},
	}

	sut, err := deck.New(setupCue, batchCue, collectCue)
	require.NoError(t, err)

	ctx := context.Background()

	// First run: Setup completes, SubmitBatch fires and suspends
	state := &WorkState{}
	result1, err := sut.Run(ctx, struct{}{}, state)
	require.NoError(t, err)
	assert.True(t, result1.Suspended)
	assert.True(t, state.SetupDone)
	assert.Equal(t, "batch-456", state.BatchID)
	assert.True(t, result1.Completed("Setup"), "Setup should be completed")
	assert.False(t, result1.Completed("SubmitBatch"), "SubmitBatch suspended, not completed")

	// Resume: Setup already completed (skipped), SubmitBatch won't match (BatchID set),
	// CollectResult fires
	result2, err := sut.Run(ctx, struct{}{}, state, result1)
	require.NoError(t, err)
	assert.False(t, result2.Suspended)
	assert.Equal(t, "collected", state.Result)
	assert.True(t, result2.Completed("Setup"), "Setup should carry over from previous result")
	assert.True(t, result2.Completed("CollectResult"), "CollectResult should be completed")
}

func TestDeck_Run_Suspend_NonSuspendedMutationWorksAsNormal(t *testing.T) {
	// Given a cue that returns a normal (non-suspended) mutation
	state := &TestState{Count: 0}

	cue := deck.Cue[struct{}, TestState]{
		Name: "NormalCue",
		When: func(_ struct{}, s TestState, r deck.Result) bool {
			return true
		},
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) {
				s.Count = 5
			}), nil
		},
	}

	sut, err := deck.New(cue)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// When the deck runs
	result, err := sut.Run(ctx, struct{}{}, state)

	// Then it completes normally with no suspension
	require.NoError(t, err)
	assert.Equal(t, 5, state.Count)
	assert.True(t, result.Completed("NormalCue"))
	assert.False(t, result.Suspended, "Result should not be suspended for normal cues")
}

func TestDeck_Export_ProducesValidJSON(t *testing.T) {
	// Given a deck that suspends with some state
	type JobState struct {
		BatchID string `json:"batch_id"`
		Result  string `json:"result"`
	}

	cue := deck.Cue[struct{}, JobState]{
		Name: "Submit",
		When: func(_ struct{}, s JobState, r deck.Result) bool { return s.BatchID == "" },
		Run: func(_ struct{}, s JobState) (deck.Mutation[JobState], error) {
			return deck.Suspended(func(s *JobState) {
				s.BatchID = "batch-789"
			}), nil
		},
	}

	d, err := deck.New(cue)
	require.NoError(t, err)

	state := &JobState{}
	result, err := d.Run(context.Background(), struct{}{}, state)
	require.NoError(t, err)
	assert.True(t, result.Suspended)

	// When we export
	data, err := d.Export(state, result)

	// Then it succeeds and produces valid JSON
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// And the JSON contains the state and result
	assert.Contains(t, string(data), `"batch_id":"batch-789"`)
	assert.Contains(t, string(data), `"suspended":true`)
}

func TestDeck_Import_RoundTrip(t *testing.T) {
	// Given a deck with submit and collect cues
	type JobState struct {
		BatchID string `json:"batch_id"`
		Result  string `json:"result"`
	}

	submitCue := deck.Cue[struct{}, JobState]{
		Name: "Submit",
		When: func(_ struct{}, s JobState, r deck.Result) bool { return s.BatchID == "" },
		Run: func(_ struct{}, s JobState) (deck.Mutation[JobState], error) {
			return deck.Suspended(func(s *JobState) {
				s.BatchID = "batch-abc"
			}), nil
		},
	}

	collectCue := deck.Cue[struct{}, JobState]{
		Name: "Collect",
		When: func(_ struct{}, s JobState, r deck.Result) bool {
			return s.BatchID != "" && s.Result == ""
		},
		Run: func(_ struct{}, s JobState) (deck.Mutation[JobState], error) {
			return deck.Complete(func(s *JobState) {
				s.Result = "collected"
			}), nil
		},
	}

	d, err := deck.New(submitCue, collectCue)
	require.NoError(t, err)

	// First run: suspends after submit
	state := &JobState{}
	result1, err := d.Run(context.Background(), struct{}{}, state)
	require.NoError(t, err)
	assert.True(t, result1.Suspended)
	assert.Equal(t, "batch-abc", state.BatchID)

	// Export the state
	data, err := d.Export(state, result1)
	require.NoError(t, err)

	// Import restores state and previous result
	importedState, prev, err := d.Import(data)
	require.NoError(t, err)
	assert.Equal(t, "batch-abc", importedState.BatchID)
	assert.True(t, prev.Suspended)

	// When we resume using the imported data
	result2, err := d.Run(context.Background(), struct{}{}, importedState, prev)

	// Then it completes successfully
	require.NoError(t, err)
	assert.False(t, result2.Suspended)
	assert.Equal(t, "batch-abc", importedState.BatchID)
	assert.Equal(t, "collected", importedState.Result)
	assert.True(t, result2.Completed("Collect"))
}

func TestDeck_Import_InvalidJSON(t *testing.T) {
	// Given a deck
	cue := deck.Cue[struct{}, TestState]{
		Name: "Cue",
		When: func(_ struct{}, s TestState, r deck.Result) bool { return true },
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) {}), nil
		},
	}

	d, err := deck.New(cue)
	require.NoError(t, err)

	// When we try to import invalid data
	_, _, err = d.Import([]byte("not json"))

	// Then it returns an error
	require.Error(t, err)
}

func TestDeck_Import_FullLifecycle(t *testing.T) {
	// Given a deck with setup, two concurrent suspenders, and a final assembly cue
	type PipelineState struct {
		Ready    bool   `json:"ready"`
		ImageID  string `json:"image_id"`
		ImageURL string `json:"image_url"`
		CopyID   string `json:"copy_id"`
		CopyText string `json:"copy_text"`
		Output   string `json:"output"`
	}

	setupCue := deck.Cue[struct{}, PipelineState]{
		Name: "Setup",
		When: func(_ struct{}, s PipelineState, r deck.Result) bool { return !s.Ready },
		Run: func(_ struct{}, s PipelineState) (deck.Mutation[PipelineState], error) {
			return deck.Complete(func(s *PipelineState) { s.Ready = true }), nil
		},
	}

	submitImage := deck.Cue[struct{}, PipelineState]{
		Name: "SubmitImage",
		When: func(_ struct{}, s PipelineState, r deck.Result) bool {
			return s.Ready && s.ImageID == ""
		},
		Run: func(_ struct{}, s PipelineState) (deck.Mutation[PipelineState], error) {
			return deck.Suspended(func(s *PipelineState) {
				s.ImageID = "img-001"
			}), nil
		},
	}

	submitCopy := deck.Cue[struct{}, PipelineState]{
		Name: "SubmitCopy",
		When: func(_ struct{}, s PipelineState, r deck.Result) bool {
			return s.Ready && s.CopyID == ""
		},
		Run: func(_ struct{}, s PipelineState) (deck.Mutation[PipelineState], error) {
			return deck.Suspended(func(s *PipelineState) {
				s.CopyID = "copy-001"
			}), nil
		},
	}

	collectImage := deck.Cue[struct{}, PipelineState]{
		Name: "CollectImage",
		When: func(_ struct{}, s PipelineState, r deck.Result) bool {
			return s.ImageID != "" && s.ImageURL == ""
		},
		Run: func(_ struct{}, s PipelineState) (deck.Mutation[PipelineState], error) {
			return deck.Complete(func(s *PipelineState) {
				s.ImageURL = "https://example.com/img.png"
			}), nil
		},
	}

	collectCopy := deck.Cue[struct{}, PipelineState]{
		Name: "CollectCopy",
		When: func(_ struct{}, s PipelineState, r deck.Result) bool {
			return s.CopyID != "" && s.CopyText == ""
		},
		Run: func(_ struct{}, s PipelineState) (deck.Mutation[PipelineState], error) {
			return deck.Complete(func(s *PipelineState) {
				s.CopyText = "Great article about Go"
			}), nil
		},
	}

	assembleCue := deck.Cue[struct{}, PipelineState]{
		Name: "Assemble",
		When: func(_ struct{}, s PipelineState, r deck.Result) bool {
			return s.ImageURL != "" && s.CopyText != "" && s.Output == ""
		},
		Run: func(_ struct{}, s PipelineState) (deck.Mutation[PipelineState], error) {
			return deck.Complete(func(s *PipelineState) {
				s.Output = s.CopyText + " [" + s.ImageURL + "]"
			}), nil
		},
	}

	d, err := deck.New(setupCue, submitImage, submitCopy, collectImage, collectCopy, assembleCue)
	require.NoError(t, err)

	ctx := context.Background()

	// Phase 1: Run — Setup completes, both submits suspend
	state := &PipelineState{}
	result1, err := d.Run(ctx, struct{}{}, state)
	require.NoError(t, err)
	assert.True(t, result1.Suspended)
	assert.True(t, state.Ready)
	assert.Equal(t, "img-001", state.ImageID)
	assert.Equal(t, "copy-001", state.CopyID)

	// Export
	data, err := d.Export(state, result1)
	require.NoError(t, err)

	// Phase 2: Import, then Resume — collects both results, assembles
	importedState, prev, err := d.Import(data)
	require.NoError(t, err)

	result2, err := d.Run(ctx, struct{}{}, importedState, prev)
	require.NoError(t, err)
	assert.False(t, result2.Suspended)
	assert.Equal(t, "Great article about Go [https://example.com/img.png]", importedState.Output)
	assert.True(t, result2.Completed("Setup"))
	assert.True(t, result2.Completed("CollectImage"))
	assert.True(t, result2.Completed("CollectCopy"))
	assert.True(t, result2.Completed("Assemble"))
}

func TestDeck_Run_Suspend_TwoConcurrentSuspends(t *testing.T) {
	// Given two cues that both suspend concurrently
	type DualState struct {
		BatchA string
		BatchB string
	}

	cueA := deck.Cue[struct{}, DualState]{
		Name: "SubmitA",
		When: func(_ struct{}, s DualState, r deck.Result) bool {
			return s.BatchA == ""
		},
		Run: func(_ struct{}, s DualState) (deck.Mutation[DualState], error) {
			time.Sleep(10 * time.Millisecond) // simulate work
			return deck.Suspended(func(s *DualState) {
				s.BatchA = "a-001"
			}), nil
		},
	}

	cueB := deck.Cue[struct{}, DualState]{
		Name: "SubmitB",
		When: func(_ struct{}, s DualState, r deck.Result) bool {
			return s.BatchB == ""
		},
		Run: func(_ struct{}, s DualState) (deck.Mutation[DualState], error) {
			time.Sleep(10 * time.Millisecond) // simulate work
			return deck.Suspended(func(s *DualState) {
				s.BatchB = "b-001"
			}), nil
		},
	}

	sut, err := deck.New(cueA, cueB)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// When both cues fire concurrently and both suspend
	state := &DualState{}
	result, err := sut.Run(ctx, struct{}{}, state)

	// Then both mutations are applied
	require.NoError(t, err)
	assert.Equal(t, "a-001", state.BatchA, "SubmitA mutation should be applied")
	assert.Equal(t, "b-001", state.BatchB, "SubmitB mutation should be applied")

	// And neither is marked as completed
	assert.False(t, result.Completed("SubmitA"), "SubmitA should not be completed")
	assert.False(t, result.Completed("SubmitB"), "SubmitB should not be completed")

	// And the result indicates suspension
	assert.True(t, result.Suspended)
}

func TestDeck_Run_Suspend_StatePreservedOnCancellation(t *testing.T) {
	// Given a cue that suspends, and the context is cancelled during drain
	state := &TestState{Count: 42}

	suspendCue := deck.Cue[struct{}, TestState]{
		Name: "Suspender",
		When: func(_ struct{}, s TestState, r deck.Result) bool {
			return true
		},
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			return deck.Suspended(func(s *TestState) {
				s.Count = 99
			}), nil
		},
	}

	slowCue := deck.Cue[struct{}, TestState]{
		Name: "SlowCue",
		When: func(_ struct{}, s TestState, r deck.Result) bool {
			return true
		},
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			time.Sleep(500 * time.Millisecond) // will exceed context
			return deck.Complete(func(s *TestState) {
				s.Count = 999
			}), nil
		},
	}

	sut, err := deck.New(suspendCue, slowCue)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// When the deck runs and context expires during drain
	_, runErr := sut.Run(ctx, struct{}{}, state)

	// Then an error is returned (context deadline exceeded)
	require.Error(t, runErr)

	// And the original state is NOT modified (error means no copy-back)
	assert.Equal(t, 42, state.Count, "State should be unchanged when Run returns an error")
}

// --- Input parameter behavior tests ---

type TestInput struct {
	Query  string
	APIKey string
}

func TestDeck_Run_InputIsAvailableToWhenPredicate(t *testing.T) {
	// Given a cue whose When predicate checks the input
	state := &TestState{Count: 0}
	input := TestInput{Query: "find me results"}

	cue := deck.Cue[TestInput, TestState]{
		Name: "InputAwareCue",
		When: func(i TestInput, s TestState, r deck.Result) bool {
			return i.Query == "find me results"
		},
		Run: func(i TestInput, s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) {
				s.Inc()
			}), nil
		},
	}

	sut, err := deck.New(cue)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// When the deck runs with the input
	result, err := sut.Run(ctx, input, state)

	// Then the cue fires because the input matched
	require.NoError(t, err)
	assert.Equal(t, 1, state.Count)
	assert.True(t, result.Completed("InputAwareCue"))
}

func TestDeck_Run_WhenPredicateDoesNotFireWhenInputDoesNotMatch(t *testing.T) {
	// Given a cue whose When predicate checks the input for a specific value
	state := &TestState{Count: 0}
	input := TestInput{Query: "wrong query"}

	cue := deck.Cue[TestInput, TestState]{
		Name: "InputAwareCue",
		When: func(i TestInput, s TestState, r deck.Result) bool {
			return i.Query == "find me results"
		},
		Run: func(i TestInput, s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) {
				s.Inc()
			}), nil
		},
	}

	sut, err := deck.New(cue)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// When the deck runs with a non-matching input
	result, err := sut.Run(ctx, input, state)

	// Then the cue does NOT fire
	require.NoError(t, err)
	assert.Equal(t, 0, state.Count, "Cue should not fire when input doesn't match")
	assert.False(t, result.Completed("InputAwareCue"))
}

func TestDeck_Run_InputIsAvailableToRunFunction(t *testing.T) {
	// Given a cue whose Run function uses the input to produce output
	type OutputState struct {
		Response string
	}

	input := TestInput{Query: "hello world"}
	state := &OutputState{}

	cue := deck.Cue[TestInput, OutputState]{
		Name: "UseInput",
		Run: func(i TestInput, s OutputState) (deck.Mutation[OutputState], error) {
			// Capture input value for use in mutation
			query := i.Query
			return deck.Complete(func(s *OutputState) {
				s.Response = "processed: " + query
			}), nil
		},
	}

	sut, err := deck.New(cue)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// When the deck runs
	_, err = sut.Run(ctx, input, state)

	// Then the run function had access to the input
	require.NoError(t, err)
	assert.Equal(t, "processed: hello world", state.Response)
}

func TestDeck_Run_InputRemainsConsistentAcrossCueChain(t *testing.T) {
	// Given two cues in a chain, both should see the same input value
	type Counter struct {
		Step int
	}

	input := TestInput{Query: "original"}
	state := &Counter{Step: 0}

	var querySeenByCue2 string

	cue1 := deck.Cue[TestInput, Counter]{
		Name: "Step1",
		When: func(i TestInput, s Counter, r deck.Result) bool {
			return s.Step == 0
		},
		Run: func(i TestInput, s Counter) (deck.Mutation[Counter], error) {
			return deck.Complete(func(s *Counter) {
				s.Step = 1
			}), nil
		},
	}

	cue2 := deck.Cue[TestInput, Counter]{
		Name: "Step2",
		When: func(i TestInput, s Counter, r deck.Result) bool {
			return s.Step == 1
		},
		Run: func(i TestInput, s Counter) (deck.Mutation[Counter], error) {
			querySeenByCue2 = i.Query
			return deck.Complete(func(s *Counter) {
				s.Step = 2
			}), nil
		},
	}

	sut, err := deck.New(cue1, cue2)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// When cues run in sequence
	_, err = sut.Run(ctx, input, state)

	// Then both cues saw the same input
	require.NoError(t, err)
	assert.Equal(t, 2, state.Step)
	assert.Equal(t, "original", querySeenByCue2, "Input should remain unchanged throughout execution")
}

func TestDeck_Run_InputIsPassedByValueToConcurrentCues(t *testing.T) {
	// Given multiple cues that run concurrently and all read the same input
	type Results struct {
		Queries []string
	}

	input := TestInput{Query: "concurrent-test"}
	state := &Results{}

	count := 10
	cues := make([]deck.Cue[TestInput, Results], count)
	for i := range count {
		cues[i] = deck.Cue[TestInput, Results]{
			Name: fmt.Sprintf("Cue-%d", i),
			Run: func(inp TestInput, s Results) (deck.Mutation[Results], error) {
				q := inp.Query
				time.Sleep(5 * time.Millisecond)
				return deck.Complete(func(s *Results) {
					s.Queries = append(s.Queries, q)
				}), nil
			},
		}
	}

	sut, err := deck.New(cues...)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// When all cues run concurrently
	_, err = sut.Run(ctx, input, state)

	// Then all cues received the same input value
	require.NoError(t, err)
	assert.Len(t, state.Queries, count)
	for _, q := range state.Queries {
		assert.Equal(t, "concurrent-test", q, "All cues should see the same input")
	}
}

func TestDeck_Export_DoesNotIncludeInput(t *testing.T) {
	// Given a deck that suspends with input and state
	type JobState struct {
		BatchID string `json:"batch_id"`
	}

	input := TestInput{Query: "secret-query", APIKey: "secret-key"}

	cue := deck.Cue[TestInput, JobState]{
		Name: "Submit",
		When: func(i TestInput, s JobState, r deck.Result) bool { return s.BatchID == "" },
		Run: func(i TestInput, s JobState) (deck.Mutation[JobState], error) {
			return deck.Suspended(func(s *JobState) {
				s.BatchID = "batch-001"
			}), nil
		},
	}

	d, err := deck.New(cue)
	require.NoError(t, err)

	state := &JobState{}
	result, err := d.Run(context.Background(), input, state)
	require.NoError(t, err)

	// When we export
	data, err := d.Export(state, result)
	require.NoError(t, err)

	// Then the exported data contains state but NOT input
	exported := string(data)
	assert.Contains(t, exported, "batch-001")
	assert.NotContains(t, exported, "secret-query", "Input should not be exported")
	assert.NotContains(t, exported, "secret-key", "Input should not be exported")
}

func TestDeck_Resume_UsesFreshInputNotOriginal(t *testing.T) {
	// Given a deck that suspended and is resumed with different input
	type JobState struct {
		BatchID string
		Output  string
	}

	submitCue := deck.Cue[TestInput, JobState]{
		Name: "Submit",
		When: func(i TestInput, s JobState, r deck.Result) bool { return s.BatchID == "" },
		Run: func(i TestInput, s JobState) (deck.Mutation[JobState], error) {
			return deck.Suspended(func(s *JobState) {
				s.BatchID = "batch-xyz"
			}), nil
		},
	}

	collectCue := deck.Cue[TestInput, JobState]{
		Name: "Collect",
		When: func(i TestInput, s JobState, r deck.Result) bool {
			return s.BatchID != "" && s.Output == ""
		},
		Run: func(i TestInput, s JobState) (deck.Mutation[JobState], error) {
			query := i.Query
			return deck.Complete(func(s *JobState) {
				s.Output = "done with " + query
			}), nil
		},
	}

	d, err := deck.New(submitCue, collectCue)
	require.NoError(t, err)

	// First run with original input
	state := &JobState{}
	result1, err := d.Run(context.Background(), TestInput{Query: "original"}, state)
	require.NoError(t, err)
	assert.True(t, result1.Suspended)

	// When we resume with DIFFERENT input
	result2, err := d.Run(context.Background(), TestInput{Query: "resumed"}, state, result1)

	// Then the collect cue uses the fresh input, not the original
	require.NoError(t, err)
	assert.False(t, result2.Suspended)
	assert.Equal(t, "done with resumed", state.Output, "Resume should use the fresh input, not the original")
}

func TestDeck_Run_InputIsNotMutatedByLibrary(t *testing.T) {
	// Given an input with several fields and a cue that reads them
	type RichInput struct {
		Query  string
		APIKey string
		Limit  int
	}
	type State struct {
		Hits int
	}

	input := RichInput{Query: "search", APIKey: "secret", Limit: 10}
	inputBefore := input

	cue := deck.Cue[RichInput, State]{
		Name: "ReadInput",
		Run: func(i RichInput, s State) (deck.Mutation[State], error) {
			return deck.Complete(func(s *State) {
				s.Hits = i.Limit
			}), nil
		},
	}

	sut, err := deck.New(cue)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// When the deck runs to completion
	state := &State{}
	_, err = sut.Run(ctx, input, state)

	// Then the caller's input variable is unchanged
	require.NoError(t, err)
	assert.Equal(t, inputBefore, input, "Library must not mutate caller's input variable")
}

func TestDeck_Run_InputReferenceFieldsAreShared(t *testing.T) {
	// Given an input containing a map field
	type ContextInput struct {
		Headers map[string]string
	}
	type CapturedPointers struct {
		Cue1Ptr uintptr
		Cue2Ptr uintptr
	}

	input := ContextInput{Headers: map[string]string{"trace-id": "abc-123"}}
	state := &CapturedPointers{}

	cue1 := deck.Cue[ContextInput, CapturedPointers]{
		Name: "CaptureMapPointer1",
		When: func(i ContextInput, s CapturedPointers, r deck.Result) bool {
			return s.Cue1Ptr == 0
		},
		Run: func(i ContextInput, s CapturedPointers) (deck.Mutation[CapturedPointers], error) {
			ptr := reflect.ValueOf(i.Headers).Pointer()
			return deck.Complete(func(s *CapturedPointers) {
				s.Cue1Ptr = ptr
			}), nil
		},
	}

	cue2 := deck.Cue[ContextInput, CapturedPointers]{
		Name: "CaptureMapPointer2",
		When: func(i ContextInput, s CapturedPointers, r deck.Result) bool {
			return s.Cue1Ptr != 0 && s.Cue2Ptr == 0
		},
		Run: func(i ContextInput, s CapturedPointers) (deck.Mutation[CapturedPointers], error) {
			ptr := reflect.ValueOf(i.Headers).Pointer()
			return deck.Complete(func(s *CapturedPointers) {
				s.Cue2Ptr = ptr
			}), nil
		},
	}

	sut, err := deck.New(cue1, cue2)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// When two cues capture the map's underlying pointer
	_, err = sut.Run(ctx, input, state)

	// Then both observe the same backing map (the reference is shared, not deep-copied)
	require.NoError(t, err)
	assert.NotZero(t, state.Cue1Ptr, "cue1 should have captured the map pointer")
	assert.Equal(t, state.Cue1Ptr, state.Cue2Ptr, "Reference fields in input must be shared, not deep-copied")
}

func TestDeck_Run_SequentialRunsHaveIndependentInputs(t *testing.T) {
	// Given a single Deck used for two consecutive runs with different inputs
	type GreetingInput struct {
		Name string
	}
	type GreetingState struct {
		Greeting string
	}

	cue := deck.Cue[GreetingInput, GreetingState]{
		Name: "Greet",
		When: func(i GreetingInput, s GreetingState, r deck.Result) bool {
			return s.Greeting == ""
		},
		Run: func(i GreetingInput, s GreetingState) (deck.Mutation[GreetingState], error) {
			return deck.Complete(func(s *GreetingState) {
				s.Greeting = "hello, " + i.Name
			}), nil
		},
	}

	sut, err := deck.New(cue)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// When the deck runs twice with different inputs and different states
	state1 := &GreetingState{}
	_, err = sut.Run(ctx, GreetingInput{Name: "alice"}, state1)
	require.NoError(t, err)

	state2 := &GreetingState{}
	_, err = sut.Run(ctx, GreetingInput{Name: "bob"}, state2)
	require.NoError(t, err)

	// Then each run sees only its own input — no leak from the previous run
	assert.Equal(t, "hello, alice", state1.Greeting)
	assert.Equal(t, "hello, bob", state2.Greeting)
}

func TestDeck_Run_PointerInputIsSupported(t *testing.T) {
	// Given a Cue parameterised with a pointer type as input
	type LargeInput struct {
		Body string
	}
	type State struct {
		Length int
	}

	cue := deck.Cue[*LargeInput, State]{
		Name: "MeasureBody",
		Run: func(i *LargeInput, s State) (deck.Mutation[State], error) {
			length := len(i.Body)
			return deck.Complete(func(s *State) {
				s.Length = length
			}), nil
		},
	}

	sut, err := deck.New(cue)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// When the deck runs with a pointer input
	input := &LargeInput{Body: "hello world"}
	state := &State{}
	_, err = sut.Run(ctx, input, state)

	// Then the cue receives the pointer and can read fields through it
	require.NoError(t, err)
	assert.Equal(t, 11, state.Length)
}

// --- Cue error propagation tests ---

func TestDeck_Run_PropagatesErrorFromCue(t *testing.T) {
	// Given a cue whose Run returns an error
	cueErr := errors.New("intentional failure")
	cue := deck.Cue[struct{}, TestState]{
		Name: "FailingCue",
		Run: func(_ struct{}, _ TestState) (deck.Mutation[TestState], error) {
			return nil, cueErr
		},
	}

	sut, err := deck.New(cue)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// When the deck runs
	state := &TestState{Count: 5}
	result, err := sut.Run(ctx, struct{}{}, state)

	// Then the error is propagated, wrapping the original
	require.Error(t, err)
	require.ErrorIs(t, err, cueErr, "Original error should be in the chain via %w")
	assert.Contains(t, err.Error(), "FailingCue", "Error should identify which cue failed")

	// And the cue is NOT recorded in CompletedCues
	assert.False(t, result.Completed("FailingCue"), "Errored cue must not appear as completed")

	// And the caller's state is unchanged (state is not copied back on error)
	assert.Equal(t, 5, state.Count, "State should not be modified when the run errors")
}

func TestDeck_Run_DoesNotStallWhenUpstreamCueErrors(t *testing.T) {
	// Given an upstream cue that errors and a downstream cue that depends on it
	// completing, the deck must not stall waiting for the downstream — it must
	// surface the upstream error and return promptly.
	upstreamErr := errors.New("upstream failed")
	upstream := deck.Cue[struct{}, TestState]{
		Name: "Upstream",
		When: func(_ struct{}, s TestState, _ deck.Result) bool {
			return s.Count == 0
		},
		Run: func(_ struct{}, _ TestState) (deck.Mutation[TestState], error) {
			return nil, upstreamErr
		},
	}

	downstream := deck.Cue[struct{}, TestState]{
		Name: "Downstream",
		When: func(_ struct{}, _ TestState, r deck.Result) bool {
			return r.Completed("Upstream")
		},
		Run: func(_ struct{}, _ TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) { s.Count = 99 }), nil
		},
	}

	sut, err := deck.New(upstream, downstream)
	require.NoError(t, err)

	// Use a generous timeout so we can distinguish "stalled" from "errored
	// promptly". A stalled deck would hit ctx cancellation; an errored deck
	// returns far sooner.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	start := time.Now()
	state := &TestState{Count: 0}
	_, err = sut.Run(ctx, struct{}{}, state)
	elapsed := time.Since(start)

	// Then the deck returns the upstream error rather than stalling on
	// Downstream's When predicate.
	require.Error(t, err)
	require.ErrorIs(t, err, upstreamErr, "Should return upstream error, not ctx error")
	assert.Less(t, elapsed, 200*time.Millisecond, "Should return promptly, not stall waiting for downstream")

	// And the downstream cue never ran — state is unchanged.
	assert.Equal(t, 0, state.Count, "Downstream cue must not have run")
}

func TestDeck_Run_ErrorPropagatesAlongsideSlowerConcurrentCue(t *testing.T) {
	// Given two concurrent cues — one fast-failing, one slow-succeeding —
	// the deck must return the failure error and not be blocked by the slower
	// cue still running.
	failErr := errors.New("fast fail")
	fastFail := deck.Cue[struct{}, TestState]{
		Name: "FastFail",
		Run: func(_ struct{}, _ TestState) (deck.Mutation[TestState], error) {
			return nil, failErr
		},
	}

	slowSucceed := deck.Cue[struct{}, TestState]{
		Name: "SlowSucceed",
		Run: func(_ struct{}, _ TestState) (deck.Mutation[TestState], error) {
			// Sleep so this cue finishes after FastFail's error has already
			// been observed by the runner.
			time.Sleep(50 * time.Millisecond)
			return deck.Complete(func(s *TestState) { s.Count = 99 }), nil
		},
	}

	sut, err := deck.New(fastFail, slowSucceed)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// When the deck runs
	state := &TestState{Count: 5}
	_, err = sut.Run(ctx, struct{}{}, state)

	// Then the fast cue's error wins
	require.Error(t, err)
	require.ErrorIs(t, err, failErr)

	// And the caller's state is unchanged — the slow cue's mutation, even if
	// it ran to completion, must not be visible because the run errored.
	assert.Equal(t, 5, state.Count, "State must not be copied back when the run errors")
}

func TestDeck_Run_ErrorIsNotDroppedWhenSuspendArrivesFirst(t *testing.T) {
	// Given two concurrent cues fired in the same cycle: one suspends quickly,
	// the other errors a short moment later. The runner observes the suspend
	// first, takes the suspend-then-drain branch, and consumes the error
	// during drain. The error must not be dropped — it must propagate from
	// Deck.Run.
	cueErr := errors.New("late failure")

	fastSuspend := deck.Cue[struct{}, TestState]{
		Name: "FastSuspend",
		When: func(_ struct{}, s TestState, _ deck.Result) bool {
			return s.Count == 0
		},
		Run: func(_ struct{}, _ TestState) (deck.Mutation[TestState], error) {
			return deck.Suspended(func(s *TestState) { s.Count = 7 }), nil
		},
	}

	slowFail := deck.Cue[struct{}, TestState]{
		Name: "SlowFail",
		When: func(_ struct{}, s TestState, _ deck.Result) bool {
			return s.Count == 0
		},
		Run: func(_ struct{}, _ TestState) (deck.Mutation[TestState], error) {
			time.Sleep(30 * time.Millisecond)
			return nil, cueErr
		},
	}

	sut, err := deck.New(fastSuspend, slowFail)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	state := &TestState{Count: 0}
	result, err := sut.Run(ctx, struct{}{}, state)

	// Then the error from the slow cue is surfaced — not silently dropped by
	// the suspend-then-drain path.
	require.Error(t, err)
	require.ErrorIs(t, err, cueErr, "Error captured during suspend-drain must propagate")
	assert.Contains(t, err.Error(), "SlowFail")

	// And the caller's state is not copied back, even though FastSuspend's
	// mutation succeeded — the run errored as a whole.
	assert.Equal(t, 0, state.Count, "State must not be copied back when the run errors")
	assert.False(t, result.Completed("SlowFail"), "Errored cue must not be completed")
}

func TestDeck_Run_ErrorTakesPrecedenceOverSuspendedMutation(t *testing.T) {
	// Given a single cue that returns BOTH a Suspended mutation and an error,
	// the error wins: the mutation is discarded, the cue is not recorded in
	// CompletedCues, and Result.Suspended is not set.
	cueErr := errors.New("boom")
	cue := deck.Cue[struct{}, TestState]{
		Name: "Both",
		Run: func(_ struct{}, _ TestState) (deck.Mutation[TestState], error) {
			return deck.Suspended(func(s *TestState) { s.Count = 99 }), cueErr
		},
	}

	sut, err := deck.New(cue)
	require.NoError(t, err)

	state := &TestState{Count: 5}
	result, err := sut.Run(context.Background(), struct{}{}, state)

	require.Error(t, err)
	require.ErrorIs(t, err, cueErr)

	assert.Equal(t, 5, state.Count, "Mutation must be discarded when err != nil")
	assert.False(t, result.Suspended, "Run must not be marked Suspended when the cue erred")
	assert.False(t, result.Completed("Both"), "Errored cue must not appear as completed")
}

func TestDeck_Run_FirstConcurrentErrorIsReturned(t *testing.T) {
	// Given two cues that both return errors concurrently, ordered
	// deterministically by sleep, only the first error is wrapped and returned.
	firstErr := errors.New("first")
	secondErr := errors.New("second")

	fastErr := deck.Cue[struct{}, TestState]{
		Name: "FastErr",
		Run: func(_ struct{}, _ TestState) (deck.Mutation[TestState], error) {
			return nil, firstErr
		},
	}

	slowErr := deck.Cue[struct{}, TestState]{
		Name: "SlowErr",
		Run: func(_ struct{}, _ TestState) (deck.Mutation[TestState], error) {
			time.Sleep(30 * time.Millisecond)
			return nil, secondErr
		},
	}

	sut, err := deck.New(fastErr, slowErr)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	state := &TestState{Count: 5}
	_, err = sut.Run(ctx, struct{}{}, state)

	require.Error(t, err)
	require.ErrorIs(t, err, firstErr, "First-observed error must be the wrapped one")
	require.NotErrorIs(t, err, secondErr, "Second error must not be wrapped (first wins)")
	assert.Contains(t, err.Error(), "FastErr")
}

func assertExecutionOrder(t *testing.T, result deck.Result, order ...string) {
	t.Helper()

	// Create a map of cue name to index in completed list
	indices := make(map[string]int)
	for i, c := range result.CompletedCues {
		indices[c.Name] = i
	}

	for i := 0; i < len(order)-1; i++ {
		currKey := order[i]
		nextKey := order[i+1]

		idx1, ok1 := indices[currKey]
		idx2, ok2 := indices[nextKey]

		if assert.True(t, ok1, "Cue %s should have completed", currKey) &&
			assert.True(t, ok2, "Cue %s should have completed", nextKey) {
			assert.Less(t, idx1, idx2, "Cue %s should complete before %s", currKey, nextKey)
		}
	}
}

// --- Deterministic execution hooks ---

func TestDeck_Run_InjectedNow_StampsCompletedCueTimes(t *testing.T) {
	// Given a deck whose clock is injected rather than read from the wall
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	ticks := 0

	cue := deck.Cue[struct{}, TestState]{
		Name: "Tick",
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) { s.Count++ }), nil
		},
	}

	sut, err := deck.New(cue)
	require.NoError(t, err)
	sut.Engine = deck.WithClock(deck.Goroutines(), func() time.Time {
		// Now is called from inside each cue; a bare counter would race.
		mu.Lock()
		defer mu.Unlock()
		ticks++
		return base.Add(time.Duration(ticks) * time.Second)
	})

	// When the deck runs
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := sut.Run(ctx, struct{}{}, &TestState{})

	// Then the cue's times come from the injected clock, not time.Now
	require.NoError(t, err)
	require.Len(t, result.CompletedCues, 1)
	assert.Equal(t, base.Add(1*time.Second), result.CompletedCues[0].StartTime)
	assert.Equal(t, base.Add(2*time.Second), result.CompletedCues[0].EndTime)
}

func TestDeck_Run_SerialSpawn_RunsEachCueToCompletionBeforeTheNext(t *testing.T) {
	// Given a deck whose Spawn runs each cue inline instead of on a goroutine.
	// The unsynchronised slice below is the point: if anything ran
	// concurrently, -race would catch it.
	var log []string
	mkCue := func(name string) deck.Cue[struct{}, TestState] {
		return deck.Cue[struct{}, TestState]{
			Name: name,
			Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
				log = append(log, "start:"+name, "end:"+name)
				return deck.Complete(func(s *TestState) { s.Count++ }), nil
			},
		}
	}

	sut, err := deck.New(mkCue("A"), mkCue("B"), mkCue("C"))
	require.NoError(t, err)
	sut.Engine = deck.Serial()

	// When the deck runs
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	state := &TestState{}
	result, err := runBounded(t, sut, ctx, struct{}{}, state)

	// Then no cue overlapped another, and every cue still completed
	require.NoError(t, err)
	assert.Equal(t, []string{
		"start:A", "end:A",
		"start:B", "end:B",
		"start:C", "end:C",
	}, log)
	assert.Equal(t, 3, state.Count)
	assert.Len(t, result.CompletedCues, 3)
}

// executionMode configures a Deck for one of the three ways a run can be
// driven: plain goroutines, a serial Spawn, or a cooperative scheduler.
type executionMode struct {
	name  string
	apply func(*deck.Deck[struct{}, TestState])
}

func executionModes() []executionMode {
	return []executionMode{
		{"goroutine", func(*deck.Deck[struct{}, TestState]) {}},
		{"serial", func(d *deck.Deck[struct{}, TestState]) {
			d.Engine = deck.Serial()
		}},
		{"scheduler", func(d *deck.Deck[struct{}, TestState]) {
			d.Engine = newCoopEngine()
		}},
	}
}

// runBounded calls Run on its own goroutine and fails the test if it has not
// returned within five seconds. A serial Spawn or an Await is never rescued
// by ctx, so a hang would otherwise stall the whole package run.
func runBounded[I, S any](t *testing.T, d *deck.Deck[I, S], ctx context.Context, input I, state *S, prev ...deck.Result) (deck.Result, error) {
	t.Helper()
	type outcome struct {
		result deck.Result
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := d.Run(ctx, input, state, prev...)
		done <- outcome{result: result, err: err}
	}()
	select {
	case o := <-done:
		return o.result, o.err
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return: a serial Spawn or an Await cannot be rescued by ctx")
		return deck.Result{}, nil
	}
}

// The next two run under every execution mode on purpose. Each mode reaches
// cancellation by a different route, and two of them once ignored it entirely:
// a serial Engine finishes its work before anything checks ctx, and an awaiting
// Engine hands waiting to the caller's scheduler. Cancellation is now checked
// between cycles precisely so all three behave alike, and running the same
// assertions across the table is what keeps that true.
func TestDeck_Run_AlreadyCancelledContext_TriggersNothing(t *testing.T) {
	marker := t.Name()
	for _, mode := range executionModes() {
		t.Run(mode.name, func(t *testing.T) {
			// Given a context cancelled before Run is called
			var mu sync.Mutex
			triggered := 0
			cue := deck.Cue[struct{}, TestState]{
				Name: "Cue",
				Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
					mu.Lock()
					triggered++
					mu.Unlock()
					return deck.Complete(func(s *TestState) { s.Count = 1 }), nil
				},
			}
			sut, err := deck.New(cue)
			require.NoError(t, err)
			mode.apply(sut)

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			// When the deck runs
			state := &TestState{Count: 42}
			result, err := runBounded(t, sut, ctx, struct{}{}, state)

			// Then no cue is started and state is untouched
			require.ErrorIs(t, err, context.Canceled)
			assert.Empty(t, result.CompletedCues)
			assert.Equal(t, 42, state.Count)
			require.Eventually(t, func() bool { return cueGoroutines(marker) == 0 }, 2*time.Second, 5*time.Millisecond)
			mu.Lock()
			defer mu.Unlock()
			assert.Zero(t, triggered, "cue was started under a dead context")
		})
	}
}

// The harder half of the pair: cancelled mid-flight rather than up front. This
// is the case that regressed once — a result already waiting used to beat
// ctx.Done(), so a chain of cues kept triggering new work under a dead context
// and ran to completion. Counting how often the second cue starts is what
// catches that; a returned error alone would not.
func TestDeck_Run_CancelledFromInsideACue_TriggersNoFurtherCues(t *testing.T) {
	marker := t.Name()
	for _, mode := range executionModes() {
		t.Run(mode.name, func(t *testing.T) {
			// Given a chain whose first cue cancels the run's context
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var mu sync.Mutex
			secondStarted := 0
			first := deck.Cue[struct{}, TestState]{
				Name: "First",
				Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
					cancel()
					return deck.Complete(func(s *TestState) { s.Count = 1 }), nil
				},
			}
			second := deck.Cue[struct{}, TestState]{
				Name: "Second",
				When: func(_ struct{}, s TestState, r deck.Result) bool { return r.Completed("First") },
				Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
					mu.Lock()
					secondStarted++
					mu.Unlock()
					return deck.Complete(func(s *TestState) { s.Count = 2 }), nil
				},
			}
			sut, err := deck.New(first, second)
			require.NoError(t, err)
			mode.apply(sut)

			// When the deck runs
			state := &TestState{}
			_, err = runBounded(t, sut, ctx, struct{}{}, state)

			// Then the next cue is never started and state is not copied back
			require.ErrorIs(t, err, context.Canceled)
			assert.Equal(t, 0, state.Count)
			require.Eventually(t, func() bool { return cueGoroutines(marker) == 0 }, 2*time.Second, 5*time.Millisecond)
			mu.Lock()
			defer mu.Unlock()
			assert.Zero(t, secondStarted, "a cue was started after the context was cancelled")
		})
	}
}

// coopEngine is an Engine modelling a cooperative, single-threaded scheduler
// like Temporal's deterministic runner: work handed to Go is queued rather than
// started, and the queue only advances when the runner yields in Await. It
// exists to prove the Deck performs no blocking operation of its own.
type coopEngine struct {
	queue    []func()
	spawned  int
	awaitErr error
	now      func() time.Time
}

func newCoopEngine() *coopEngine { return &coopEngine{} }

func (e *coopEngine) Now() time.Time {
	if e.now != nil {
		return e.now()
	}
	return time.Now()
}

func (e *coopEngine) Spawn(fn func()) deck.Future {
	return e.enqueue(func() error { fn(); return nil })
}

func (e *coopEngine) Execute(ctx context.Context, w deck.Work) deck.Future {
	return e.enqueue(func() error { return w.Local(ctx) })
}

func (e *coopEngine) enqueue(fn func() error) deck.Future {
	e.spawned++
	f := &coopFuture{}
	e.queue = append(e.queue, func() {
		f.err = fn()
		f.ready = true
	})
	return f
}

func (e *coopEngine) Await(_ context.Context, fs []deck.Future) error {
	if e.awaitErr != nil {
		return e.awaitErr
	}
	for !deck.AnyReady(fs) {
		if len(e.queue) == 0 {
			return errors.New("scheduler deadlocked: nothing left to run")
		}
		next := e.queue[0]
		e.queue = e.queue[1:]
		next()
	}
	return nil
}

type coopFuture struct {
	ready bool
	err   error
}

func (f *coopFuture) IsReady() bool { return f.ready }
func (f *coopFuture) Get() error    { return f.err }

// The shape that makes parallelism possible: the Deck must hand every triggered
// cue to the Engine before waiting on any of them. Assert on the interleaving,
// not just the results — a Deck that spawned and waited, spawned and waited,
// would produce the same final state while running everything in series.
func TestDeck_Run_Await_HandsEveryCueToTheSchedulerBeforeAnyRuns(t *testing.T) {
	// Given a scheduler that defers spawned work rather than running it
	sched := newCoopEngine()
	var log []string

	mkCue := func(name string) deck.Cue[struct{}, TestState] {
		return deck.Cue[struct{}, TestState]{
			Name: name,
			Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
				log = append(log, "run:"+name)
				return deck.Complete(func(s *TestState) { s.Count++ }), nil
			},
		}
	}

	sut, err := deck.New(mkCue("A"), mkCue("B"), mkCue("C"))
	require.NoError(t, err)
	sut.Engine = &loggingEngine{Engine: sched, log: &log}

	// When the deck runs
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	state := &TestState{}
	result, err := runBounded(t, sut, ctx, struct{}{}, state)

	// Then all three cues reached the scheduler before any of them executed —
	// the fan-out shape that puts three Temporal activities in flight at once
	require.NoError(t, err)
	assert.Equal(t, []string{"spawn", "spawn", "spawn", "run:A", "run:B", "run:C"}, log)
	assert.Equal(t, 3, state.Count)
	assert.Len(t, result.CompletedCues, 3)
}

func TestDeck_Run_Await_SchedulerCancellationSurfacesAsError(t *testing.T) {
	// Given a scheduler that accepts the work but is cancelled before running
	// it — Temporal's Await returns CanceledError while coroutines are still
	// queued behind it
	cue := deck.Cue[struct{}, TestState]{
		Name: "NeverRuns",
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) { s.Count = 999 }), nil
		},
	}

	sut, err := deck.New(cue)
	require.NoError(t, err)
	sched := newCoopEngine()
	sched.awaitErr = errors.New("workflow canceled")
	sut.Engine = sched

	// When the deck runs
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	state := &TestState{Count: 42}
	_, err = runBounded(t, sut, ctx, struct{}{}, state)

	// Then the scheduler's error is surfaced and state is not copied back,
	// with the cue still sitting unrun in the scheduler's queue
	require.Error(t, err)
	assert.Contains(t, err.Error(), "deck run aborted: workflow canceled")
	assert.Equal(t, 42, state.Count)
	assert.Len(t, sched.queue, 1)
}

// Abandoned cues used to leak. Run would return on cancellation without
// draining, and every cue still in flight parked forever trying to hand back a
// result nobody was left to receive — one stuck goroutine per abandoned cue,
// for the life of the process. Nothing about a leak shows up in a normal test,
// so it needs counting directly.
func TestDeck_Run_Cancellation_DoesNotLeakCueGoroutines(t *testing.T) {
	// Given cues that block until released, each abandoned by a cancelled run
	const runs = 20
	started := make(chan struct{}, runs)
	release := make(chan struct{})
	cue := deck.Cue[struct{}, TestState]{
		Name: "Blocked",
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			started <- struct{}{}
			<-release
			return deck.Complete(func(s *TestState) { s.Count++ }), nil
		},
	}
	sut, err := deck.New(cue)
	require.NoError(t, err)

	// When every run is cancelled as soon as its cue has started
	for i := 0; i < runs; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			<-started
			cancel()
		}()
		_, err := sut.Run(ctx, struct{}{}, &TestState{})
		require.ErrorIs(t, err, context.Canceled)
	}
	require.Equal(t, runs, cueGoroutines(t.Name()), "every abandoned cue should still be parked")

	// Then, once released, each cue goroutine finishes and exits rather than
	// parking forever on a send nobody is left to receive
	close(release)
	assert.Eventually(t, func() bool { return cueGoroutines(t.Name()) == 0 },
		2*time.Second, 5*time.Millisecond, "cue goroutines leaked after %d abandoned runs", runs)
}

// cueGoroutines counts goroutines still running a cue spawned by the named
// test: those with a deck runner frame beneath that test's closure.
func cueGoroutines(testName string) int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	count := 0
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, "lordtatty/deck.(*runner[") && strings.Contains(g, testName) {
			count++
		}
	}
	return count
}

func TestDeck_Run_Await_DrainsRemainingCuesThroughTheScheduler(t *testing.T) {
	// Given one cue that suspends the run while another is still queued
	sched := newCoopEngine()

	suspendCue := deck.Cue[struct{}, TestState]{
		Name: "Suspender",
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			return deck.Suspended(func(s *TestState) { s.Count = 99 }), nil
		},
	}
	otherCue := deck.Cue[struct{}, TestState]{
		Name: "Other",
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) { s.Buffer = append(s.Buffer, 'x') }), nil
		},
	}

	sut, err := deck.New(suspendCue, otherCue)
	require.NoError(t, err)
	sut.Engine = sched

	// When the deck runs
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	state := &TestState{}
	result, err := runBounded(t, sut, ctx, struct{}{}, state)

	// Then the run suspends, and the still-queued cue is drained by yielding
	// to the scheduler rather than by blocking on it
	require.NoError(t, err)
	assert.True(t, result.Suspended)
	assert.Empty(t, sched.queue, "scheduler should have no work left")
	assert.Equal(t, 99, state.Count)
	assert.Equal(t, []rune{'x'}, state.Buffer)
	assert.True(t, result.Completed("Other"))
	assert.False(t, result.Completed("Suspender"))
}

func TestDeck_Run_SerialSpawn_RunsDependentCuesAcrossCycles(t *testing.T) {
	// Given three cues that each unlock the next, registered out of order so
	// that only the When predicates can produce the right sequence
	var order []string
	mkCue := func(name, needs string) deck.Cue[struct{}, TestState] {
		return deck.Cue[struct{}, TestState]{
			Name: name,
			When: func(_ struct{}, s TestState, r deck.Result) bool {
				return needs == "" || r.Completed(needs)
			},
			Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
				order = append(order, name)
				return deck.Complete(func(s *TestState) { s.Count++ }), nil
			},
		}
	}

	sut, err := deck.New(mkCue("C", "B"), mkCue("A", ""), mkCue("B", "A"))
	require.NoError(t, err)
	sut.Engine = deck.Serial()

	// When the deck runs
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	state := &TestState{}
	result, err := runBounded(t, sut, ctx, struct{}{}, state)

	// Then the chain still advances one cycle at a time: a cue's mutation is
	// absorbed before the next check sees it
	require.NoError(t, err)
	assert.Equal(t, []string{"A", "B", "C"}, order)
	assert.Equal(t, 3, state.Count)
	assertExecutionOrder(t, result, "A", "B", "C")
}

func TestDeck_Run_Await_RunsDependentCuesAcrossCycles(t *testing.T) {
	// Given the same chain, driven by a cooperative scheduler
	sched := newCoopEngine()
	var order []string
	mkCue := func(name, needs string) deck.Cue[struct{}, TestState] {
		return deck.Cue[struct{}, TestState]{
			Name: name,
			When: func(_ struct{}, s TestState, r deck.Result) bool {
				return needs == "" || r.Completed(needs)
			},
			Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
				order = append(order, name)
				return deck.Complete(func(s *TestState) { s.Count++ }), nil
			},
		}
	}

	sut, err := deck.New(mkCue("C", "B"), mkCue("A", ""), mkCue("B", "A"))
	require.NoError(t, err)
	sut.Engine = sched

	// When the deck runs
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	state := &TestState{}
	result, err := runBounded(t, sut, ctx, struct{}{}, state)

	// Then the chain advances across cycles without ever blocking natively
	require.NoError(t, err)
	assert.Equal(t, []string{"A", "B", "C"}, order)
	assert.Equal(t, 3, state.Count)
	assert.Empty(t, sched.queue)
	assertExecutionOrder(t, result, "A", "B", "C")
}

func TestDeck_Run_SerialSpawn_SurfacesCueErrorAndDiscardsItsMutation(t *testing.T) {
	// Given a cue that returns both a mutation and an error
	boom := deck.Cue[struct{}, TestState]{
		Name: "Boom",
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) { s.Count = 999 }), errors.New("kaboom")
		},
	}

	sut, err := deck.New(boom)
	require.NoError(t, err)
	sut.Engine = deck.Serial()

	// When the deck runs
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	state := &TestState{Count: 42}
	result, err := runBounded(t, sut, ctx, struct{}{}, state)

	// Then the error wins: the mutation is dropped and the cue is not recorded
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cue Boom: kaboom")
	assert.False(t, result.Completed("Boom"))
	assert.Equal(t, 42, state.Count)
}

func TestDeck_Run_Await_SurfacesCueErrorAndDiscardsItsMutation(t *testing.T) {
	// Given the same failing cue, run through a cooperative scheduler
	sched := newCoopEngine()
	boom := deck.Cue[struct{}, TestState]{
		Name: "Boom",
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) { s.Count = 999 }), errors.New("kaboom")
		},
	}

	sut, err := deck.New(boom)
	require.NoError(t, err)
	sut.Engine = sched

	// When the deck runs
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	state := &TestState{Count: 42}
	result, err := runBounded(t, sut, ctx, struct{}{}, state)

	// Then the error surfaces from the scheduled cue just the same
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cue Boom: kaboom")
	assert.False(t, result.Completed("Boom"))
	assert.Equal(t, 42, state.Count)
	assert.Empty(t, sched.queue)
}

func TestDeck_Run_SerialSpawn_SuspendsExportsAndResumes(t *testing.T) {
	// Given a submit/collect pair spanning a suspend — the durable-execution
	// round trip, driven entirely by a serial Spawn
	submit := deck.Cue[struct{}, TestState]{
		Name: "Submit",
		When: func(_ struct{}, s TestState, r deck.Result) bool { return s.Count == 0 },
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			return deck.Suspended(func(s *TestState) { s.Count = 1 }), nil
		},
	}
	collect := deck.Cue[struct{}, TestState]{
		Name: "Collect",
		When: func(_ struct{}, s TestState, r deck.Result) bool { return s.Count == 1 },
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) { s.Count = 2 }), nil
		},
	}

	sut, err := deck.New(submit, collect)
	require.NoError(t, err)
	sut.Engine = deck.Serial()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// When the first run suspends and its snapshot is round-tripped
	state := &TestState{}
	first, err := runBounded(t, sut, ctx, struct{}{}, state)
	require.NoError(t, err)
	require.True(t, first.Suspended)
	require.Equal(t, 1, state.Count)

	data, err := sut.Export(state, first)
	require.NoError(t, err)
	resumedState, prev, err := sut.Import(data)
	require.NoError(t, err)

	// And the deck is resumed from it
	second, err := runBounded(t, sut, ctx, struct{}{}, resumedState, prev)

	// Then the collecting cue runs and the work finishes
	require.NoError(t, err)
	assert.False(t, second.Suspended)
	assert.True(t, second.Completed("Collect"))
	assert.Equal(t, 2, resumedState.Count)
}

// Guards the one failure the Deck cannot rescue itself from. Under a serial
// Engine every cue completes before anything collects the results, so if the
// Engine could not hold them all the run would wedge inside trigger — before
// any context is consulted. Hence runBounded: five seconds and a clear message,
// rather than the package timing out after ten minutes with a goroutine dump.
func TestDeck_Run_SerialSpawn_DoesNotDeadlockWhenEveryCueTriggersAtOnce(t *testing.T) {
	// Given many cues that all trigger in one cycle. A serial Spawn delivers
	// every result before anything receives, so the buffer must hold them all
	// or Run deadlocks before any context is consulted.
	const cueCount = 25
	var cues []deck.Cue[struct{}, TestState]
	for i := 0; i < cueCount; i++ {
		cues = append(cues, deck.Cue[struct{}, TestState]{
			Name: fmt.Sprintf("Cue%d", i),
			Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
				return deck.Complete(func(s *TestState) { s.Count++ }), nil
			},
		})
	}

	sut, err := deck.New(cues...)
	require.NoError(t, err)
	sut.Engine = deck.Serial()

	// When the deck runs
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := runBounded(t, sut, ctx, struct{}{}, &TestState{})

	// Then it finishes rather than parking on a send nobody can receive
	require.NoError(t, err)
	assert.Len(t, result.CompletedCues, cueCount)
}

// An Engine that reports success with nothing ready is broken, but the failure
// mode matters more than the cause: the Deck would park on a receive that no
// context can reach, and inside a workflow engine that is a permanently stuck
// run with nothing in the logs to explain it. A named error costs one line and
// turns that into something diagnosable.
func TestDeck_Run_Await_ReturningWithoutAResultFailsRatherThanBlocking(t *testing.T) {
	// Given a scheduler whose Await returns without a result being ready — a
	// broken adapter
	sched := newCoopEngine()
	cue := deck.Cue[struct{}, TestState]{
		Name: "C",
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) { s.Count++ }), nil
		},
	}

	sut, err := deck.New(cue)
	require.NoError(t, err)
	sut.Engine = &spuriousAwaitEngine{Engine: sched}

	// When the deck runs under a context that would normally rescue it
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err = runBounded(t, sut, ctx, struct{}{}, &TestState{})

	// Then Run fails with a diagnosable error instead of blocking forever
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stalled")
}

// A cancelled run returns while its cues are still executing. Those cues keep
// reading whatever the Engine gave them — the clock, in this case — so if the
// runner read Deck.Engine live, a caller setting up their next run would race
// with a cue from the last one. The runner takes its own copy of the Engine at
// the start of each run, which is what makes the reassignment below safe.
//
// Only -race can fail this: without it the reassignment and the read simply
// interleave harmlessly. CI runs -race.
func TestDeck_Run_EngineMayBeReassignedWhileAnAbandonedCueStillRuns(t *testing.T) {
	// Given a cue still running after its run was cancelled
	started := make(chan struct{})
	cue := deck.Cue[struct{}, TestState]{
		Name: "Lingering",
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			close(started)
			time.Sleep(30 * time.Millisecond)
			return deck.Complete(func(s *TestState) { s.Count++ }), nil
		},
	}
	sut, err := deck.New(cue)
	require.NoError(t, err)
	sut.Engine = deck.Goroutines()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-started
		cancel()
	}()
	_, err = sut.Run(ctx, struct{}{}, &TestState{})
	require.ErrorIs(t, err, context.Canceled)

	// When a hook is reassigned for the next run while that cue is still live
	sut.Engine = deck.WithClock(deck.Goroutines(), func() time.Time { return time.Unix(0, 0) })

	// Then the lingering cue finishes without racing on the hook — the run
	// took its own copy when it started (-race would report otherwise)
	assert.Eventually(t, func() bool { return cueGoroutines(t.Name()) == 0 }, 2*time.Second, 5*time.Millisecond)
}

func TestDeck_Run_Await_CueErrorSurvivesAnAbortedDrain(t *testing.T) {
	// Given one cue that fails while another is still queued, and a
	// scheduler that is cancelled during the drain
	boom := errors.New("boom")
	sched := newCoopEngine()
	failing := deck.Cue[struct{}, TestState]{
		Name: "Failing",
		Run:  func(_ struct{}, s TestState) (deck.Mutation[TestState], error) { return nil, boom },
	}
	queued := deck.Cue[struct{}, TestState]{
		Name: "Queued",
		Run: func(_ struct{}, s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) { s.Count++ }), nil
		},
	}
	sut, err := deck.New(failing, queued)
	require.NoError(t, err)
	sut.Engine = &failAfterEngine{Engine: sched, after: 1, err: errors.New("workflow canceled")}

	// When the deck runs
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := runBounded(t, sut, ctx, struct{}{}, &TestState{})

	// Then the cue's error is the root cause reported, with the drain's
	// cancellation alongside rather than in its place
	require.ErrorIs(t, err, boom)
	assert.Contains(t, err.Error(), "workflow canceled")
	assert.False(t, result.Suspended)
}

// loggingEngine records a marker each time work is handed to the engine, so a
// test can see how many cues were started before any of them ran.
type loggingEngine struct {
	deck.Engine
	log *[]string
}

func (e *loggingEngine) Spawn(fn func()) deck.Future {
	*e.log = append(*e.log, "spawn")
	return e.Engine.Spawn(fn)
}

// spuriousAwaitEngine reports success from Await without anything being ready —
// a broken engine.
type spuriousAwaitEngine struct{ deck.Engine }

func (e *spuriousAwaitEngine) Await(context.Context, []deck.Future) error { return nil }

// failAfterEngine yields normally, then fails — an engine cancelled partway
// through a run.
type failAfterEngine struct {
	deck.Engine
	after int
	err   error
	calls int
}

func (e *failAfterEngine) Await(ctx context.Context, fs []deck.Future) error {
	e.calls++
	if e.calls > e.after {
		return e.err
	}
	return e.Engine.Await(ctx, fs) //nolint:wrapcheck // delegating to the wrapped engine
}

// --- Declared work ---

// slowValue is an ordinary Go function: the shape a cue declares with Do, and
// the shape a durable engine would register as an activity.
func slowValue(ctx context.Context, name string) (string, error) {
	select {
	case <-time.After(50 * time.Millisecond):
	case <-ctx.Done():
		return "", fmt.Errorf("slowValue cancelled: %w", ctx.Err())
	}
	return "value:" + name, nil
}

type workState struct {
	Values []string
	Starts map[string]time.Time
	Ends   map[string]time.Time
}

func TestDeck_Run_Do_RunsDeclaredWorkConcurrently(t *testing.T) {
	// Given two cues that declare work rather than doing it themselves
	var mu sync.Mutex
	starts, ends := map[string]time.Time{}, map[string]time.Time{}
	timed := func(ctx context.Context, name string) (string, error) {
		mu.Lock()
		starts[name] = time.Now()
		mu.Unlock()
		v, err := slowValue(ctx, name)
		mu.Lock()
		ends[name] = time.Now()
		mu.Unlock()
		return v, err
	}

	mkCue := func(name string) deck.Cue[struct{}, workState] {
		return deck.Cue[struct{}, workState]{
			Name: name,
			Run: func(_ struct{}, _ workState) (deck.Mutation[workState], error) {
				return deck.Do(timed, name, func(v string) deck.Mutation[workState] {
					return deck.Complete(func(s *workState) { s.Values = append(s.Values, v) })
				}), nil
			},
		}
	}

	sut, err := deck.New(mkCue("a"), mkCue("b"))
	require.NoError(t, err)

	// When the deck runs on the default engine
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	state := &workState{}
	result, err := sut.Run(ctx, struct{}{}, state)

	// Then both results reached state, and the two units of work overlapped
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"value:a", "value:b"}, state.Values)
	assert.Len(t, result.CompletedCues, 2)

	mu.Lock()
	defer mu.Unlock()
	assert.True(t, starts["a"].Before(ends["b"]) && starts["b"].Before(ends["a"]),
		"declared work should have run concurrently")
}

func TestDeck_Run_Do_UnderSerialEngine_RunsWorkInlineInOrder(t *testing.T) {
	// Given the same shape of cue on the serial engine
	var order []string
	record := func(_ context.Context, name string) (string, error) {
		order = append(order, name)
		return "value:" + name, nil
	}

	mkCue := func(name string) deck.Cue[struct{}, workState] {
		return deck.Cue[struct{}, workState]{
			Name: name,
			Run: func(_ struct{}, _ workState) (deck.Mutation[workState], error) {
				return deck.Do(record, name, func(v string) deck.Mutation[workState] {
					return deck.Complete(func(s *workState) { s.Values = append(s.Values, v) })
				}), nil
			},
		}
	}

	sut, err := deck.New(mkCue("a"), mkCue("b"), mkCue("c"))
	require.NoError(t, err)
	sut.Engine = deck.Serial()

	// When the deck runs
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	state := &workState{}
	result, err := runBounded(t, sut, ctx, struct{}{}, state)

	// Then the work ran inline, in order, with no concurrency at all
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, order)
	assert.Equal(t, []string{"value:a", "value:b", "value:c"}, state.Values)
	assert.Len(t, result.CompletedCues, 3)
}

func TestDeck_Run_Do_WorkErrorFailsTheCue(t *testing.T) {
	// Given a cue whose declared work fails
	boom := errors.New("boom")
	failing := func(_ context.Context, name string) (string, error) { return "", boom }

	cue := deck.Cue[struct{}, workState]{
		Name: "Failing",
		Run: func(_ struct{}, _ workState) (deck.Mutation[workState], error) {
			return deck.Do(failing, "x", func(v string) deck.Mutation[workState] {
				return deck.Complete(func(s *workState) { s.Values = append(s.Values, v) })
			}), nil
		},
	}

	sut, err := deck.New(cue)
	require.NoError(t, err)
	sut.Engine = deck.Serial()

	// When the deck runs
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	state := &workState{}
	result, err := runBounded(t, sut, ctx, struct{}{}, state)

	// Then the failure is reported against the cue, and nothing was applied
	require.ErrorIs(t, err, boom)
	assert.Contains(t, err.Error(), "cue Failing")
	assert.False(t, result.Completed("Failing"))
	assert.Empty(t, state.Values)
}

// captureEngine records the Work it is handed, so a test can see what an
// engine has to go on when it decides how to perform it.
type captureEngine struct {
	deck.Engine
	works []deck.Work
}

func (e *captureEngine) Execute(ctx context.Context, w deck.Work) deck.Future {
	e.works = append(e.works, w)
	return e.Engine.Execute(ctx, w) //nolint:wrapcheck // delegating to the wrapped engine
}

// CueName is what lets an Engine treat one cue's work differently from
// another's without the cue knowing anything about the Engine — it is what
// decktemporal.ForCue is built on. Cheap to break by accident when the runner
// changes, and nothing else in the core would notice.
func TestDeck_Run_Do_TellsTheEngineWhichCueDeclaredTheWork(t *testing.T) {
	// Given cues that declare work, on an engine that inspects it
	mkCue := func(name string) deck.Cue[struct{}, workState] {
		return deck.Cue[struct{}, workState]{
			Name: name,
			Run: func(_ struct{}, _ workState) (deck.Mutation[workState], error) {
				return deck.Do(slowValue, name, func(v string) deck.Mutation[workState] {
					return deck.Complete(func(s *workState) { s.Values = append(s.Values, v) })
				}), nil
			},
		}
	}

	engine := &captureEngine{Engine: deck.Serial()}
	sut, err := deck.New(mkCue("alpha"), mkCue("beta"))
	require.NoError(t, err)
	sut.Engine = engine

	// When the deck runs
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = runBounded(t, sut, ctx, struct{}{}, &workState{})
	require.NoError(t, err)

	// Then each unit of work names the cue that declared it, so an engine can
	// treat one cue's work differently from another's
	require.Len(t, engine.works, 2)
	assert.Equal(t, "alpha", engine.works[0].CueName)
	assert.Equal(t, "beta", engine.works[1].CueName)
}

// whenAnyEngine models an engine whose only waiting primitive is "block until
// one of these handles is done" — the shape durable-execution engines other
// than Temporal tend to offer. It is never handed a predicate, only the
// futures, so it can only be written if Await says what it is waiting on.
type whenAnyEngine struct {
	queue []func()
}

func (e *whenAnyEngine) Now() time.Time { return time.Now() }

func (e *whenAnyEngine) Spawn(fn func()) deck.Future {
	return e.enqueue(func() error { fn(); return nil })
}

func (e *whenAnyEngine) Execute(ctx context.Context, w deck.Work) deck.Future {
	return e.enqueue(func() error { return w.Local(ctx) })
}

func (e *whenAnyEngine) enqueue(fn func() error) deck.Future {
	f := &coopFuture{}
	e.queue = append(e.queue, func() {
		f.err = fn()
		f.ready = true
	})
	return f
}

func (e *whenAnyEngine) Await(_ context.Context, fs []deck.Future) error {
	for !deck.AnyReady(fs) {
		if len(e.queue) == 0 {
			return errors.New("nothing left to run")
		}
		next := e.queue[0]
		e.queue = e.queue[1:]
		next()
	}
	return nil
}

func TestDeck_Run_EngineThatCanOnlyWaitOnHandles(t *testing.T) {
	// Given cues that declare work, and an engine with no way to evaluate a
	// condition — it can only be told which handles to wait on
	mkCue := func(name, needs string) deck.Cue[struct{}, workState] {
		c := deck.Cue[struct{}, workState]{
			Name: name,
			Run: func(_ struct{}, _ workState) (deck.Mutation[workState], error) {
				return deck.Do(slowValue, name, func(v string) deck.Mutation[workState] {
					return deck.Complete(func(s *workState) { s.Values = append(s.Values, v) })
				}), nil
			},
		}
		if needs != "" {
			c.When = func(_ struct{}, _ workState, r deck.Result) bool { return r.Completed(needs) }
		}
		return c
	}

	sut, err := deck.New(mkCue("a", ""), mkCue("b", ""), mkCue("c", "a"))
	require.NoError(t, err)
	sut.Engine = &whenAnyEngine{}

	// When the deck runs
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	state := &workState{}
	result, err := runBounded(t, sut, ctx, struct{}{}, state)

	// Then it completes exactly as it would on any other engine
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"value:a", "value:b", "value:c"}, state.Values)
	assert.Len(t, result.CompletedCues, 3)
}

// This guards the mistake a Temporal user is most likely to make. The natural
// way to write a workflow is to build the Deck once, at package level, and give
// each workflow its own Engine:
//
//	var flowDeck, _ = deck.New(cues...)
//
//	func MyWorkflow(ctx workflow.Context) error {
//		flowDeck.Engine = temporal.New(ctx)   // <-- data race
//		...
//	}
//
// That assignment looks harmless and is not: one worker runs many workflows at
// a time, so every one of them writes that single field while the others are
// reading it. WithEngine exists so the obvious spelling is also the safe one,
// and this test is the reason it cannot quietly go back to assigning in place.
//
// It fails in two independent ways if WithEngine ever stops copying, which is
// deliberate:
//
//   - under -race, the detector fires on the concurrent writes (CI runs -race)
//   - without -race, the final assertion still catches it, because a
//     non-copying WithEngine leaves its last Engine behind on the shared Deck
func TestDeck_WithEngine_LetsOneDeckServeConcurrentRuns(t *testing.T) {
	// Given one Deck built once and shared, as a package-level Deck would be
	var cues []deck.Cue[struct{}, TestState]
	for i := range 4 {
		cues = append(cues, deck.Cue[struct{}, TestState]{
			Name: string(rune('a' + i)),
			Run: func(_ struct{}, _ TestState) (deck.Mutation[TestState], error) {
				return deck.Complete(func(s *TestState) { s.Count++ }), nil
			},
		})
	}
	sut, err := deck.New(cues...)
	require.NoError(t, err)

	// When many runs go at once, each choosing its own engine. They alternate
	// between two engines on purpose: if every run wrote the same value, an
	// in-place assignment could look correct by luck.
	var wg sync.WaitGroup
	for i := range 8 {
		engine := deck.Goroutines()
		if i%2 == 0 {
			engine = deck.Serial()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Each run gets its own Deck, so there is nothing shared to write
			// to. Every run must still see all four cues complete — proof that
			// the copy kept the cues and only swapped the engine.
			state := &TestState{}
			_, runErr := sut.WithEngine(engine).Run(ctx, struct{}{}, state)
			assert.NoError(t, runErr)
			assert.Equal(t, 4, state.Count)
		}()
	}
	wg.Wait()

	// Then the shared Deck is exactly as New left it. Nothing wrote to it, so
	// there was nothing to race on.
	assert.Nil(t, sut.Engine, "WithEngine must copy the Deck, not modify it")
}

// --- Panics in user code ---

// A cue is user code, often calling further user code, so it can panic. Where
// that panic surfaced used to depend entirely on the Engine: with Serial it
// reached the caller, under Temporal the SDK caught it, and under the default
// Engine it ran on a goroutine deck had created — killing the process, with no
// way for the caller to defend against it.
//
// Deck now turns a panic in user code into the cue error it already has a path
// for, identically under every Engine. These three tests cover the three places
// deck calls into user code, and each runs across the whole table because
// consistency between engines is the point.

func TestDeck_Run_PanicInCueRunBecomesACueError(t *testing.T) {
	for _, mode := range executionModes() {
		t.Run(mode.name, func(t *testing.T) {
			// Given a cue whose Run panics
			cue := deck.Cue[struct{}, TestState]{
				Name: "Boom",
				Run: func(_ struct{}, _ TestState) (deck.Mutation[TestState], error) {
					panic("cue exploded")
				},
			}
			sut, err := deck.New(cue)
			require.NoError(t, err)
			mode.apply(sut)

			// When the deck runs
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			state := &TestState{Count: 7}
			result, err := runBounded(t, sut, ctx, struct{}{}, state)

			// Then the run fails, naming the cue and carrying the panic and a
			// stack trace — without which a recovered panic is far harder to
			// chase than the crash it replaced
			require.Error(t, err)
			assert.Contains(t, err.Error(), "cue Boom")
			assert.Contains(t, err.Error(), "cue exploded")
			assert.Contains(t, err.Error(), "goroutine")
			assert.False(t, result.Completed("Boom"))
			assert.Equal(t, 7, state.Count, "state should not be copied back")
		})
	}
}

func TestDeck_Run_PanicInDeclaredWorkBecomesACueError(t *testing.T) {
	for _, mode := range executionModes() {
		t.Run(mode.name, func(t *testing.T) {
			// Given work declared with Do that panics when the Engine runs it
			exploding := func(_ context.Context, _ string) (string, error) {
				panic("work exploded")
			}
			cue := deck.Cue[struct{}, TestState]{
				Name: "Boom",
				Run: func(_ struct{}, _ TestState) (deck.Mutation[TestState], error) {
					return deck.Do(exploding, "x", func(string) deck.Mutation[TestState] {
						return deck.Complete(func(s *TestState) { s.Count = 99 })
					}), nil
				},
			}
			sut, err := deck.New(cue)
			require.NoError(t, err)
			mode.apply(sut)

			// When the deck runs
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			state := &TestState{Count: 7}
			_, err = runBounded(t, sut, ctx, struct{}{}, state)

			// Then it fails the same way as a panic in Run
			require.Error(t, err)
			assert.Contains(t, err.Error(), "cue Boom")
			assert.Contains(t, err.Error(), "work exploded")
			assert.Equal(t, 7, state.Count)
		})
	}
}

func TestDeck_Run_PanicInResultHandlerBecomesACueError(t *testing.T) {
	for _, mode := range executionModes() {
		t.Run(mode.name, func(t *testing.T) {
			// Given work that succeeds but whose result handler panics. That
			// handler runs on deck's own goroutine rather than the Engine's, so
			// it is a separate path from the two above.
			cue := deck.Cue[struct{}, TestState]{
				Name: "Boom",
				Run: func(_ struct{}, _ TestState) (deck.Mutation[TestState], error) {
					return deck.Do(slowValue, "x", func(string) deck.Mutation[TestState] {
						panic("handler exploded")
					}), nil
				},
			}
			sut, err := deck.New(cue)
			require.NoError(t, err)
			mode.apply(sut)

			// When the deck runs
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			state := &TestState{Count: 7}
			_, err = runBounded(t, sut, ctx, struct{}{}, state)

			// Then it fails the same way again
			require.Error(t, err)
			assert.Contains(t, err.Error(), "cue Boom")
			assert.Contains(t, err.Error(), "handler exploded")
			assert.Equal(t, 7, state.Count)
		})
	}
}
