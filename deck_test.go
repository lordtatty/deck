package deck_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
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
