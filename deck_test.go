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

	cue := deck.Cue[TestState]{
		Name: "HappyPath",
		When: func(s TestState, r deck.Result) bool {
			return s.GetCount() == 1
		},
		Run: func(s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) {
				s.Inc()
			}), nil
		},
	}

	sut, err := deck.New(cue)
	assert.NoError(t, err)

	// Act
	// Run for a short duration to allow the loop to execute
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	result, err := sut.Run(ctx, state)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 2, state.GetCount(), "Count should be incremented to 2")
	assert.Equal(t, "HappyPath", result.CompletedCues[0].Name)
}

func TestDeck_Run_ChainReaction(t *testing.T) {
	// Arrange
	state := &TestState{
		Count: 0,
	}

	// Cue 1: 0 -> 1
	cue1 := deck.Cue[TestState]{
		Name: "cue1",
		When: func(s TestState, r deck.Result) bool {
			return s.GetCount() == 0
		},
		Run: func(s TestState) (deck.Mutation[TestState], error) {
			// Ensure some time passes so timestamps are distinct
			time.Sleep(1 * time.Millisecond)
			return deck.Complete(func(s *TestState) {
				s.Inc()
			}), nil
		},
	}

	// Cue 2: 1 -> 2
	cue2 := deck.Cue[TestState]{
		Name: "cue2",
		When: func(s TestState, r deck.Result) bool {
			return s.GetCount() == 1
		},
		Run: func(s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) {
				s.Inc()
			}), nil
		},
	}

	sut, err := deck.New(cue1, cue2)
	assert.NoError(t, err)

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	result, err := sut.Run(ctx, state)

	// Assert
	assert.NoError(t, err)
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
	cue := deck.Cue[TestState]{
		Name: "SleepyCue",
		When: func(s TestState, r deck.Result) bool {
			return true
		},
		Run: func(s TestState) (deck.Mutation[TestState], error) {
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
		_, err := sut.Run(ctx, state)
		errChan <- err
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
		When: func(s TestState, r deck.Result) bool {
			return true
		},
		Run: func(s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) {
				s.Inc()
			}), nil
		},
	}

	sut, err := deck.New(cue)
	assert.NoError(t, err)

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	result, err := sut.Run(ctx, state)

	// Assert
	assert.NoError(t, err)
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

	cues := make([]deck.Cue[TestState], count)
	for i := range count {
		cues[i] = deck.Cue[TestState]{
			Name: fmt.Sprintf("Cue-%d", i),
			When: func(s TestState, r deck.Result) bool {
				return true
			},
			Run: func(s TestState) (deck.Mutation[TestState], error) {
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
	assert.NoError(t, err)

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := sut.Run(ctx, state)

	// Assert
	assert.NoError(t, err)
	// If race conditions occur, Buffer will be corrupted (missing chars, wrong order)
	assert.Equal(t, string(expected), string(state.Buffer), "Buffer content should match expected sequence if updates are safe")
	assert.Len(t, result.CompletedCues, count)
}

func TestDeck_New_DuplicateNames(t *testing.T) {
	// Arrange
	cue1 := deck.Cue[TestState]{
		Name: "Duplicate",
		When: func(s TestState, r deck.Result) bool { return true },
		Run:  func(s TestState) (deck.Mutation[TestState], error) { return nil, nil },
	}
	cue2 := deck.Cue[TestState]{
		Name: "Duplicate",
		When: func(s TestState, r deck.Result) bool { return true },
		Run:  func(s TestState) (deck.Mutation[TestState], error) { return nil, nil },
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
		When: func(s TestState, r deck.Result) bool { return true },
		Run:  func(s TestState) (deck.Mutation[TestState], error) { return nil, nil },
	}

	// Act
	_, err := deck.New(cue)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cue name cannot be empty")
}

func TestDeck_New_NilRun(t *testing.T) {
	// Arrange
	cue := deck.Cue[TestState]{
		Name: "NilRunCue",
		When: nil,
		Run:  nil,
	}

	// Act
	_, err := deck.New(cue)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cue run cannot be nil: NilRunCue")
}

func TestDeck_Run_ReturnsCompletedCues(t *testing.T) {
	// Arrange
	state := &TestState{Count: 0}

	cue1 := deck.Cue[TestState]{
		Name: "CueA",
		When: func(s TestState, r deck.Result) bool { return s.Count == 0 },
		Run: func(s TestState) (deck.Mutation[TestState], error) {
			time.Sleep(10 * time.Millisecond) // Simulate work
			return deck.Complete(func(s *TestState) { s.Inc() }), nil
		},
	}

	cue2 := deck.Cue[TestState]{
		Name: "CueB",
		When: func(s TestState, r deck.Result) bool { return s.Count == 1 },
		Run: func(s TestState) (deck.Mutation[TestState], error) {
			time.Sleep(20 * time.Millisecond) // Simulate work
			return deck.Complete(func(s *TestState) { s.Inc() }), nil
		},
	}

	sut, err := deck.New(cue1, cue2)
	assert.NoError(t, err)

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	result, err := sut.Run(ctx, state)

	// Assert
	assert.NoError(t, err)

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
		assert.True(t, c.Duration() > 0, "Duration should be positive")

		if c.Name == "CueA" {
			assert.True(t, c.Duration() >= 10*time.Millisecond, "CueA duration should be at least 10ms")
		}
		if c.Name == "CueB" {
			assert.True(t, c.Duration() >= 20*time.Millisecond, "CueB duration should be at least 20ms")
		}
	}
}

func TestDeck_Run_TriggerOnHistory(t *testing.T) {
	// Arrange
	state := &TestState{Count: 0}

	cue1 := deck.Cue[TestState]{
		Name: "CueA",
		When: func(s TestState, r deck.Result) bool { return s.Count == 0 },
		Run: func(s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) { s.Inc() }), nil
		},
	}

	cue2 := deck.Cue[TestState]{
		Name: "CueB",
		When: func(s TestState, r deck.Result) bool {
			// Trigger only if CueA has completed
			return r.Completed("CueA")
		},
		Run: func(s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) { s.Inc() }), nil
		},
	}

	sut, err := deck.New(cue1, cue2)
	assert.NoError(t, err)

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	result, err := sut.Run(ctx, state)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 2, state.Count)
	assertExecutionOrder(t, result, "CueA", "CueB")
}

func TestDeck_StateImmutability(t *testing.T) {
	// Arrange
	state := &TestState{Count: 0}

	cue := deck.Cue[TestState]{
		Name: "BadActor",
		When: func(s TestState, r deck.Result) bool {
			// Attempt to modify state in When (should be a copy)
			s.Count = 999
			return true
		},
		Run: func(s TestState) (deck.Mutation[TestState], error) {
			// Attempt to modify state in Run (should be a copy)
			s.Count = 888
			return deck.Complete(func(s *TestState) {
				// Only this mutation should affect the real state
				s.Inc()
			}), nil
		},
	}

	sut, err := deck.New(cue)
	assert.NoError(t, err)

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err = sut.Run(ctx, state)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 1, state.Count, "State should only be modified by the mutation function")
}

func TestDeck_Run_ExternalModification(t *testing.T) {
	// Arrange
	state := &TestState{Count: 0}

	started := make(chan struct{})
	continueChan := make(chan struct{})

	// Cue that signals start, waits, then increments
	cue := deck.Cue[TestState]{
		Name: "CoordinatedCue",
		When: func(s TestState, r deck.Result) bool {
			return s.Count == 0
		},
		Run: func(s TestState) (deck.Mutation[TestState], error) {
			close(started)
			<-continueChan
			return deck.Complete(func(s *TestState) {
				s.Inc()
			}), nil
		},
	}

	sut, err := deck.New(cue)
	assert.NoError(t, err)

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Start Run in background
	errChan := make(chan error)
	go func() {
		_, err := sut.Run(ctx, state)
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
	assert.NoError(t, err)

	// Assert
	// The cue logic (When: s.Count == 0) used the initial state.
	// The mutation (0 -> 1) is applied to the isolated state.
	// The final isolated state (Count: 1) is copied back, overwriting the 100.
	assert.Equal(t, 1, state.Count, "External modification should be overwritten by isolated run result")
}

func TestDeck_Run_NilWhen(t *testing.T) {
	// Arrange
	state := &TestState{Count: 0}

	cue := deck.Cue[TestState]{
		Name: "AlwaysRun",
		When: nil, // Should default to true
		Run: func(s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) {
				s.Inc()
			}), nil
		},
	}

	sut, err := deck.New(cue)
	assert.NoError(t, err)

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	result, err := sut.Run(ctx, state)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 1, state.Count, "Cue with nil When should run")
	assert.Equal(t, "AlwaysRun", result.CompletedCues[0].Name)
}

func TestDeck_Run_Suspend_MutationAppliedButNotCompleted(t *testing.T) {
	// Given a cue that returns a Suspended mutation
	state := &TestState{Count: 0}

	cue := deck.Cue[TestState]{
		Name: "SuspendingCue",
		When: func(s TestState, r deck.Result) bool {
			return s.Count == 0
		},
		Run: func(s TestState) (deck.Mutation[TestState], error) {
			return deck.Suspended(func(s *TestState) {
				s.Count = 42
			}), nil
		},
	}

	sut, err := deck.New(cue)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// When the deck runs
	result, err := sut.Run(ctx, state)

	// Then the mutation is applied
	assert.NoError(t, err)
	assert.Equal(t, 42, state.Count, "Suspended mutation should still be applied to state")

	// And the cue is NOT in CompletedCues
	assert.False(t, result.Completed("SuspendingCue"), "Suspended cue should not appear in completed cues")

	// And the result indicates suspension
	assert.True(t, result.Suspended, "Result should indicate the deck was suspended")
}

func TestDeck_Run_Suspend_OtherCuesDrainBeforeReturning(t *testing.T) {
	// Given a cue that suspends and another cue that runs normally
	state := &TestState{Count: 0}

	suspendCue := deck.Cue[TestState]{
		Name: "Suspender",
		When: func(s TestState, r deck.Result) bool {
			return true
		},
		Run: func(s TestState) (deck.Mutation[TestState], error) {
			return deck.Suspended(func(s *TestState) {
				s.Count += 10
			}), nil
		},
	}

	normalCue := deck.Cue[TestState]{
		Name: "Normal",
		When: func(s TestState, r deck.Result) bool {
			return true
		},
		Run: func(s TestState) (deck.Mutation[TestState], error) {
			time.Sleep(20 * time.Millisecond) // takes a bit longer
			return deck.Complete(func(s *TestState) {
				s.Count += 1
			}), nil
		},
	}

	sut, err := deck.New(suspendCue, normalCue)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// When the deck runs
	result, err := sut.Run(ctx, state)

	// Then both mutations are applied
	assert.NoError(t, err)
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

	cueA := deck.Cue[TestState]{
		Name: "CueA",
		When: func(s TestState, r deck.Result) bool {
			return true // would fire if not already completed
		},
		Run: func(s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) {
				s.Count += 100 // should NOT happen on resume
			}), nil
		},
	}

	cueB := deck.Cue[TestState]{
		Name: "CueB",
		When: func(s TestState, r deck.Result) bool {
			return r.Completed("CueA")
		},
		Run: func(s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) {
				s.Count += 1
			}), nil
		},
	}

	sut, err := deck.New(cueA, cueB)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// When we resume with CueA already completed
	prev := deck.Result{
		CompletedCues: []deck.CompletedCue{{Name: "CueA"}},
	}
	result, err := sut.Resume(ctx, state, prev)

	// Then CueA does not re-run (count would be 101+ if it did)
	assert.NoError(t, err)
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

	submitCue := deck.Cue[BatchState]{
		Name: "SubmitBatch",
		When: func(s BatchState, r deck.Result) bool {
			return s.BatchID == ""
		},
		Run: func(s BatchState) (deck.Mutation[BatchState], error) {
			return deck.Suspended(func(s *BatchState) {
				s.BatchID = "batch-123"
			}), nil
		},
	}

	checkCue := deck.Cue[BatchState]{
		Name: "CheckBatch",
		When: func(s BatchState, r deck.Result) bool {
			return s.BatchID != "" && s.Result == ""
		},
		Run: func(s BatchState) (deck.Mutation[BatchState], error) {
			return deck.Complete(func(s *BatchState) {
				s.Result = "done"
			}), nil
		},
	}

	sut, err := deck.New(submitCue, checkCue)
	assert.NoError(t, err)

	ctx := context.Background()

	// First run: SubmitBatch fires and suspends
	state := &BatchState{}
	result1, err := sut.Run(ctx, state)
	assert.NoError(t, err)
	assert.True(t, result1.Suspended)
	assert.Equal(t, "batch-123", state.BatchID)
	assert.Equal(t, "", state.Result)
	// SubmitBatch should not be in completed (it suspended)
	assert.False(t, result1.Completed("SubmitBatch"))

	// Resume: SubmitBatch won't fire (BatchID != ""), CheckBatch fires
	result2, err := sut.Resume(ctx, state, result1)
	assert.NoError(t, err)
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

	setupCue := deck.Cue[WorkState]{
		Name: "Setup",
		When: func(s WorkState, r deck.Result) bool {
			return !s.SetupDone
		},
		Run: func(s WorkState) (deck.Mutation[WorkState], error) {
			return deck.Complete(func(s *WorkState) {
				s.SetupDone = true
			}), nil
		},
	}

	batchCue := deck.Cue[WorkState]{
		Name: "SubmitBatch",
		When: func(s WorkState, r deck.Result) bool {
			return s.SetupDone && s.BatchID == ""
		},
		Run: func(s WorkState) (deck.Mutation[WorkState], error) {
			return deck.Suspended(func(s *WorkState) {
				s.BatchID = "batch-456"
			}), nil
		},
	}

	collectCue := deck.Cue[WorkState]{
		Name: "CollectResult",
		When: func(s WorkState, r deck.Result) bool {
			return s.BatchID != "" && s.Result == ""
		},
		Run: func(s WorkState) (deck.Mutation[WorkState], error) {
			return deck.Complete(func(s *WorkState) {
				s.Result = "collected"
			}), nil
		},
	}

	sut, err := deck.New(setupCue, batchCue, collectCue)
	assert.NoError(t, err)

	ctx := context.Background()

	// First run: Setup completes, SubmitBatch fires and suspends
	state := &WorkState{}
	result1, err := sut.Run(ctx, state)
	assert.NoError(t, err)
	assert.True(t, result1.Suspended)
	assert.True(t, state.SetupDone)
	assert.Equal(t, "batch-456", state.BatchID)
	assert.True(t, result1.Completed("Setup"), "Setup should be completed")
	assert.False(t, result1.Completed("SubmitBatch"), "SubmitBatch suspended, not completed")

	// Resume: Setup already completed (skipped), SubmitBatch won't match (BatchID set),
	// CollectResult fires
	result2, err := sut.Resume(ctx, state, result1)
	assert.NoError(t, err)
	assert.False(t, result2.Suspended)
	assert.Equal(t, "collected", state.Result)
	assert.True(t, result2.Completed("Setup"), "Setup should carry over from previous result")
	assert.True(t, result2.Completed("CollectResult"), "CollectResult should be completed")
}

func TestDeck_Run_Suspend_NonSuspendedMutationWorksAsNormal(t *testing.T) {
	// Given a cue that returns a normal (non-suspended) mutation
	state := &TestState{Count: 0}

	cue := deck.Cue[TestState]{
		Name: "NormalCue",
		When: func(s TestState, r deck.Result) bool {
			return true
		},
		Run: func(s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) {
				s.Count = 5
			}), nil
		},
	}

	sut, err := deck.New(cue)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// When the deck runs
	result, err := sut.Run(ctx, state)

	// Then it completes normally with no suspension
	assert.NoError(t, err)
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

	cue := deck.Cue[JobState]{
		Name: "Submit",
		When: func(s JobState, r deck.Result) bool { return s.BatchID == "" },
		Run: func(s JobState) (deck.Mutation[JobState], error) {
			return deck.Suspended(func(s *JobState) {
				s.BatchID = "batch-789"
			}), nil
		},
	}

	d, err := deck.New(cue)
	assert.NoError(t, err)

	state := &JobState{}
	result, err := d.Run(context.Background(), state)
	assert.NoError(t, err)
	assert.True(t, result.Suspended)

	// When we export
	data, err := d.Export(state, result)

	// Then it succeeds and produces valid JSON
	assert.NoError(t, err)
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

	submitCue := deck.Cue[JobState]{
		Name: "Submit",
		When: func(s JobState, r deck.Result) bool { return s.BatchID == "" },
		Run: func(s JobState) (deck.Mutation[JobState], error) {
			return deck.Suspended(func(s *JobState) {
				s.BatchID = "batch-abc"
			}), nil
		},
	}

	collectCue := deck.Cue[JobState]{
		Name: "Collect",
		When: func(s JobState, r deck.Result) bool {
			return s.BatchID != "" && s.Result == ""
		},
		Run: func(s JobState) (deck.Mutation[JobState], error) {
			return deck.Complete(func(s *JobState) {
				s.Result = "collected"
			}), nil
		},
	}

	d, err := deck.New(submitCue, collectCue)
	assert.NoError(t, err)

	// First run: suspends after submit
	state := &JobState{}
	result1, err := d.Run(context.Background(), state)
	assert.NoError(t, err)
	assert.True(t, result1.Suspended)
	assert.Equal(t, "batch-abc", state.BatchID)

	// Export the state
	data, err := d.Export(state, result1)
	assert.NoError(t, err)

	// Import restores state and previous result
	importedState, prev, err := d.Import(data)
	assert.NoError(t, err)
	assert.Equal(t, "batch-abc", importedState.BatchID)
	assert.True(t, prev.Suspended)

	// When we resume using the imported data
	result2, err := d.Resume(context.Background(), importedState, prev)

	// Then it completes successfully
	assert.NoError(t, err)
	assert.False(t, result2.Suspended)
	assert.Equal(t, "batch-abc", importedState.BatchID)
	assert.Equal(t, "collected", importedState.Result)
	assert.True(t, result2.Completed("Collect"))
}

func TestDeck_Import_InvalidJSON(t *testing.T) {
	// Given a deck
	cue := deck.Cue[TestState]{
		Name: "Cue",
		When: func(s TestState, r deck.Result) bool { return true },
		Run: func(s TestState) (deck.Mutation[TestState], error) {
			return deck.Complete(func(s *TestState) {}), nil
		},
	}

	d, err := deck.New(cue)
	assert.NoError(t, err)

	// When we try to import invalid data
	_, _, err = d.Import([]byte("not json"))

	// Then it returns an error
	assert.Error(t, err)
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

	setupCue := deck.Cue[PipelineState]{
		Name: "Setup",
		When: func(s PipelineState, r deck.Result) bool { return !s.Ready },
		Run: func(s PipelineState) (deck.Mutation[PipelineState], error) {
			return deck.Complete(func(s *PipelineState) { s.Ready = true }), nil
		},
	}

	submitImage := deck.Cue[PipelineState]{
		Name: "SubmitImage",
		When: func(s PipelineState, r deck.Result) bool {
			return s.Ready && s.ImageID == ""
		},
		Run: func(s PipelineState) (deck.Mutation[PipelineState], error) {
			return deck.Suspended(func(s *PipelineState) {
				s.ImageID = "img-001"
			}), nil
		},
	}

	submitCopy := deck.Cue[PipelineState]{
		Name: "SubmitCopy",
		When: func(s PipelineState, r deck.Result) bool {
			return s.Ready && s.CopyID == ""
		},
		Run: func(s PipelineState) (deck.Mutation[PipelineState], error) {
			return deck.Suspended(func(s *PipelineState) {
				s.CopyID = "copy-001"
			}), nil
		},
	}

	collectImage := deck.Cue[PipelineState]{
		Name: "CollectImage",
		When: func(s PipelineState, r deck.Result) bool {
			return s.ImageID != "" && s.ImageURL == ""
		},
		Run: func(s PipelineState) (deck.Mutation[PipelineState], error) {
			return deck.Complete(func(s *PipelineState) {
				s.ImageURL = "https://example.com/img.png"
			}), nil
		},
	}

	collectCopy := deck.Cue[PipelineState]{
		Name: "CollectCopy",
		When: func(s PipelineState, r deck.Result) bool {
			return s.CopyID != "" && s.CopyText == ""
		},
		Run: func(s PipelineState) (deck.Mutation[PipelineState], error) {
			return deck.Complete(func(s *PipelineState) {
				s.CopyText = "Great article about Go"
			}), nil
		},
	}

	assembleCue := deck.Cue[PipelineState]{
		Name: "Assemble",
		When: func(s PipelineState, r deck.Result) bool {
			return s.ImageURL != "" && s.CopyText != "" && s.Output == ""
		},
		Run: func(s PipelineState) (deck.Mutation[PipelineState], error) {
			return deck.Complete(func(s *PipelineState) {
				s.Output = s.CopyText + " [" + s.ImageURL + "]"
			}), nil
		},
	}

	d, err := deck.New(setupCue, submitImage, submitCopy, collectImage, collectCopy, assembleCue)
	assert.NoError(t, err)

	ctx := context.Background()

	// Phase 1: Run — Setup completes, both submits suspend
	state := &PipelineState{}
	result1, err := d.Run(ctx, state)
	assert.NoError(t, err)
	assert.True(t, result1.Suspended)
	assert.True(t, state.Ready)
	assert.Equal(t, "img-001", state.ImageID)
	assert.Equal(t, "copy-001", state.CopyID)

	// Export
	data, err := d.Export(state, result1)
	assert.NoError(t, err)

	// Phase 2: Import, then Resume — collects both results, assembles
	importedState, prev, err := d.Import(data)
	assert.NoError(t, err)

	result2, err := d.Resume(ctx, importedState, prev)
	assert.NoError(t, err)
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

	cueA := deck.Cue[DualState]{
		Name: "SubmitA",
		When: func(s DualState, r deck.Result) bool {
			return s.BatchA == ""
		},
		Run: func(s DualState) (deck.Mutation[DualState], error) {
			time.Sleep(10 * time.Millisecond) // simulate work
			return deck.Suspended(func(s *DualState) {
				s.BatchA = "a-001"
			}), nil
		},
	}

	cueB := deck.Cue[DualState]{
		Name: "SubmitB",
		When: func(s DualState, r deck.Result) bool {
			return s.BatchB == ""
		},
		Run: func(s DualState) (deck.Mutation[DualState], error) {
			time.Sleep(10 * time.Millisecond) // simulate work
			return deck.Suspended(func(s *DualState) {
				s.BatchB = "b-001"
			}), nil
		},
	}

	sut, err := deck.New(cueA, cueB)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// When both cues fire concurrently and both suspend
	state := &DualState{}
	result, err := sut.Run(ctx, state)

	// Then both mutations are applied
	assert.NoError(t, err)
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

	suspendCue := deck.Cue[TestState]{
		Name: "Suspender",
		When: func(s TestState, r deck.Result) bool {
			return true
		},
		Run: func(s TestState) (deck.Mutation[TestState], error) {
			return deck.Suspended(func(s *TestState) {
				s.Count = 99
			}), nil
		},
	}

	slowCue := deck.Cue[TestState]{
		Name: "SlowCue",
		When: func(s TestState, r deck.Result) bool {
			return true
		},
		Run: func(s TestState) (deck.Mutation[TestState], error) {
			time.Sleep(500 * time.Millisecond) // will exceed context
			return deck.Complete(func(s *TestState) {
				s.Count = 999
			}), nil
		},
	}

	sut, err := deck.New(suspendCue, slowCue)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// When the deck runs and context expires during drain
	_, runErr := sut.Run(ctx, state)

	// Then an error is returned (context deadline exceeded)
	assert.Error(t, runErr)

	// And the original state is NOT modified (error means no copy-back)
	assert.Equal(t, 42, state.Count, "State should be unchanged when Run returns an error")
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
			assert.True(t, idx1 < idx2, "Cue %s should complete before %s", currKey, nextKey)
		}
	}
}
