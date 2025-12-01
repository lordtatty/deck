package deck_test

import (
	"context"
	"testing"
	"time"

	"github.com/lordtatty/deck"
	"github.com/stretchr/testify/assert"
)

type TestState struct {
	Count int
}

func TestDeck_Run_HappyPath(t *testing.T) {
	// Arrange
	state := &TestState{Count: 1}
	sut := deck.New(state)

	// Add a cue that increments the count if it's 0
	sut.AddCue(deck.Cue[TestState]{
		When: func(s *TestState) bool {
			return s.Count == 1
		},
		Run: func(s *TestState) error {
			s.Count++
			return nil
		},
	})

	// Act
	// Run for a short duration to allow the loop to execute
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := sut.Run(ctx)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 2, state.Count, "Count should be incremented to 2")
}
