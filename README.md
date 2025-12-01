# Deck

A streamlined Go package for orchestrating AI agents with trigger-based execution.

## What is Deck?

Deck manages multiple AI agents through a simple loop: each iteration checks which agents should run based on the current state, then executes them. Agents are added as **Cues** - a combination of a trigger condition and the agent to run when that condition is met.

## Installation
```bash
go get github.com/yourusername/deck
```

## Quick Start
```go
package main

import "github.com/yourusername/deck"

// Define your state
type MyState struct {
    NeedsSummary bool
    Summary      string
    Messages     []string
}

func main() {
    // Create your state
    state := &MyState{
        NeedsSummary: true,
        Messages:     []string{"Hello", "World"},
    }
    
    // Create deck with your state type
    d := deck.New[MyState](state)
    
    // Add a cue: runs when the trigger returns true
    d.AddCue(deck.Cue[MyState]{
        When: func(s *MyState) bool {
            return s.NeedsSummary
        },
        Run: func(s *MyState) error {
            // Your agent logic here
            s.Summary = generateSummary(s.Messages)
            s.NeedsSummary = false
            return nil
        },
    })
    
    // Start the deck
    d.Run()
}
```

## Key Concepts

- **State**: Your own struct containing whatever data your agents need
- **Cue**: A trigger condition + agent action pair that's type-safe to your state
- **Loop**: Each iteration evaluates all cues and runs matching agents

## License

MIT