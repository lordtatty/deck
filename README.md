# Deck

[![Test](https://github.com/lordtatty/deck/actions/workflows/test.yml/badge.svg)](https://github.com/lordtatty/deck/actions/workflows/test.yml)

## Key Concepts

*   **Stateless Deck**: The `Deck` struct is immutable and stateless. State is passed to `Run()`.
*   **Isolated Execution**: `Run()` operates on a copy of the state. External modifications are ignored. On success, the final state is copied back.
*   **Concurrent Cues**: Triggered cues run concurrently. State updates are serialized via mutation functions.
*   **Result History**: `Run()` returns a `Result` struct containing execution history, which cues can inspect.

## Usage

### 1. Define Your State
```go
type MyState struct {
    Count int
    Done  bool
}
```

### 2. Create Cues
A `Cue` consists of a `Name`, a `When` predicate, and a `Run` action.

```go
cue := deck.Cue[MyState]{
    Name: "Incrementer",
    When: func(s MyState, r deck.Result) bool {
        return s.Count < 5 && !r.Completed("Stopper")
    },
    Run: func(s MyState) (func(*MyState), error) {
        // Perform actions then return mutation function
        return func(s *MyState) {
            s.Count++
        }, nil
    },
}
```

### 3. Initialize and Run
```go
// Create Deck (immutable configuration)
d, err := deck.New(cue1, cue2)
if err != nil {
    log.Fatal(err)
}

// Initialize State
state := &MyState{Count: 0}

// Run (blocks until stable or cancelled)
// Returns Result containing execution history
result, err := d.Run(context.Background(), state)
```