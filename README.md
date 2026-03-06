# Deck
A streamlined Go package for orchestrating concurrent, state-driven agent execution.

[![Test](https://github.com/lordtatty/deck/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/lordtatty/deck/actions/workflows/test.yml)

## Key Concepts

*   **Stateless Deck**: The `Deck` struct is immutable and stateless. State is passed to `Run()`.
*   **Isolated Execution**: `Run()` operates on a copy of the state. External modifications are ignored. On success, the final state is copied back.
*   **Concurrent Cues**: Triggered cues run concurrently. State updates are serialized via mutation functions.
*   **Result History**: `Run()` returns a `Result` struct containing execution history, which cues can inspect.
*   **Suspend/Resume**: Cues can signal suspension for long-running async work. The Deck stops cleanly and can be resumed later.
*   **Export/Import**: Serialize the full Deck state to bytes with `Export()` and restore it with `Import()` for durable persistence between suspend/resume cycles.

## Usage

### 1. Define Your State
```go
type MyState struct {
    Count int
    Done  bool
}
```

### 2. Create Cues
A `Cue` consists of a `Name`, a `When` predicate, and a `Run` action. `Run` returns a `Mutation` — use `deck.Complete()` for normal state changes.

```go
cue := deck.Cue[MyState]{
    Name: "Incrementer",
    When: func(s MyState, r deck.Result) bool {
        return s.Count < 5 && !r.Completed("Stopper")
    },
    Run: func(s MyState) (deck.Mutation[MyState], error) {
        return deck.Complete(func(s *MyState) {
            s.Count++
        }), nil
    },
}
```

### 3. Initialize and Run
```go
d, err := deck.New(cue1, cue2)
if err != nil {
    log.Fatal(err)
}

state := &MyState{Count: 0}

result, err := d.Run(context.Background(), state)
```

## Examples

### Chain Reaction

Cues can trigger other cues by mutating state. Here, `Prepare` runs first and sets up data, then `Process` fires when the data is ready.

```go
type PipelineState struct {
    Data      []string
    Processed bool
}

prepare := deck.Cue[PipelineState]{
    Name: "Prepare",
    When: func(s PipelineState, r deck.Result) bool {
        return len(s.Data) == 0
    },
    Run: func(s PipelineState) (deck.Mutation[PipelineState], error) {
        items := fetchItems()
        return deck.Complete(func(s *PipelineState) {
            s.Data = items
        }), nil
    },
}

process := deck.Cue[PipelineState]{
    Name: "Process",
    When: func(s PipelineState, r deck.Result) bool {
        return len(s.Data) > 0 && !s.Processed
    },
    Run: func(s PipelineState) (deck.Mutation[PipelineState], error) {
        results := transform(s.Data)
        return deck.Complete(func(s *PipelineState) {
            s.Data = results
            s.Processed = true
        }), nil
    },
}

d, _ := deck.New(prepare, process)
state := &PipelineState{}
result, err := d.Run(ctx, state)
```

### Triggering on Execution History

Cues can depend on the completion of other cues using `Result.Completed()`, rather than checking state.

```go
cleanup := deck.Cue[PipelineState]{
    Name: "Cleanup",
    When: func(s PipelineState, r deck.Result) bool {
        return r.Completed("Process")
    },
    Run: func(s PipelineState) (deck.Mutation[PipelineState], error) {
        return deck.Complete(func(s *PipelineState) {
            s.Data = nil // free memory
        }), nil
    },
}
```

### Concurrent Execution

Cues whose `When` predicates are satisfied simultaneously run concurrently. Their mutations are applied serially as each completes, ensuring state consistency without locks.

```go
type AnalyticsState struct {
    Users    int
    Orders   int
    Revenue  float64
}

// These three cues all trigger immediately and run concurrently
fetchUsers := deck.Cue[AnalyticsState]{
    Name: "FetchUsers",
    When: func(s AnalyticsState, r deck.Result) bool { return s.Users == 0 },
    Run: func(s AnalyticsState) (deck.Mutation[AnalyticsState], error) {
        count := db.CountUsers()
        return deck.Complete(func(s *AnalyticsState) { s.Users = count }), nil
    },
}

fetchOrders := deck.Cue[AnalyticsState]{
    Name: "FetchOrders",
    When: func(s AnalyticsState, r deck.Result) bool { return s.Orders == 0 },
    Run: func(s AnalyticsState) (deck.Mutation[AnalyticsState], error) {
        count := db.CountOrders()
        return deck.Complete(func(s *AnalyticsState) { s.Orders = count }), nil
    },
}

fetchRevenue := deck.Cue[AnalyticsState]{
    Name: "FetchRevenue",
    When: func(s AnalyticsState, r deck.Result) bool { return s.Revenue == 0 },
    Run: func(s AnalyticsState) (deck.Mutation[AnalyticsState], error) {
        rev := db.SumRevenue()
        return deck.Complete(func(s *AnalyticsState) { s.Revenue = rev }), nil
    },
}

d, _ := deck.New(fetchUsers, fetchOrders, fetchRevenue)
state := &AnalyticsState{}
result, err := d.Run(ctx, state)
// All three fetches ran concurrently; state is fully populated
```

### Suspend and Resume for Long-Running Jobs

When a cue kicks off work that may take a long time (e.g. an API batch job), it can return `deck.Suspended()` instead of `deck.Complete()`. This:

- Applies the mutation to state (e.g. storing a batch ID)
- Does **not** mark the cue as completed
- Signals the Deck to drain all other active cues and stop

Use separate cues for submitting and collecting async work. Each cue has a single responsibility, and `When` predicates route execution based on the current state.

```go
type BatchState struct {
    Prompt  string
    BatchID string
    Result  string
}

// Cue 1: Submit the batch (only if no batch exists yet)
submitCue := deck.Cue[BatchState]{
    Name: "SubmitBatch",
    When: func(s BatchState, r deck.Result) bool {
        return s.Prompt != "" && s.BatchID == ""
    },
    Run: func(s BatchState) (deck.Mutation[BatchState], error) {
        batchID, err := openai.SubmitBatch(s.Prompt)
        if err != nil {
            return nil, err
        }
        return deck.Suspended(func(s *BatchState) {
            s.BatchID = batchID
        }), nil
    },
}

// Cue 2: Collect the result (only if batch exists but no result yet)
collectCue := deck.Cue[BatchState]{
    Name: "CollectResult",
    When: func(s BatchState, r deck.Result) bool {
        return s.BatchID != "" && s.Result == ""
    },
    Run: func(s BatchState) (deck.Mutation[BatchState], error) {
        result, err := openai.GetBatchResult(s.BatchID)
        if err != nil {
            return nil, err
        }
        return deck.Complete(func(s *BatchState) {
            s.Result = result
        }), nil
    },
}
```

#### Export and Import

Use `Export()` to serialize the full Deck state (state + result) to bytes, and `Import()` to restore it. This keeps persistence simple — one opaque blob to store, one call to restore.

```go
d, _ := deck.New(setupCue, submitCue, collectCue)
state := &BatchState{Prompt: "Explain quantum computing"}

// First run — setupCue completes, submitCue fires and suspends
result, err := d.Run(ctx, state)
if result.Suspended {
    // Export everything needed to resume later
    data, _ := d.Export(state, result)
    saveToRedis(jobID, data)
    return // worker exits
}
```

Later, when a webhook fires or a cron job checks:

```go
// Load the exported snapshot
data := loadFromRedis(jobID)

// Import restores state and previous result
state, prev, _ := d.Import(data)

// Resume — same call as a fresh run, just with previous context
result, err := d.Resume(ctx, state, prev)
if result.Suspended {
    data, _ := d.Export(state, result)
    saveToRedis(jobID, data) // still not done
    return
}
// Done! state.Result is populated.
```

Since `Run()` is just `Resume()` with an empty result, you can unify both paths:

```go
// Worker handles both new and resume jobs with one code path
var state *BatchState
var prev deck.Result

switch msg.Type {
case "new":
    state = &BatchState{Prompt: msg.Prompt}
case "resume":
    state, prev, _ = d.Import(loadFromRedis(msg.JobID))
}

// One call — works for both fresh and resumed runs
result, err := d.Resume(ctx, state, prev)
```

## Runnable Examples

The `examples/` directory contains complete, runnable examples:

| Example | Description |
|---|---|
| `examples/basic` | Simple three-stage pipeline: Prepare → Process → Summarise |
| `examples/concurrent` | Three parallel data fetches + report generation |
| `examples/jobqueue` | Testcontainers Redis job queue with stateless workers, webhooks, and two concurrent suspending cues |

The jobqueue example has its own `go.mod` to keep heavy dependencies out of the root module. Run it with:

```sh
cd examples/jobqueue && go run .
```
