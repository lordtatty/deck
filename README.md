# Deck
A streamlined Go package for orchestrating concurrent, state-driven agent execution.

[![Test](https://github.com/lordtatty/deck/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/lordtatty/deck/actions/workflows/test.yml)

## Key Concepts

*   **Input vs State**: Each run has an immutable input `I` (read-only data that defines the run — a user message, request parameters) and a mutable state `S` (progress built up by cues). Input is passed by value to every cue and is never persisted. State is mutated via `Mutation`s and persisted by `Export`/`Import`.
*   **Stateless Deck**: The `Deck` struct is immutable and stateless. Input and state are passed to `Run()`.
*   **Isolated Execution**: `Run()` operates on a copy of the state. External modifications during execution are ignored. On success, the final state is copied back.
*   **Concurrent Cues**: Triggered cues run concurrently. State updates are serialised via mutation functions.
*   **Result History**: `Run()` returns a `Result` struct containing execution history, which cues can inspect.
*   **Suspend/Resume**: Cues can signal suspension for long-running async work. The Deck stops cleanly and can be resumed later.
*   **Export/Import**: Serialise state and result to bytes with `Export()` and restore with `Import()`. Input is **not** in the snapshot — the caller provides it fresh on resume.

## Input vs State — what goes where

`I` is what comes **in**. `S` is what you build **up**.

| Put it in `I` (input) when… | Put it in `S` (state) when… |
|---|---|
| It's set once at the start of the run and never changes | It changes as cues run |
| It defines what this run is *about* (a user message, a job's parameters) | It records progress (results, IDs, fetched data) |
| It must not appear in persisted snapshots (secrets, request context) | It must survive suspend/resume |

Cues must not mutate `I`. Reference fields inside `I` (slices, maps, pointers) are passed by reference, not deep-copied — treat their contents as read-only too. Don't put functions or clients in `I`; inject those via closures over `Cue.Run`.

## Usage

### 1. Define Your Input and State

```go
type ReplyInput struct {
    UserMessage string
}

type ReplyState struct {
    Reply string
}
```

### 2. Create Cues

A `Cue` has a `Name`, an optional `When` predicate, and a `Run` action. `When` and `Run` both receive `input` by value (read-only) and `state` by value (a snapshot). `Run` returns a `Mutation` — use `deck.Complete()` for normal state changes.

```go
reply := deck.Cue[ReplyInput, ReplyState]{
    Name: "Reply",
    When: func(i ReplyInput, s ReplyState, r deck.Result) bool {
        return s.Reply == ""
    },
    Run: func(i ReplyInput, s ReplyState) (deck.Mutation[ReplyState], error) {
        return deck.Complete(func(s *ReplyState) {
            s.Reply = "echo: " + i.UserMessage
        }), nil
    },
}
```

### 3. Initialise and Run

```go
d, err := deck.New(reply)
if err != nil {
    log.Fatal(err)
}

input := ReplyInput{UserMessage: "hello"}
state := &ReplyState{}

result, err := d.Run(context.Background(), input, state)
```

## Examples

### Chain Reaction

Cues trigger other cues by mutating state. `Prepare` runs first, fetching items from the source defined in `input`; `Process` fires once items exist.

```go
type PipelineInput struct {
    Source string
}

type PipelineState struct {
    Items     []string
    Processed bool
}

prepare := deck.Cue[PipelineInput, PipelineState]{
    Name: "Prepare",
    When: func(i PipelineInput, s PipelineState, r deck.Result) bool {
        return len(s.Items) == 0
    },
    Run: func(i PipelineInput, s PipelineState) (deck.Mutation[PipelineState], error) {
        items := fetchItems(i.Source)
        return deck.Complete(func(s *PipelineState) {
            s.Items = items
        }), nil
    },
}

process := deck.Cue[PipelineInput, PipelineState]{
    Name: "Process",
    When: func(i PipelineInput, s PipelineState, r deck.Result) bool {
        return len(s.Items) > 0 && !s.Processed
    },
    Run: func(i PipelineInput, s PipelineState) (deck.Mutation[PipelineState], error) {
        results := transform(s.Items)
        return deck.Complete(func(s *PipelineState) {
            s.Items = results
            s.Processed = true
        }), nil
    },
}

d, _ := deck.New(prepare, process)
input := PipelineInput{Source: "warehouse-A"}
state := &PipelineState{}
result, err := d.Run(ctx, input, state)
```

### Triggering on Execution History

Cues can depend on completion of other cues using `Result.Completed()`, instead of inspecting state.

```go
cleanup := deck.Cue[PipelineInput, PipelineState]{
    Name: "Cleanup",
    When: func(i PipelineInput, s PipelineState, r deck.Result) bool {
        return r.Completed("Process")
    },
    Run: func(i PipelineInput, s PipelineState) (deck.Mutation[PipelineState], error) {
        return deck.Complete(func(s *PipelineState) {
            s.Items = nil // free memory
        }), nil
    },
}
```

### Concurrent Execution

Cues whose `When` predicates are satisfied simultaneously run concurrently. Their mutations are applied serially as each completes, so state stays consistent without locks.

```go
type DashboardInput struct {
    ReportTitle string
}

type DashboardState struct {
    Users   int
    Orders  int
    Revenue float64
    Report  string
}

// These three cues all trigger immediately and run concurrently
fetchUsers := deck.Cue[DashboardInput, DashboardState]{
    Name: "FetchUsers",
    When: func(i DashboardInput, s DashboardState, r deck.Result) bool { return s.Users == 0 },
    Run: func(i DashboardInput, s DashboardState) (deck.Mutation[DashboardState], error) {
        count := db.CountUsers()
        return deck.Complete(func(s *DashboardState) { s.Users = count }), nil
    },
}

fetchOrders := deck.Cue[DashboardInput, DashboardState]{
    Name: "FetchOrders",
    When: func(i DashboardInput, s DashboardState, r deck.Result) bool { return s.Orders == 0 },
    Run: func(i DashboardInput, s DashboardState) (deck.Mutation[DashboardState], error) {
        count := db.CountOrders()
        return deck.Complete(func(s *DashboardState) { s.Orders = count }), nil
    },
}

fetchRevenue := deck.Cue[DashboardInput, DashboardState]{
    Name: "FetchRevenue",
    When: func(i DashboardInput, s DashboardState, r deck.Result) bool { return s.Revenue == 0 },
    Run: func(i DashboardInput, s DashboardState) (deck.Mutation[DashboardState], error) {
        rev := db.SumRevenue()
        return deck.Complete(func(s *DashboardState) { s.Revenue = rev }), nil
    },
}

// Generate the report once all three fetches complete; uses input for the title
generateReport := deck.Cue[DashboardInput, DashboardState]{
    Name: "GenerateReport",
    When: func(i DashboardInput, s DashboardState, r deck.Result) bool {
        return r.Completed("FetchUsers") && r.Completed("FetchOrders") && r.Completed("FetchRevenue")
    },
    Run: func(i DashboardInput, s DashboardState) (deck.Mutation[DashboardState], error) {
        report := fmt.Sprintf("%s — %d users, %d orders, $%.2f", i.ReportTitle, s.Users, s.Orders, s.Revenue)
        return deck.Complete(func(s *DashboardState) { s.Report = report }), nil
    },
}

d, _ := deck.New(fetchUsers, fetchOrders, fetchRevenue, generateReport)
input := DashboardInput{ReportTitle: "Q1 Daily Snapshot"}
state := &DashboardState{}
result, err := d.Run(ctx, input, state)
```

### Suspend and Resume for Long-Running Jobs

When a cue kicks off work that may take a long time (e.g. an API batch job), it returns `deck.Suspended()` instead of `deck.Complete()`. This:

- Applies the mutation to state (e.g. storing a batch ID)
- Does **not** mark the cue as completed
- Signals the Deck to drain other active cues and stop

Use separate cues for submitting and collecting async work. Each cue has a single responsibility, and `When` predicates route execution based on the current state.

```go
type BatchInput struct {
    Prompt string
}

type BatchState struct {
    BatchID string
    Result  string
}

// Cue 1: Submit the batch (only if no batch exists yet)
submitCue := deck.Cue[BatchInput, BatchState]{
    Name: "SubmitBatch",
    When: func(i BatchInput, s BatchState, r deck.Result) bool {
        return s.BatchID == ""
    },
    Run: func(i BatchInput, s BatchState) (deck.Mutation[BatchState], error) {
        batchID, err := openai.SubmitBatch(i.Prompt)
        if err != nil {
            return nil, err
        }
        return deck.Suspended(func(s *BatchState) {
            s.BatchID = batchID
        }), nil
    },
}

// Cue 2: Collect the result (only if batch exists but no result yet)
collectCue := deck.Cue[BatchInput, BatchState]{
    Name: "CollectResult",
    When: func(i BatchInput, s BatchState, r deck.Result) bool {
        return s.BatchID != "" && s.Result == ""
    },
    Run: func(i BatchInput, s BatchState) (deck.Mutation[BatchState], error) {
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

Use `Export()` to serialise **state and result** (not input) into bytes, and `Import()` to restore them. Input is provided fresh on every run.

```go
d, _ := deck.New(submitCue, collectCue)
input := BatchInput{Prompt: "Explain quantum computing"}
state := &BatchState{}

// First run — submitCue fires and suspends with a batch ID stored in state
result, err := d.Run(ctx, input, state)
if result.Suspended {
    // Persist the input separately (it isn't in the snapshot)
    saveInputToRedis(jobID, input)
    // Persist the snapshot — state + result, no input
    data, _ := d.Export(state, result)
    saveSnapshotToRedis(jobID, data)
    return // worker exits
}
```

Later, when a webhook fires or a cron job checks:

```go
// Load the input from wherever it was stored (separate from the snapshot)
input := loadInputFromRedis(jobID)

// Load and restore state + previous result from the snapshot
data := loadSnapshotFromRedis(jobID)
state, prev, _ := d.Import(data)

// Resume — same call shape as a fresh run, just with prev passed in
result, err := d.Run(ctx, input, state, prev)
if result.Suspended {
    data, _ := d.Export(state, result)
    saveSnapshotToRedis(jobID, data) // still not done
    return
}
// Done — state.Result is populated.
```

`Run()` accepts an optional previous `Result`, so fresh runs and resumes use the same call shape:

```go
// Worker handles both new and resume jobs with one code path
input := loadInputFromRedis(msg.JobID) // input is always loaded separately

var state *BatchState
var prev deck.Result
switch msg.Type {
case "new":
    state = &BatchState{}
case "resume":
    state, prev, _ = d.Import(loadSnapshotFromRedis(msg.JobID))
}

// One call — works for both fresh and resumed runs
result, err := d.Run(ctx, input, state, prev)
```

#### Building Safe Worker Flows

When multiple cues suspend concurrently (e.g. an image API and a copywriting API both kick off batch jobs), multiple webhooks arrive independently. Two things to keep in mind:

**Each resume runs the whole Deck, not just one cue.** When a webhook arrives and you resume, the Deck evaluates every pending cue. If the image result is ready but the copy result isn't, the image collect cue completes and the copy collect cue re-suspends. You don't need separate jobs per suspended cue — just resume the whole Deck each time any webhook arrives.

**You must ensure mutual exclusion per job.** If two webhooks arrive simultaneously and two workers both load the same snapshot, you'll get duplicated work. Use a lock to ensure only one worker processes a given job ID at a time:

(The snippet below uses the `examples/jobqueue` types — `ContentState` for mutable progress, with input loaded separately from the snapshot.)

```go
// Webhook handler — store the result and re-enqueue
func handleWebhook(jobID, requestID, resultData string) {
    rdb.Set(ctx, resultKey(requestID), resultData, 0)
    enqueue(QueueMessage{Type: "resume", JobID: jobID})
}

// Worker — lock per job ID prevents concurrent processing
func processJob(msg QueueMessage) {
    lock := acquireLock(msg.JobID)
    if lock == nil {
        // Another worker is already processing this job.
        // Re-enqueue so we retry after the lock is released.
        enqueue(msg)
        return
    }
    defer lock.Release()

    // Always load input — it lives in Redis under inputKey(jobID),
    // separate from the snapshot.
    input := loadInputFromRedis(msg.JobID)

    var state *ContentState
    var prev deck.Result
    switch msg.Type {
    case "new":
        state = &ContentState{}
    case "resume":
        state, prev, _ = d.Import(loadSnapshotFromRedis(msg.JobID))
    }

    result, _ := d.Run(ctx, input, state, prev)

    if result.Suspended {
        data, _ := d.Export(state, result)
        saveSnapshotToRedis(msg.JobID, data)
        return // lock released, next webhook will re-enqueue
    }

    // Done — clean up both keys
    deleteInputFromRedis(msg.JobID)
    deleteSnapshotFromRedis(msg.JobID)
}
```

The typical flow with two concurrent suspends:

1. Producer stores input in Redis and enqueues a "new" message.
2. Worker pops the message, loads input, runs the Deck → both submit cues suspend → exports snapshot.
3. Image webhook → worker acquires lock → loads input + snapshot → resumes Deck → image collected, copy not ready → re-suspends → exports updated snapshot → releases lock.
4. Copy webhook → worker acquires lock → loads input + snapshot → resumes Deck → copy collected → all done → deletes both keys → releases lock.

If both webhooks arrive simultaneously, one worker gets the lock and processes. The second worker waits or retries with the updated snapshot — no duplicated work.

## Runnable Examples

The `examples/` directory contains complete, runnable examples. Each demonstrates the I/S split:

| Example | Input | What it shows |
|---|---|---|
| `examples/basic` | `PipelineInput{Source}` | Three-stage pipeline: Prepare → Process → Summarise |
| `examples/concurrent` | `DashboardInput{ReportTitle}` | Three parallel data fetches + report generation |
| `examples/jobqueue` | `JobInput{JobID, Topic, Style}` | Testcontainers Redis job queue with stateless workers, webhooks, and two concurrent suspending cues — input lives at `jobs:<id>:input`, snapshots at `jobs:<id>:snapshot` |

The jobqueue example has its own `go.mod` to keep heavy dependencies out of the root module. Run it with:

```sh
cd examples/jobqueue && go run .
```
