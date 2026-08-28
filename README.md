# Deck
A streamlined Go package for orchestrating concurrent, state-driven agent execution.

[![Test](https://github.com/lordtatty/deck/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/lordtatty/deck/actions/workflows/test.yml)
[![Lint](https://github.com/lordtatty/deck/actions/workflows/lint.yml/badge.svg?branch=main)](https://github.com/lordtatty/deck/actions/workflows/lint.yml)

## Key Concepts

*   **Input vs State**: Each run has an immutable input `I` (read-only data that defines the run — a user message, request parameters) and a mutable state `S` (progress built up by cues). Input is passed by value to every cue and is never persisted. State is mutated via `Mutation`s and persisted by `Export`/`Import`.
*   **Stateless Deck**: A `Deck` holds no run state — input and state are passed to `Run()`, and a run never writes back to the Deck. The one field you may set is `Engine`.
*   **Isolated Execution**: `Run()` operates on a copy of the state. External modifications during execution are ignored. On success, the final state is copied back.
*   **Concurrent Cues**: Triggered cues run concurrently. State updates are serialised via mutation functions.
*   **Engine**: Where cues run. Leave it nil for goroutines and the wall clock, or set it to run one at a time for a reproducible test — or inside a Temporal workflow, where the flow becomes durable.
*   **Declared Work**: A cue can perform its work itself, or describe it with `deck.Do` and let the Engine perform it. Describing it is what lets one set of cues run both locally and durably.
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

### Where Cues Run — the Engine

By default a Deck runs each cue on its own goroutine and reads the wall clock. `Engine` changes that without touching a single cue:

```go
d, _ := deck.New(cues...)

d.Engine = nil            // the default: a goroutine per cue, wall clock
d.Engine = deck.Serial()  // one at a time, in registration order — reproducible
```

`deck.Serial()` is what you want in a test that asserts on ordering: it runs each cue inline to completion, so the same run happens the same way every time. `deck.WithClock(engine, now)` layers a fixed clock onto any engine so `CompletedCue` timestamps are predictable too.

Anything satisfying the `Engine` interface will do, so you can supply your own. The contract is deliberately small, and nothing in it is specific to any one system — an engine has to:

1. give a clock (`Now`)
2. run a closure and say when it is done (`Spawn`)
3. take a function, an argument and somewhere to put the result, run it *somewhere*, and say when it is done (`Execute`)
4. block until at least one outstanding piece of work is done (`Await`)

`Await` is handed the outstanding handles rather than a condition to evaluate, because "wait for any of these" is the primitive durable-execution engines tend to offer. `deck.AnyReady(futures)` is there for engines that would rather poll.

`deck/temporal` is one implementation of that contract, in under 150 lines. The core has no dependency on it, or on anything else.

### Declaring Work Instead of Doing It

A cue that *does* its work is tied to the process it runs in. A cue that *declares* it can run anywhere:

```go
// An ordinary Go function. It is also exactly the shape of a Temporal activity.
func FetchUser(ctx context.Context, id string) (User, error) { ... }

Run: func(in Input, s State) (deck.Mutation[State], error) {
    return deck.Do(FetchUser, in.UserID, func(u User) deck.Mutation[State] {
        return deck.Complete(func(s *State) { s.User = u })
    }), nil
},
```

`deck.Do` returns immediately — it hands the work to the Engine and lets the Deck carry on triggering other cues. That is where the parallelism comes from, and it is why the same cue works whether the Engine runs the function on a goroutine or dispatches it to a worker on another machine.

#### `Do` or `Complete`?

Not every cue should declare work. Most flows are a mixture:

| Use | When | Cost under Temporal |
|---|---|---|
| `deck.Do` | The work leaves the process, is slow, or is not deterministic — a network call, a database query, an LLM request | One activity: retried, timed out and recorded independently |
| `deck.Complete` | The cue only computes from state it already holds | None. It runs inline as workflow code |

A join cue that formats a string from what the others fetched should be a `Complete`. Making it a `Do` would cost a round trip to the Temporal server and an entry in the workflow history to run a `Sprintf`, and would gain nothing — there is nothing there that can fail, time out, or need retrying.

The distinction is load-bearing under Temporal, and harmless without it: with the default Engine, `Do` simply runs the function on a goroutine.

One thing to be careful of: a cue that does slow or non-deterministic work **inline in `Run`** is fine in plain Go, but under Temporal it runs on the workflow coroutine, where it will break replay or trip the deadlock detector. That is what `Do` exists to avoid.

### Running Durably with Temporal

`deck/temporal` is a separate module, so `go get` on the core pulls no Temporal dependency. Attaching it is one visible line:

```go
func MyWorkflow(ctx workflow.Context, in Input) (State, error) {
    // Activity settings are yours to choose; deck will not invent them.
    ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
        StartToCloseTimeout: time.Minute,
    })

    d, err := deck.New(cues...)
    if err != nil {
        return State{}, err
    }
    d.Engine = decktemporal.New(ctx)

    var state State
    _, err = d.Run(context.Background(), in, &state)
    return state, err
}
```

Work declared with `deck.Do` becomes a Temporal activity — register the same functions with your worker and Temporal resolves them by name. Cues still run in parallel: independent activities all go out before any of them is waited on.

Where one cue needs different activity settings from the rest, name it in the wiring rather than in the flow:

```go
d.Engine = decktemporal.New(ctx,
    decktemporal.ForCue("ship", workflow.ActivityOptions{
        StartToCloseTimeout: 10 * time.Minute,
    }),
)
```

Three things worth knowing:

- **Cancellation** reaches the Deck through the Engine's workflow context, not through the context passed to `Run`. Pass `context.Background()` there.
- **A cue's `Run` executes inline** on the workflow coroutine, so it must not block. Declare work with `deck.Do`; a cue that blocks will trip Temporal's deadlock detector.
- **`CompletedCue` timestamps** come from `workflow.Now`, which reports when the current workflow task started. A cue that starts and finishes inside one task reports a zero `Duration`.

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

The [`examples/`](examples) directory contains complete, runnable examples, each with its own module. [`examples/README.md`](examples/README.md) indexes them.

**Plain deck**, no extra dependencies:

| Example | What it shows |
|---|---|
| `examples/basic` | Three-stage pipeline: Prepare → Process → Summarise |
| `examples/concurrent` | Three parallel data fetches + report generation |
| `examples/engines` | One set of cues under two Engines, side by side: concurrent and shortest-first, versus inline and reproducible. Also where to reach for `Do` and where for `Complete`. |
| `examples/jobqueue` | Testcontainers Redis job queue with stateless workers, webhooks, and two concurrent suspending cues |

**With Temporal.** Three examples, because they answer three different questions — pick the one matching what you are trying to work out:

| Example | The question it answers |
|---|---|
| `examples/portable` | *"Will my cues really run unchanged in both worlds?"* The same flow run locally and then on Temporal, back to back, reaching the same answer. The simplest possible demonstration — no indirection to read past. |
| `examples/eitherway` | *"How do I build one service that can do either?"* The same idea as `portable`, but arranged the way you would actually ship it: a `Runner` interface, and the choice made once at startup from a flag. |
| `examples/temporal` | *"What does Temporal actually give me?"* Order fulfilment on Temporal alone: a payment that fails twice and is retried with no retry code in the flow, per-cue timeouts via `ForCue`, and a Web UI to read the history. |

Read them in that order if you are new to the pairing: `portable` shows it works, `eitherway` shows how to arrange it, `temporal` shows what you gain by it.

Each example is its own module, keeping heavy dependencies out of the root. Run any of them with:

```sh
cd examples/engines && go run .
```

The Temporal examples start a throwaway dev server themselves, so there is nothing to install first.
