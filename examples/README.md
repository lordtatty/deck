# Examples

Each directory is its own Go module, so the heavy dependencies stay out of the
core. Run any of them with:

```sh
cd examples/<name> && go run .
```

## Plain deck

Nothing to install.

| Example | For when you are asking |
|---|---|
| [`basic`](basic) | *How does a flow fit together at all?* A three-stage pipeline: Prepare → Process → Summarise, each cue triggered by the state the last one left. |
| [`concurrent`](concurrent) | *How do cues run in parallel?* Three independent fetches go at once; a fourth waits for all three and writes the report. |
| [`engines`](engines) | *What is an Engine, and when should a cue use `Do` rather than `Complete`?* One set of cues run under two Engines, side by side, so you can see the difference in ordering and duration. |
| [`jobqueue`](jobqueue) | *How do I survive a process restart without a workflow engine?* A Redis job queue with stateless workers, webhooks, and two cues suspending at once. Needs Docker. |

## With Temporal

These start a throwaway Temporal dev server themselves — no Docker, and nothing
to install. The server binary is downloaded once and cached by the SDK.

| Example | For when you are asking |
|---|---|
| [`portable`](portable) | *Will my cues really run unchanged in both worlds?* The same flow run locally and then on Temporal, back to back, reaching the same answer. The simplest demonstration, with no indirection to read past. |
| [`eitherway`](eitherway) | *How do I build one service that can do either?* The same idea, arranged the way you would ship it: a `Runner` interface, and the choice made once at startup. `-mode inline\|temporal\|both`. |
| [`temporal`](temporal) | *What does Temporal actually give me?* Order fulfilment on Temporal alone: a payment that fails twice and is retried with no retry code in the flow, per-cue timeouts via `ForCue`, and a Web UI to read the history. `-ui` keeps it open. |

New to the pairing? Read them in that order: `portable` shows it works,
`eitherway` shows how to arrange it, `temporal` shows what you gain by it.

## The thing worth noticing

In every Temporal example the cues live in a `flow` package that imports `deck`
and nothing else — no Temporal, no branching on environment. Open one of those
first. The flow is the part that stays the same; everything else is wiring.

The other thing to look for is which cues declare their work with `deck.Do` and
which return `deck.Complete`. It is never all of them. `Do` is for work that
leaves the process — a call, a query, a model — and becomes a retryable,
recorded unit under a durable engine. `Complete` is for a cue that only computes
from state it already holds, and costs nothing. `eitherway` has three of one and
two of the other, which is the usual shape.
