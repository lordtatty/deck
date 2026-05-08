package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/lordtatty/deck"
)

// This example simulates a realistic job queue pipeline for generating
// marketing content. Workers are stateless and ephemeral — they pick up
// a job, run or resume a Deck, and exit. Webhooks re-enqueue resume jobs.
//
// The flow:
//
//  1. Producer stores the job's immutable input (jobs:<id>:input) and
//     enqueues a "new" message
//  2. Worker picks it up, loads input, runs the Deck — two API calls suspend
//  3. Worker exports STATE (not input) to Redis (jobs:<id>:snapshot), exits
//  4. External APIs complete and hit a webhook endpoint
//  5. Webhook handler stores the result, re-enqueues a resume job
//  6. A new worker picks up the resume job, loads input AND snapshot, resumes
//  7. If still waiting on other APIs → export state, exit again
//  8. When all results are in → final cue publishes content; both keys cleaned up
//
// Note the I/S split: JobInput is immutable per-job parameters that live in a
// separate Redis key. ContentState is mutable progress that's persisted via
// Export/Import. Snapshots never contain input — that's the contract.

// QueueMessage is what goes on the work queue. It carries only the job ID;
// the input itself lives in Redis at inputKey(jobID).
type QueueMessage struct {
	Type  string `json:"type"` // "new" or "resume"
	JobID string `json:"job_id"`
}

// JobInput is the immutable input for a job: who/what we're generating for.
// Stored once at job creation, read by every cue, never persisted via Export.
type JobInput struct {
	JobID string `json:"job_id"`
	Topic string `json:"topic"`
	Style string `json:"style"`
}

// ContentState is the mutable progress as the pipeline runs. Persisted via
// Export when the Deck suspends.
type ContentState struct {
	Validated bool `json:"validated"`

	ImageRequestID string `json:"image_request_id,omitempty"`
	ImageURL       string `json:"image_url,omitempty"`

	CopyRequestID string `json:"copy_request_id,omitempty"`
	CopyText      string `json:"copy_text,omitempty"`

	PublishedURL string `json:"published_url,omitempty"`
}

const (
	queueKey       = "jobs:queue"
	webhookChannel = "jobs:webhooks"
)

func inputKey(jobID string) string    { return fmt.Sprintf("jobs:%s:input", jobID) }
func snapshotKey(jobID string) string { return fmt.Sprintf("jobs:%s:snapshot", jobID) }
func resultKey(requestID string) string {
	return fmt.Sprintf("results:%s", requestID)
}

func main() {
	ctx := context.Background()

	// --- Start Redis via testcontainers ---
	fmt.Println("Starting Redis container...")
	redisContainer, err := tcredis.Run(ctx,
		"redis:7-alpine",
		testcontainers.WithWaitStrategy(
			wait.ForLog("Ready to accept connections").
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		log.Fatalf("Failed to start Redis: %v", err)
	}
	defer redisContainer.Terminate(ctx)

	connStr, err := redisContainer.ConnectionString(ctx)
	if err != nil {
		log.Fatalf("Failed to get connection string: %v", err)
	}
	opts, err := redis.ParseURL(connStr)
	if err != nil {
		log.Fatalf("Failed to parse Redis URL: %v", err)
	}
	rdb := redis.NewClient(opts)
	defer rdb.Close()
	fmt.Println("Redis ready.")
	fmt.Println()

	d := buildDeck(rdb)
	completed := make(chan struct{})

	// --- Start the worker pool (processes both new and resume jobs) ---
	go workerLoop(ctx, rdb, d, completed)

	// --- Start the webhook handler (re-enqueues resume jobs) ---
	go webhookHandler(ctx, rdb)

	// Give workers time to start listening
	time.Sleep(200 * time.Millisecond)

	// --- Producer: store input + enqueue a new job ---
	fmt.Println("=== Producer: Enqueuing Job ===")
	jobID := uuid.New().String()[:8]
	input := JobInput{
		JobID: jobID,
		Topic: "Why Go is great for building concurrent systems",
		Style: "professional blog post",
	}
	storeInput(ctx, rdb, input)
	enqueue(ctx, rdb, QueueMessage{Type: "new", JobID: jobID})
	fmt.Println()

	// --- Simulate external API completions ---

	// Image generation completes after 1.5s
	go func() {
		time.Sleep(1500 * time.Millisecond)
		reqID := waitForStateField(ctx, rdb, d, jobID, "image_request_id")
		if reqID == "" {
			return // ctx cancelled
		}
		fmt.Printf("  [ImageGen API] Request %s complete, posting webhook\n", reqID)
		rdb.Set(ctx, resultKey(reqID), "https://cdn.localhost/images/go-concurrency-hero.png", 0)
		rdb.Publish(ctx, webhookChannel, fmt.Sprintf(`{"job_id":"%s","type":"image_complete"}`, jobID))
	}()

	// Copywriting completes after 3s
	go func() {
		time.Sleep(3000 * time.Millisecond)
		reqID := waitForStateField(ctx, rdb, d, jobID, "copy_request_id")
		if reqID == "" {
			return // ctx cancelled
		}
		fmt.Printf("  [CopyGen API] Request %s complete, posting webhook\n", reqID)
		copyText := `Go's goroutines and channels make concurrent programming intuitive and safe. ` +
			`Unlike thread-based models, Go's lightweight goroutines let you spin up thousands of ` +
			`concurrent tasks with minimal overhead. Combined with channels for safe communication, ` +
			`Go eliminates entire classes of concurrency bugs at compile time.`
		rdb.Set(ctx, resultKey(reqID), copyText, 0)
		rdb.Publish(ctx, webhookChannel, fmt.Sprintf(`{"job_id":"%s","type":"copy_complete"}`, jobID))
	}()

	select {
	case <-completed:
	case <-time.After(30 * time.Second):
		log.Fatal("Timed out")
	}
}

// storeInput persists the immutable input for a job. Producers call this once
// at job creation; workers read it on every run (new or resume).
func storeInput(ctx context.Context, rdb *redis.Client, input JobInput) {
	data, err := json.Marshal(input)
	if err != nil {
		log.Fatalf("Producer: failed to marshal input: %v", err)
	}
	if err := rdb.Set(ctx, inputKey(input.JobID), data, 0).Err(); err != nil {
		log.Fatalf("Producer: failed to store input: %v", err)
	}
	fmt.Printf("  Input stored at %s (topic=%q, style=%q)\n", inputKey(input.JobID), input.Topic, input.Style)
}

// loadInput fetches a job's immutable input from Redis.
func loadInput(ctx context.Context, rdb *redis.Client, jobID string) (JobInput, error) {
	data, err := rdb.Get(ctx, inputKey(jobID)).Bytes()
	if err != nil {
		return JobInput{}, fmt.Errorf("load input for %s: %w", jobID, err)
	}
	var input JobInput
	if err := json.Unmarshal(data, &input); err != nil {
		return JobInput{}, fmt.Errorf("unmarshal input for %s: %w", jobID, err)
	}
	return input, nil
}

// enqueue pushes a message onto the work queue.
func enqueue(ctx context.Context, rdb *redis.Client, msg QueueMessage) {
	data, err := json.Marshal(msg)
	if err != nil {
		log.Fatalf("enqueue: marshal failed: %v", err)
	}
	if err := rdb.LPush(ctx, queueKey, data).Err(); err != nil {
		log.Fatalf("enqueue: LPush failed: %v", err)
	}
	fmt.Printf("  Enqueued: type=%s job=%s\n", msg.Type, msg.JobID)
}

// workerLoop continuously pops jobs from the queue and processes them.
// Each iteration is a stateless unit of work — the worker has no memory
// between jobs.
func workerLoop(ctx context.Context, rdb *redis.Client, d *deck.Deck[JobInput, ContentState], completed chan<- struct{}) {
	for {
		fmt.Println("  [Worker] Waiting for next job...")
		popped, err := rdb.BRPop(ctx, 30*time.Second, queueKey).Result()
		if err != nil {
			log.Fatalf("Worker: queue pop failed: %v", err)
		}

		var msg QueueMessage
		if err := json.Unmarshal([]byte(popped[1]), &msg); err != nil {
			log.Fatalf("Worker: bad message: %v", err)
		}

		fmt.Printf("\n  [Worker] Picked up %s job for %s\n", msg.Type, msg.JobID)

		// Always load the immutable input for this job — it lives in Redis,
		// not in the snapshot, and the worker provides it fresh on every run.
		input, err := loadInput(ctx, rdb, msg.JobID)
		if err != nil {
			log.Fatalf("Worker: %v", err)
		}

		// Build state and previous result depending on whether this is a fresh
		// run or a resume.
		var state *ContentState
		var prev deck.Result

		switch msg.Type {
		case "new":
			state = &ContentState{}
		case "resume":
			exportData, loadErr := rdb.Get(ctx, snapshotKey(msg.JobID)).Bytes()
			if loadErr != nil {
				log.Fatalf("Worker: failed to load snapshot for %s: %v", msg.JobID, loadErr)
			}
			state, prev, err = d.Import(exportData)
			if err != nil {
				log.Fatalf("Worker: failed to import snapshot for %s: %v", msg.JobID, err)
			}
		default:
			log.Fatalf("Worker: unknown message type %q for job %s", msg.Type, msg.JobID)
		}

		// One code path — Run handles both fresh (empty prev) and continued runs.
		// Input is passed every time; state + prev only matter on resume.
		fmt.Println()
		fmt.Println("=== Worker: Running Deck ===")
		result, err := d.Run(ctx, input, state, prev)
		if err != nil {
			log.Fatalf("Worker: deck failed for %s: %v", msg.JobID, err)
		}

		if result.Suspended {
			// Export state and exit — worker is done, webhook will re-enqueue.
			// Note: input is NOT in this snapshot. It's already in Redis at
			// inputKey() and the next worker will load it fresh.
			fmt.Println()
			fmt.Println("  [Worker] Deck suspended — exporting state and exiting")
			data, exportErr := d.Export(state, result)
			if exportErr != nil {
				log.Fatalf("Worker: export failed: %v", exportErr)
			}
			if err := rdb.Set(ctx, snapshotKey(msg.JobID), data, 0).Err(); err != nil {
				log.Fatalf("Worker: failed to save snapshot for %s: %v", msg.JobID, err)
			}
			fmt.Println("  [Worker] State saved. Worker exiting, waiting for webhook.")
			continue
		}

		// Complete — clean up both keys (input + snapshot) and signal done.
		if err := rdb.Del(ctx, snapshotKey(msg.JobID), inputKey(msg.JobID)).Err(); err != nil {
			log.Fatalf("Worker: failed to clean up keys for %s: %v", msg.JobID, err)
		}
		printComplete(input, state, result)
		close(completed)
		return
	}
}

// webhookHandler listens for external API completion events and re-enqueues
// resume jobs. In production this would be an HTTP endpoint; here we
// simulate it with Redis pub/sub.
func webhookHandler(ctx context.Context, rdb *redis.Client) {
	sub := rdb.Subscribe(ctx, webhookChannel)
	defer sub.Close()

	for msg := range sub.Channel() {
		var webhook struct {
			JobID string `json:"job_id"`
			Type  string `json:"type"`
		}
		if err := json.Unmarshal([]byte(msg.Payload), &webhook); err != nil {
			log.Printf("Webhook: bad payload: %v", err)
			continue
		}

		fmt.Println()
		fmt.Printf("=== Webhook Received: %s (job %s) ===\n", webhook.Type, webhook.JobID)
		fmt.Println("  [Webhook] Re-enqueuing resume job...")

		enqueue(ctx, rdb, QueueMessage{
			Type:  "resume",
			JobID: webhook.JobID,
		})
	}
}

// buildDeck creates the content generation pipeline.
func buildDeck(rdb *redis.Client) *deck.Deck[JobInput, ContentState] {
	cues := []deck.Cue[JobInput, ContentState]{
		{
			Name: "ValidateRequest",
			When: func(i JobInput, s ContentState, r deck.Result) bool {
				return !s.Validated
			},
			Run: func(i JobInput, s ContentState) (deck.Mutation[ContentState], error) {
				fmt.Printf("  [ValidateRequest] Validating job %s...\n", i.JobID)
				if i.Topic == "" {
					return nil, fmt.Errorf("topic is required")
				}
				if i.Style == "" {
					return nil, fmt.Errorf("style is required")
				}
				fmt.Println("  [ValidateRequest] Valid.")
				return deck.Complete(func(s *ContentState) {
					s.Validated = true
				}), nil
			},
		},
		{
			Name: "SubmitImageGeneration",
			When: func(i JobInput, s ContentState, r deck.Result) bool {
				return s.Validated && s.ImageRequestID == ""
			},
			Run: func(i JobInput, s ContentState) (deck.Mutation[ContentState], error) {
				requestID := fmt.Sprintf("img_%s", uuid.New().String()[:8])
				fmt.Printf("  [SubmitImageGeneration] Requesting image for %q — request %s\n", i.Topic, requestID)
				return deck.Suspended(func(s *ContentState) {
					s.ImageRequestID = requestID
				}), nil
			},
		},
		{
			Name: "SubmitCopyGeneration",
			When: func(i JobInput, s ContentState, r deck.Result) bool {
				return s.Validated && s.CopyRequestID == ""
			},
			Run: func(i JobInput, s ContentState) (deck.Mutation[ContentState], error) {
				requestID := fmt.Sprintf("copy_%s", uuid.New().String()[:8])
				fmt.Printf("  [SubmitCopyGeneration] Requesting %s — request %s\n", i.Style, requestID)
				return deck.Suspended(func(s *ContentState) {
					s.CopyRequestID = requestID
				}), nil
			},
		},
		{
			Name: "CollectImage",
			When: func(i JobInput, s ContentState, r deck.Result) bool {
				return s.ImageRequestID != "" && s.ImageURL == ""
			},
			Run: func(i JobInput, s ContentState) (deck.Mutation[ContentState], error) {
				ctx := context.Background()
				url, err := rdb.Get(ctx, resultKey(s.ImageRequestID)).Result()
				if err == redis.Nil {
					fmt.Printf("  [CollectImage] Request %s not ready yet — suspending\n", s.ImageRequestID)
					return deck.Suspended(func(s *ContentState) {}), nil
				}
				if err != nil {
					return nil, fmt.Errorf("failed to check image result: %w", err)
				}
				fmt.Printf("  [CollectImage] Got image: %s\n", url)
				return deck.Complete(func(s *ContentState) {
					s.ImageURL = url
				}), nil
			},
		},
		{
			Name: "CollectCopy",
			When: func(i JobInput, s ContentState, r deck.Result) bool {
				return s.CopyRequestID != "" && s.CopyText == ""
			},
			Run: func(i JobInput, s ContentState) (deck.Mutation[ContentState], error) {
				ctx := context.Background()
				text, err := rdb.Get(ctx, resultKey(s.CopyRequestID)).Result()
				if err == redis.Nil {
					fmt.Printf("  [CollectCopy] Request %s not ready yet — suspending\n", s.CopyRequestID)
					return deck.Suspended(func(s *ContentState) {}), nil
				}
				if err != nil {
					return nil, fmt.Errorf("failed to check copy result: %w", err)
				}
				fmt.Printf("  [CollectCopy] Got copy (%d chars)\n", len(text))
				return deck.Complete(func(s *ContentState) {
					s.CopyText = text
				}), nil
			},
		},
		{
			Name: "PublishContent",
			When: func(i JobInput, s ContentState, r deck.Result) bool {
				return s.ImageURL != "" && s.CopyText != "" && s.PublishedURL == ""
			},
			Run: func(i JobInput, s ContentState) (deck.Mutation[ContentState], error) {
				fmt.Println("  [PublishContent] Assembling and publishing content...")
				publishedURL := fmt.Sprintf("https://localhost/posts/%s", i.JobID)
				fmt.Printf("  [PublishContent] Published to %s\n", publishedURL)
				return deck.Complete(func(s *ContentState) {
					s.PublishedURL = publishedURL
				}), nil
			},
		},
	}

	d, err := deck.New(cues...)
	if err != nil {
		log.Fatal(err)
	}
	return d
}

// waitForStateField polls a saved job's snapshot until a state field is
// populated. Simulates an external API looking up the request ID it was
// asked to fulfil. (Request IDs live in state, not input — they're produced
// by the Submit cues during the run.)
//
// Returns the field value, or "" if ctx is cancelled before it appears.
func waitForStateField(ctx context.Context, rdb *redis.Client, d *deck.Deck[JobInput, ContentState], jobID, field string) string {
	for {
		data, err := rdb.Get(ctx, snapshotKey(jobID)).Bytes()
		if err == nil {
			state, _, importErr := d.Import(data)
			if importErr == nil {
				var val string
				switch field {
				case "image_request_id":
					val = state.ImageRequestID
				case "copy_request_id":
					val = state.CopyRequestID
				}
				if val != "" {
					return val
				}
			}
		}
		select {
		case <-ctx.Done():
			return ""
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func printComplete(input JobInput, state *ContentState, result deck.Result) {
	fmt.Println()
	fmt.Println("==========================================================")
	fmt.Println("  CONTENT PUBLISHED SUCCESSFULLY")
	fmt.Println("==========================================================")
	fmt.Printf("  Job:   %s\n", input.JobID)
	fmt.Printf("  Topic: %s\n", input.Topic)
	fmt.Printf("  Style: %s\n", input.Style)
	fmt.Printf("  Image: %s\n", state.ImageURL)
	fmt.Printf("  URL:   %s\n", state.PublishedURL)
	fmt.Println()
	fmt.Println("  Copy:")
	fmt.Printf("    %s\n", state.CopyText)
	fmt.Println()
	fmt.Printf("  Completed %d cues:\n", len(result.CompletedCues))
	for _, c := range result.CompletedCues {
		fmt.Printf("    - %s (%s)\n", c.Name, c.Duration())
	}
	fmt.Println("==========================================================")
}
