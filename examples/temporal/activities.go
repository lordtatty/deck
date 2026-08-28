package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
)

// The three units of work this order flow needs. They are ordinary Go
// functions; Temporal turns them into activities, retries them when they fail,
// and records every attempt in the workflow's history.

// ReserveStock is quick and reliable. Nothing interesting happens here.
func ReserveStock(ctx context.Context, orderID string) (string, error) {
	report(ctx, "reserve", "checking warehouse")
	time.Sleep(150 * time.Millisecond)
	return "reserved:" + orderID, nil
}

// ChargePayment fails its first two attempts, standing in for a payment
// gateway having a bad afternoon. Nothing in the cue handles that: Temporal
// retries it according to the RetryPolicy, and the cue only ever sees the
// attempt that finally worked.
func ChargePayment(ctx context.Context, orderID string) (string, error) {
	attempt := activity.GetInfo(ctx).Attempt
	if attempt < 3 {
		report(ctx, "charge", "gateway returned 503 — Temporal will retry")
		return "", errors.New("payment gateway unavailable")
	}
	report(ctx, "charge", "succeeded")
	return "charged:" + orderID, nil
}

// ShipOrder is slow — slower than the timeout every other cue is happy with.
// That is what ForCue is for; see main.go.
func ShipOrder(ctx context.Context, orderID string) (string, error) {
	report(ctx, "ship", "booking courier (slow)")
	time.Sleep(1500 * time.Millisecond)
	return "shipped:" + orderID, nil
}

// report prints which attempt of which activity is running. activity.GetInfo
// works inside any activity and is the easiest way to see Temporal's retries
// happening.
func report(ctx context.Context, name, msg string) {
	fmt.Printf("    %-9s attempt %d   %s\n", name, activity.GetInfo(ctx).Attempt, msg)
}
