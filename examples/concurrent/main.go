package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/lordtatty/deck"
)

// Three independent data fetches run concurrently.
// A fourth cue waits for all three to complete before generating a report.

type DashboardState struct {
	Users    int
	Orders   int
	Revenue  float64
	Report   string
}

func main() {
	cues := []deck.Cue[DashboardState]{
		{
			Name: "FetchUsers",
			When: func(s DashboardState, r deck.Result) bool {
				return s.Users == 0
			},
			Run: func(s DashboardState) (deck.Mutation[DashboardState], error) {
				fmt.Println("[FetchUsers] Querying user count...")
				time.Sleep(time.Duration(50+rand.Intn(100)) * time.Millisecond)
				count := 1542
				return deck.Complete(func(s *DashboardState) {
					s.Users = count
				}), nil
			},
		},
		{
			Name: "FetchOrders",
			When: func(s DashboardState, r deck.Result) bool {
				return s.Orders == 0
			},
			Run: func(s DashboardState) (deck.Mutation[DashboardState], error) {
				fmt.Println("[FetchOrders] Querying order count...")
				time.Sleep(time.Duration(50+rand.Intn(100)) * time.Millisecond)
				count := 328
				return deck.Complete(func(s *DashboardState) {
					s.Orders = count
				}), nil
			},
		},
		{
			Name: "FetchRevenue",
			When: func(s DashboardState, r deck.Result) bool {
				return s.Revenue == 0
			},
			Run: func(s DashboardState) (deck.Mutation[DashboardState], error) {
				fmt.Println("[FetchRevenue] Querying revenue...")
				time.Sleep(time.Duration(50+rand.Intn(100)) * time.Millisecond)
				rev := 48293.50
				return deck.Complete(func(s *DashboardState) {
					s.Revenue = rev
				}), nil
			},
		},
		{
			Name: "GenerateReport",
			When: func(s DashboardState, r deck.Result) bool {
				// Only run after all three fetches complete
				return r.Completed("FetchUsers") &&
					r.Completed("FetchOrders") &&
					r.Completed("FetchRevenue")
			},
			Run: func(s DashboardState) (deck.Mutation[DashboardState], error) {
				fmt.Println("[GenerateReport] Building report...")
				report := fmt.Sprintf("%d users, %d orders, $%.2f revenue", s.Users, s.Orders, s.Revenue)
				return deck.Complete(func(s *DashboardState) {
					s.Report = report
				}), nil
			},
		},
	}

	d, err := deck.New(cues...)
	if err != nil {
		log.Fatal(err)
	}

	state := &DashboardState{}
	result, err := d.Run(context.Background(), state)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println()
	fmt.Println("=== Dashboard ===")
	fmt.Printf("Report: %s\n", state.Report)
	fmt.Printf("Completed: %d cues\n", len(result.CompletedCues))
	for _, c := range result.CompletedCues {
		fmt.Printf("  - %s (%s)\n", c.Name, c.Duration())
	}
}
