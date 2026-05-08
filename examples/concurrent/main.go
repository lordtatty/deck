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
//
// DashboardInput holds the immutable parameters for this run — the report's
// title. DashboardState holds the mutable counts and the generated report.

type DashboardInput struct {
	ReportTitle string
}

type DashboardState struct {
	Users   int
	Orders  int
	Revenue float64
	Report  string
}

func main() {
	cues := []deck.Cue[DashboardInput, DashboardState]{
		{
			Name: "FetchUsers",
			When: func(i DashboardInput, s DashboardState, r deck.Result) bool {
				return s.Users == 0
			},
			Run: func(i DashboardInput, s DashboardState) (deck.Mutation[DashboardState], error) {
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
			When: func(i DashboardInput, s DashboardState, r deck.Result) bool {
				return s.Orders == 0
			},
			Run: func(i DashboardInput, s DashboardState) (deck.Mutation[DashboardState], error) {
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
			When: func(i DashboardInput, s DashboardState, r deck.Result) bool {
				return s.Revenue == 0
			},
			Run: func(i DashboardInput, s DashboardState) (deck.Mutation[DashboardState], error) {
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
			When: func(i DashboardInput, s DashboardState, r deck.Result) bool {
				// Only run after all three fetches complete
				return r.Completed("FetchUsers") &&
					r.Completed("FetchOrders") &&
					r.Completed("FetchRevenue")
			},
			Run: func(i DashboardInput, s DashboardState) (deck.Mutation[DashboardState], error) {
				fmt.Println("[GenerateReport] Building report...")
				report := fmt.Sprintf("%s — %d users, %d orders, $%.2f revenue",
					i.ReportTitle, s.Users, s.Orders, s.Revenue)
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

	input := DashboardInput{ReportTitle: "Q1 Daily Snapshot"}
	state := &DashboardState{}
	result, err := d.Run(context.Background(), input, state)
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
