package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/peter/reindexer-poc/pkg/queue"
)

func main() {
	// Define command-line flags for message counts
	highCount := flag.Int("high", 0, "Number of messages to publish to high priority queue")
	standardCount := flag.Int("standard", 0, "Number of messages to publish to standard priority queue")
	lowCount := flag.Int("low", 0, "Number of messages to publish to low priority queue")

	flag.Parse()

	// Load configuration
	rabbitMQURI := os.Getenv("RABBITMQ_URI")
	if rabbitMQURI == "" {
		rabbitMQURI = "amqp://guest:guest@localhost:5672/"
	}

	if *highCount == 0 && *standardCount == 0 && *lowCount == 0 {
		fmt.Println("Usage: producer -high=<count> -standard=<count> -low=<count>")
		fmt.Println("At least one count must be greater than 0.")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Connect to RabbitMQ with retry
	var rmq *queue.RabbitMQ
	var err error

	maxRetries := 5
	retryDelay := 5 * time.Second

	for i := 0; i < maxRetries; i++ {
		fmt.Printf("Attempting to connect to RabbitMQ (attempt %d/%d)\n", i+1, maxRetries)
		rmq, err = queue.NewRabbitMQ(rabbitMQURI)
		if err == nil {
			fmt.Println("Successfully connected to RabbitMQ")
			break
		}
		fmt.Printf("Failed to connect to RabbitMQ: %v\n", err)

		if i < maxRetries-1 {
			fmt.Printf("Retrying in %v...\n", retryDelay)
			time.Sleep(retryDelay)
		}
	}

	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ after %d attempts: %v", maxRetries, err)
	}

	defer rmq.Close()

	// Create context for publishing
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Generate and publish messages
	totalCount := *highCount + *standardCount + *lowCount
	fmt.Printf("Preparing to publish %d total messages\n", totalCount)

	// Publish to high priority queue
	if *highCount > 0 {
		fmt.Printf("Publishing %d messages to high priority queue...\n", *highCount)
		err := publishMessages(ctx, rmq, queue.QueueHigh, *highCount)
		if err != nil {
			log.Fatalf("Error publishing to high priority queue: %v", err)
		}
		fmt.Printf("Successfully published %d messages to high priority queue\n", *highCount)
	}

	// Publish to standard priority queue
	if *standardCount > 0 {
		fmt.Printf("Publishing %d messages to standard priority queue...\n", *standardCount)
		err := publishMessages(ctx, rmq, queue.QueueStandard, *standardCount)
		if err != nil {
			log.Fatalf("Error publishing to standard priority queue: %v", err)
		}
		fmt.Printf("Successfully published %d messages to standard priority queue\n", *standardCount)
	}

	// Publish to low priority queue
	if *lowCount > 0 {
		fmt.Printf("Publishing %d messages to low priority queue...\n", *lowCount)
		err := publishMessages(ctx, rmq, queue.QueueLow, *lowCount)
		if err != nil {
			log.Fatalf("Error publishing to low priority queue: %v", err)
		}
		fmt.Printf("Successfully published %d messages to low priority queue\n", *lowCount)
	}

	fmt.Printf("Total %d messages published successfully\n", totalCount)
}

func publishMessages(ctx context.Context, rmq *queue.RabbitMQ, queueName string, count int) error {
	// Create a timestamp to use in orderIDs
	timestamp := time.Now().UnixNano()

	// Publish in batches of 1000 for better performance
	const batchSize = 20000
	remaining := count
	processed := 0

	for remaining > 0 {
		currentBatchSize := min(remaining, batchSize)
		batch := make([]string, currentBatchSize)

		for i := 0; i < currentBatchSize; i++ {
			// Generate a unique orderID with queue name, timestamp and counter
			orderID := fmt.Sprintf("order-%s-%d-%d", queueName, timestamp, processed+i)
			batch[i] = orderID
		}

		err := rmq.PublishBatch(ctx, queueName, batch)
		if err != nil {
			return fmt.Errorf("failed to publish batch: %w", err)
		}

		processed += currentBatchSize
		remaining -= currentBatchSize

		// Progress update for large batches
		if count > batchSize && processed%batchSize == 0 {
			fmt.Printf("Progress: %d/%d messages published to %s\n", processed, count, queueName)
		}
	}

	return nil
}

// Helper function to find the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
