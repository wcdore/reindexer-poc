package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/peter/reindexer-poc/pkg/queue"
)

type Config struct {
	RabbitMQURI         string
	MaxBatchSize        int
	HighQueuePercentage int
	ProcessingDelayMS   int
}

type BatchStats struct {
	TotalSize   int
	HighCount   int
	StdCount    int
	LowCount    int
	ProcessedAt time.Time
}

func main() {
	// Load configuration from environment variables
	config := loadConfig()

	log.Printf("Starting consumer with max batch size: %d, high queue percentage: %d%%",
		config.MaxBatchSize, config.HighQueuePercentage)

	// Connect to RabbitMQ with retry
	var rmq *queue.RabbitMQ
	var err error

	maxRetries := 5
	retryDelay := 5 * time.Second

	for i := 0; i < maxRetries; i++ {
		log.Printf("Attempting to connect to RabbitMQ (attempt %d/%d)", i+1, maxRetries)
		rmq, err = queue.NewRabbitMQ(config.RabbitMQURI)
		if err == nil {
			log.Println("Successfully connected to RabbitMQ")
			break
		}
		log.Printf("Failed to connect to RabbitMQ: %v", err)

		if i < maxRetries-1 {
			log.Printf("Retrying in %v...", retryDelay)
			time.Sleep(retryDelay)
		}
	}

	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ after %d attempts: %v", maxRetries, err)
	}

	defer rmq.Close()

	// Monitor RabbitMQ connection
	rmq.MonitorConnection()

	// Create a context that will be canceled on interrupt signal
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		log.Println("Received shutdown signal, stopping...")
		cancel()
	}()

	// Create a channel to receive batch stats
	statsChan := make(chan BatchStats, 100)
	var statsWg sync.WaitGroup

	// Start the batch processor
	go processBatches(ctx, statsChan, config.ProcessingDelayMS, &statsWg)

	// Start the consumer loop
	consumeLoop(ctx, rmq, config, statsChan, &statsWg)

	// Wait for all stats processing to complete
	statsWg.Wait()
	log.Println("Consumer shutdown complete")
}

func loadConfig() Config {
	var config Config

	// Default values
	config.RabbitMQURI = "amqp://guest:guest@localhost:5672/"
	config.MaxBatchSize = 10000
	config.HighQueuePercentage = 95
	config.ProcessingDelayMS = 300

	// Override from environment if available
	if uri := os.Getenv("RABBITMQ_URI"); uri != "" {
		config.RabbitMQURI = uri
	}

	if maxSizeStr := os.Getenv("MAX_BATCH_SIZE"); maxSizeStr != "" {
		if val, err := strconv.Atoi(maxSizeStr); err == nil && val > 0 {
			config.MaxBatchSize = val
		}
	}

	if pctStr := os.Getenv("HIGH_QUEUE_PERCENTAGE"); pctStr != "" {
		if val, err := strconv.Atoi(pctStr); err == nil && val >= 0 && val <= 100 {
			config.HighQueuePercentage = val
		}
	}

	if delayStr := os.Getenv("PROCESSING_DELAY_MS"); delayStr != "" {
		if val, err := strconv.Atoi(delayStr); err == nil && val >= 0 {
			config.ProcessingDelayMS = val
		}
	}

	return config
}

func consumeLoop(ctx context.Context, rmq *queue.RabbitMQ, config Config, statsChan chan<- BatchStats, wg *sync.WaitGroup) {
	for {
		select {
		case <-ctx.Done():
			log.Println("Consumer loop stopped due to context cancellation")
			close(statsChan)
			return
		default:
			// Create a batch according to priority rules
			batch, stats := createBatch(ctx, rmq, config)

			if len(batch) > 0 {
				wg.Add(1)
				statsChan <- stats
				log.Printf("Created batch of %d messages (%d high, %d standard, %d low)",
					stats.TotalSize, stats.HighCount, stats.StdCount, stats.LowCount)
			} else {
				// If no messages were found, wait a bit before trying again
				log.Println("No messages available, waiting before next attempt...")
				select {
				case <-ctx.Done():
					close(statsChan)
					return
				case <-time.After(1 * time.Second):
					// Continue with the loop
				}
			}
		}
	}
}

func createBatch(ctx context.Context, rmq *queue.RabbitMQ, config Config) ([]string, BatchStats) {
	batch := make([]string, 0, config.MaxBatchSize)
	stats := BatchStats{
		ProcessedAt: time.Now(),
	}

	// Calculate how many messages to take from high and standard queues
	highTargetCount := int(float64(config.MaxBatchSize) * float64(config.HighQueuePercentage) / 100.0)

	// First, get messages from high priority queue
	highMessages, err := getMessagesFromQueue(ctx, rmq, queue.QueueHigh, highTargetCount)
	if err != nil {
		log.Printf("Error getting messages from high priority queue: %v", err)
	}
	batch = append(batch, highMessages...)
	stats.HighCount = len(highMessages)
	stats.TotalSize += stats.HighCount

	// Calculate available slots after high priority queue
	remainingSlots := config.MaxBatchSize - stats.TotalSize

	// Get messages from standard priority queue - use all remaining slots
	// This ensures standard gets priority over low queue
	if remainingSlots > 0 {
		standardMessages, err := getMessagesFromQueue(ctx, rmq, queue.QueueStandard, remainingSlots)
		if err != nil {
			log.Printf("Error getting messages from standard priority queue: %v", err)
		}
		batch = append(batch, standardMessages...)
		stats.StdCount = len(standardMessages)
		stats.TotalSize += stats.StdCount

		// Recalculate remaining slots after standard queue
		remainingSlots = config.MaxBatchSize - stats.TotalSize
	}

	// If we still have space, fill remaining slots with low priority messages
	if remainingSlots > 0 {
		lowMessages, err := getMessagesFromQueue(ctx, rmq, queue.QueueLow, remainingSlots)
		if err != nil {
			log.Printf("Error getting messages from low priority queue: %v", err)
		}
		batch = append(batch, lowMessages...)
		stats.LowCount = len(lowMessages)
		stats.TotalSize += stats.LowCount
	}

	return batch, stats
}

func getMessagesFromQueue(ctx context.Context, rmq *queue.RabbitMQ, queueName string, count int) ([]string, error) {
	if count <= 0 {
		return []string{}, nil
	}

	messages := make([]string, 0, count)
	deliveries, err := rmq.ConsumeBatch(queueName, count)
	if err != nil {
		return nil, fmt.Errorf("failed to consume from queue %s: %w", queueName, err)
	}

	// Use a longer timeout to allow collecting more messages
	timeout := time.After(2 * time.Second)
	for len(messages) < count {
		select {
		case <-ctx.Done():
			return messages, nil
		case <-timeout:
			// Only log when we actually received some messages
			if len(messages) > 0 {
				log.Printf("Retrieved %d messages from %s queue", len(messages), queueName)
			}
			return messages, nil
		case delivery, ok := <-deliveries:
			if !ok {
				return messages, nil
			}
			messages = append(messages, string(delivery.Body))
			delivery.Ack(false) // Acknowledge the message
		}
	}

	return messages, nil
}

func processBatches(ctx context.Context, statsChan <-chan BatchStats, delayMS int, wg *sync.WaitGroup) {
	for {
		select {
		case <-ctx.Done():
			log.Println("Batch processor stopped due to context cancellation")
			return
		case stats, ok := <-statsChan:
			if !ok {
				log.Println("Stats channel closed, stopping batch processor")
				return
			}

			// Process the batch in a separate goroutine
			go func(stats BatchStats) {
				defer wg.Done()

				// Simulate processing time
				time.Sleep(time.Duration(delayMS) * time.Millisecond)

				log.Printf("Processed batch of %d messages at %v (High: %d, Standard: %d, Low: %d)",
					stats.TotalSize, stats.ProcessedAt, stats.HighCount, stats.StdCount, stats.LowCount)
			}(stats)
		}
	}
}

// Helper function to find the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
