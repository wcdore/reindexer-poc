package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/peter/reindexer-poc/pkg/queue"
	amqp "github.com/rabbitmq/amqp091-go"
)

// MockDelivery is a struct that wraps a message and implements the necessary methods
type MockDelivery struct {
	content string
}

// Ack mocks the acknowledgment of a message
func (m MockDelivery) Ack(multiple bool) error {
	return nil
}

// MockRabbitMQ implements a mock version of the needed RabbitMQ methods for testing
type MockRabbitMQ struct {
	highMessages     []string
	standardMessages []string
	lowMessages      []string
}

// NewMockRabbitMQ creates a new mock RabbitMQ client with predefined messages
func NewMockRabbitMQ(highCount, standardCount, lowCount int) *MockRabbitMQ {
	mock := &MockRabbitMQ{
		highMessages:     make([]string, highCount),
		standardMessages: make([]string, standardCount),
		lowMessages:      make([]string, lowCount),
	}

	// Generate dummy messages
	for i := 0; i < highCount; i++ {
		mock.highMessages[i] = fmt.Sprintf("high-%d", i)
	}
	for i := 0; i < standardCount; i++ {
		mock.standardMessages[i] = fmt.Sprintf("standard-%d", i)
	}
	for i := 0; i < lowCount; i++ {
		mock.lowMessages[i] = fmt.Sprintf("low-%d", i)
	}

	return mock
}

// ConsumeBatch mocks consuming a batch of messages from a queue
func (m *MockRabbitMQ) ConsumeBatch(queueName string, prefetchCount int) (<-chan amqp.Delivery, error) {
	ch := make(chan amqp.Delivery)

	// Select the appropriate queue based on the name
	var messages []string
	switch queueName {
	case queue.QueueHigh:
		messages = m.highMessages
	case queue.QueueStandard:
		messages = m.standardMessages
	case queue.QueueLow:
		messages = m.lowMessages
	default:
		close(ch)
		return ch, fmt.Errorf("unknown queue: %s", queueName)
	}

	// Start a goroutine to send messages to the channel
	go func() {
		defer close(ch)

		// Calculate how many messages to actually send (min of requested count and available)
		sendCount := prefetchCount
		if sendCount > len(messages) {
			sendCount = len(messages)
		}

		// Send the messages
		for i := 0; i < sendCount; i++ {
			ch <- amqp.Delivery{
				Body: []byte(messages[i]),
			}
		}

		// Remove the sent messages from the mock queue
		if sendCount > 0 && sendCount < len(messages) {
			messages = messages[sendCount:]
		} else if sendCount > 0 {
			messages = nil
		}

		// Update the appropriate queue
		switch queueName {
		case queue.QueueHigh:
			m.highMessages = messages
		case queue.QueueStandard:
			m.standardMessages = messages
		case queue.QueueLow:
			m.lowMessages = messages
		}
	}()

	return ch, nil
}

// Close is a no-op for the mock
func (m *MockRabbitMQ) Close() error {
	return nil
}

// TestCreateBatch verifies that the createBatch function correctly prioritizes messages
func TestCreateBatch(t *testing.T) {
	// Common test config with a max batch size of 10000 and 95% high priority percentage
	config := Config{
		MaxBatchSize:        10000,
		HighQueuePercentage: 95,
	}

	// Define test cases
	testCases := []struct {
		name          string
		highCount     int
		standardCount int
		lowCount      int
		expectedHigh  int
		expectedStd   int
		expectedLow   int
		expectedTotal int
		description   string
	}{
		{
			name:          "All queues have plenty of messages",
			highCount:     20000,
			standardCount: 20000,
			lowCount:      20000,
			expectedHigh:  9500,  // 95% of max batch size
			expectedStd:   500,   // 5% of max batch size
			expectedLow:   0,     // No low priority messages should be included
			expectedTotal: 10000, // Full batch
			description:   "When all queues have plenty of messages, batch should be 95% high and 5% standard",
		},
		{
			name:          "High queue has fewer messages than allocation",
			highCount:     5000,
			standardCount: 20000,
			lowCount:      20000,
			expectedHigh:  5000,  // All available high priority messages
			expectedStd:   5000,  // Rest should be from standard
			expectedLow:   0,     // No low priority messages should be included
			expectedTotal: 10000, // Full batch
			description:   "When high queue has fewer messages than 95%, standard should fill the rest",
		},
		{
			name:          "Standard queue has fewer messages than allocation",
			highCount:     20000,
			standardCount: 200,
			lowCount:      20000,
			expectedHigh:  9800,  // 9500 initial + 300 additional
			expectedStd:   200,   // All available standard messages
			expectedLow:   0,     // No low priority messages should be included
			expectedTotal: 10000, // Full batch
			description:   "When standard queue has fewer messages than 5%, more high priority should be used before low",
		},
		{
			name:          "Standard queue is empty",
			highCount:     20000,
			standardCount: 0,
			lowCount:      20000,
			expectedHigh:  10000, // Should fill entire batch with high
			expectedStd:   0,     // No standard messages available
			expectedLow:   0,     // No low priority messages should be included
			expectedTotal: 10000, // Full batch
			description:   "When standard queue is empty, batch should be filled entirely with high priority messages",
		},
		{
			name:          "Both high and standard queues have fewer messages",
			highCount:     4000,
			standardCount: 3000,
			lowCount:      20000,
			expectedHigh:  4000,  // All available high priority messages
			expectedStd:   3000,  // All available standard messages
			expectedLow:   3000,  // Rest should be from low
			expectedTotal: 10000, // Full batch
			description:   "When both high and standard queues have fewer messages than needed, low should fill the rest",
		},
		{
			name:          "Edge case: 10k high messages, 100 standard",
			highCount:     10000,
			standardCount: 100,
			lowCount:      10000,
			expectedHigh:  9900,  // 9500 initial + 400 additional
			expectedStd:   100,   // All available standard messages
			expectedLow:   0,     // No low priority messages should be included
			expectedTotal: 10000, // Full batch
			description:   "Edge case: with 10k high messages but only 100 standard, should use 9900 high and 100 standard",
		},
		{
			name:          "Batch size less than max when not enough messages",
			highCount:     3000,
			standardCount: 2000,
			lowCount:      1000,
			expectedHigh:  3000, // All available high priority messages
			expectedStd:   2000, // All available standard messages
			expectedLow:   1000, // All available low priority messages
			expectedTotal: 6000, // Less than max batch size
			description:   "When not enough messages across all queues, should use all available messages",
		},
		{
			name:          "Empty high queue",
			highCount:     0,
			standardCount: 8000,
			lowCount:      8000,
			expectedHigh:  0,     // No high priority messages available
			expectedStd:   8000,  // All available standard messages
			expectedLow:   2000,  // Rest should be from low
			expectedTotal: 10000, // Full batch
			description:   "When high queue is empty, should use standard first, then low",
		},
		{
			name:          "Standard has exact allocation amount",
			highCount:     9500, // Exactly 95%
			standardCount: 500,  // Exactly 5%
			lowCount:      5000,
			expectedHigh:  9500,  // All allocated high priority messages
			expectedStd:   500,   // All allocated standard messages
			expectedLow:   0,     // No low priority messages should be included
			expectedTotal: 10000, // Full batch
			description:   "When high and standard have exactly their allocation amount, should use all of each",
		},
		{
			name:          "All queues empty",
			highCount:     0,
			standardCount: 0,
			lowCount:      0,
			expectedHigh:  0,
			expectedStd:   0,
			expectedLow:   0,
			expectedTotal: 0,
			description:   "When all queues are empty, batch should be empty",
		},
	}

	// Run all test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create mock RabbitMQ
			mockRMQ := NewMockRabbitMQ(tc.highCount, tc.standardCount, tc.lowCount)

			// Create a test context
			ctx := context.Background()

			// Run the actual createBatch function (not a test duplicate)
			batch, stats := createBatch(ctx, mockRMQ, config)

			// Verify the results
			if len(batch) != tc.expectedTotal {
				t.Errorf("Expected batch size %d, got %d", tc.expectedTotal, len(batch))
			}

			if stats.HighCount != tc.expectedHigh {
				t.Errorf("Expected %d high priority messages, got %d", tc.expectedHigh, stats.HighCount)
			}

			if stats.StdCount != tc.expectedStd {
				t.Errorf("Expected %d standard priority messages, got %d", tc.expectedStd, stats.StdCount)
			}

			if stats.LowCount != tc.expectedLow {
				t.Errorf("Expected %d low priority messages, got %d", tc.expectedLow, stats.LowCount)
			}

			if stats.TotalSize != tc.expectedTotal {
				t.Errorf("Expected total size %d, got %d", tc.expectedTotal, stats.TotalSize)
			}

			// Check that the total matches the sum of the parts
			sumOfParts := stats.HighCount + stats.StdCount + stats.LowCount
			if sumOfParts != stats.TotalSize {
				t.Errorf("Sum of parts (%d) doesn't match total (%d)", sumOfParts, stats.TotalSize)
			}
		})
	}
}

// TestVariousConfigurations tests createBatch with different configurations
func TestVariousConfigurations(t *testing.T) {
	// Define different configurations to test
	configurations := []struct {
		maxBatchSize        int
		highQueuePercentage int
	}{
		{10000, 95}, // Our standard config
		{10000, 80}, // Lower high priority percentage
		{10000, 50}, // Equal distribution
		{5000, 95},  // Smaller batch size
		{1000, 90},  // Much smaller batch size
	}

	// For each configuration, test a couple of key scenarios
	for _, cfg := range configurations {
		config := Config{
			MaxBatchSize:        cfg.maxBatchSize,
			HighQueuePercentage: cfg.highQueuePercentage,
		}

		highTarget := int(float64(cfg.maxBatchSize) * float64(cfg.highQueuePercentage) / 100.0)
		stdTarget := cfg.maxBatchSize - highTarget

		// Test case 1: All queues have plenty of messages
		t.Run(fmt.Sprintf("Config_%d_%d_AllQueuesHavePlenty", cfg.maxBatchSize, cfg.highQueuePercentage), func(t *testing.T) {
			mockRMQ := NewMockRabbitMQ(cfg.maxBatchSize*2, cfg.maxBatchSize*2, cfg.maxBatchSize*2)
			_, stats := createBatch(context.Background(), mockRMQ, config)

			if stats.HighCount != highTarget {
				t.Errorf("Expected %d high priority messages, got %d", highTarget, stats.HighCount)
			}

			if stats.StdCount != stdTarget {
				t.Errorf("Expected %d standard priority messages, got %d", stdTarget, stats.StdCount)
			}

			if stats.TotalSize != cfg.maxBatchSize {
				t.Errorf("Expected total size %d, got %d", cfg.maxBatchSize, stats.TotalSize)
			}
		})

		// Test case 2: Standard queue has only half its allocation
		t.Run(fmt.Sprintf("Config_%d_%d_StandardHalfFull", cfg.maxBatchSize, cfg.highQueuePercentage), func(t *testing.T) {
			halfStd := stdTarget / 2
			mockRMQ := NewMockRabbitMQ(cfg.maxBatchSize*2, halfStd, cfg.maxBatchSize*2)
			_, stats := createBatch(context.Background(), mockRMQ, config)

			expectedHigh := highTarget + (stdTarget - halfStd)

			if stats.HighCount != expectedHigh {
				t.Errorf("Expected %d high priority messages, got %d", expectedHigh, stats.HighCount)
			}

			if stats.StdCount != halfStd {
				t.Errorf("Expected %d standard priority messages, got %d", halfStd, stats.StdCount)
			}

			if stats.TotalSize != cfg.maxBatchSize {
				t.Errorf("Expected total size %d, got %d", cfg.maxBatchSize, stats.TotalSize)
			}
		})
	}
}
