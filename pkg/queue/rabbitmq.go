package queue

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	QueueHigh     = "high_priority"
	QueueStandard = "standard_priority"
	QueueLow      = "low_priority"
)

// RabbitMQ represents a connection to RabbitMQ server
type RabbitMQ struct {
	uri          string
	conn         *amqp.Connection
	channel      *amqp.Channel // Main channel for publishing
	isConnected  bool
	errorChannel chan *amqp.Error
}

// NewRabbitMQ creates a new RabbitMQ connection
func NewRabbitMQ(uri string) (*RabbitMQ, error) {
	rmq := &RabbitMQ{
		uri:          uri,
		isConnected:  false,
		errorChannel: make(chan *amqp.Error),
	}

	err := rmq.connect()
	if err != nil {
		return nil, err
	}

	return rmq, nil
}

// connect establishes a connection to RabbitMQ
func (r *RabbitMQ) connect() error {
	var err error

	r.conn, err = amqp.Dial(r.uri)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	r.channel, err = r.conn.Channel()
	if err != nil {
		r.conn.Close()
		return fmt.Errorf("failed to open a channel: %w", err)
	}

	r.errorChannel = make(chan *amqp.Error)
	r.conn.NotifyClose(r.errorChannel)
	r.isConnected = true

	// Ensure the queues exist
	err = r.declareQueues()
	if err != nil {
		r.Close()
		return fmt.Errorf("failed to declare queues: %w", err)
	}

	return nil
}

// declareQueues declares the required queues
func (r *RabbitMQ) declareQueues() error {
	// Declare the high priority queue
	_, err := r.channel.QueueDeclare(
		QueueHigh, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return err
	}

	// Declare the standard priority queue
	_, err = r.channel.QueueDeclare(
		QueueStandard, // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		return err
	}

	// Declare the low priority queue
	_, err = r.channel.QueueDeclare(
		QueueLow, // name
		true,     // durable
		false,    // delete when unused
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		return err
	}

	return nil
}

// Close closes the connection to RabbitMQ
func (r *RabbitMQ) Close() error {
	if r.channel != nil {
		r.channel.Close()
	}

	if r.conn != nil && r.conn.IsClosed() == false {
		return r.conn.Close()
	}

	return nil
}

// PublishBatch publishes a batch of messages to a specific queue
func (r *RabbitMQ) PublishBatch(ctx context.Context, queue string, orderIDs []string) error {
	for _, orderID := range orderIDs {
		err := r.Publish(ctx, queue, orderID)
		if err != nil {
			return err
		}
	}
	return nil
}

// Publish publishes a message to a specific queue
func (r *RabbitMQ) Publish(ctx context.Context, queue string, orderID string) error {
	if !r.isConnected {
		return fmt.Errorf("not connected to RabbitMQ")
	}

	return r.channel.PublishWithContext(
		ctx,
		"",    // exchange
		queue, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(orderID),
		})
}

// ConsumeBatch consumes a batch of messages from a queue with a specified prefetch count
func (r *RabbitMQ) ConsumeBatch(queue string, prefetchCount int) (<-chan amqp.Delivery, error) {
	if !r.isConnected {
		return nil, fmt.Errorf("not connected to RabbitMQ")
	}

	// Create a new channel specifically for this consumer
	ch, err := r.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to create channel for consuming: %w", err)
	}

	// Set QoS on this specific channel
	// Ensure we set a large enough prefetch count to get all the messages we need
	adjustedPrefetchCount := int(float64(prefetchCount) * 1.2)
	if adjustedPrefetchCount < 1000 {
		adjustedPrefetchCount = 1000 // Minimum prefetch of 1000 messages
	}

	err = ch.Qos(
		adjustedPrefetchCount, // prefetch count
		0,                     // prefetch size
		false,                 // global
	)
	if err != nil {
		ch.Close()
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	// We use a unique consumer tag for each consume operation
	consumerTag := fmt.Sprintf("c-%s-%d", queue, time.Now().UnixNano())

	deliveries, err := ch.Consume(
		queue,       // queue name
		consumerTag, // consumer tag
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		ch.Close()
		return nil, fmt.Errorf("failed to consume from queue %s: %w", queue, err)
	}

	// Start a goroutine to close the channel when consumption is done
	go func(ch *amqp.Channel) {
		// Wait a bit after consumption is done before closing the channel
		time.Sleep(3 * time.Second)
		ch.Close()
	}(ch)

	return deliveries, nil
}

// Reconnect attempts to reconnect to RabbitMQ
func (r *RabbitMQ) Reconnect() error {
	if r.isConnected {
		return nil
	}

	log.Println("Attempting to reconnect to RabbitMQ")
	return r.connect()
}

// MonitorConnection monitors the connection and attempts to reconnect if needed
func (r *RabbitMQ) MonitorConnection() {
	go func() {
		for {
			err, ok := <-r.errorChannel
			if !ok {
				return
			}

			log.Printf("RabbitMQ connection error: %v", err)
			r.isConnected = false

			// Attempt to reconnect with exponential backoff
			backoff := 1 * time.Second
			maxBackoff := 30 * time.Second

			for !r.isConnected {
				log.Printf("Attempting to reconnect in %v...", backoff)
				time.Sleep(backoff)

				err := r.Reconnect()
				if err == nil {
					log.Println("Reconnected to RabbitMQ")
					break
				}

				log.Printf("Failed to reconnect: %v", err)
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
		}
	}()
}
