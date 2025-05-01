# RabbitMQ Priority Queue Batch Processor

A proof-of-concept application demonstrating batch processing with priority queues using RabbitMQ. This system processes messages in batches with configurable priority rules.

## System Overview

This application consists of three main components:

1. **RabbitMQ** - Message broker that stores messages in three priority queues
2. **Producer** - Service that publishes messages to the priority queues
3. **Consumer** - Service that consumes messages from the queues in batches, following priority rules

### Priority Queue Structure

The system uses three queues with different priorities:

- **High Priority Queue**: Messages that should be processed first
- **Standard Priority Queue**: Messages that should be processed after high priority
- **Low Priority Queue**: Messages that should only be processed when no high or standard messages are available

### Batch Processing Logic

The consumer creates batches of messages according to these rules:

1. By default, 95% of a batch will be filled with high priority messages
2. The remaining 5% will be filled with standard priority messages
3. If there aren't enough high priority messages, standard priority messages will fill the gap
4. If there aren't enough standard priority messages, additional high priority messages will be used
5. Only if both high and standard queues are exhausted will low priority messages be added to the batch

## Setup

### Prerequisites

- Docker
- Docker Compose
- Git

### Installation

1. Clone the repository:
   ```
   git clone <repository-url>
   cd reindexer-poc
   ```

2. Build the images:
   ```
   docker-compose build
   ```

## Running the Application

### Start the Services

Start RabbitMQ and the consumer:

```
docker-compose up
```

This command starts:
- RabbitMQ service on port 5672 (AMQP) and 15672 (management UI)
- Consumer service that processes messages in batches

### Publish Messages

Publish messages using the producer service:

```
docker-compose run producer ./producer -high=19000 -standard=19000 -low=5000
```

This command publishes:
- 19,000 messages to the high priority queue
- 19,000 messages to the standard priority queue
- 5,000 messages to the low priority queue

You can adjust these numbers to test different scenarios.

### View the RabbitMQ Management Console

Access the RabbitMQ management console at [http://localhost:15672](http://localhost:15672) with:
- Username: guest
- Password: guest

### Monitor Processing

The consumer logs show how messages are being processed in batches, including:
- Batch size
- Number of messages from each priority queue
- Processing time

## Configuration

Both the consumer and producer can be configured via environment variables in the `docker-compose.yml` file:

### Consumer Configuration

- `RABBITMQ_URI`: Connection string for RabbitMQ
- `MAX_BATCH_SIZE`: Maximum number of messages in a batch (default: 10000)
- `HIGH_QUEUE_PERCENTAGE`: Percentage of batch allocated to high priority messages (default: 95)
- `PROCESSING_DELAY_MS`: Simulated processing time per batch in milliseconds (default: 300)

### Producer Configuration

- `RABBITMQ_URI`: Connection string for RabbitMQ

## Development

### Running Tests

To run the consumer batch creation tests:

```
go test ./cmd/consumer -v
```

These tests verify the batch creation logic works correctly in all edge cases.

## Shutting Down

To shut down all services:

```
docker-compose down
```

Use `-v` to remove volumes (this will delete all queued messages):

```
docker-compose down -v
```
