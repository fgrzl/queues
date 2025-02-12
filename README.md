[![Dependabot Updates](https://github.com/fgrzl/queues/actions/workflows/dependabot/dependabot-updates/badge.svg)](https://github.com/fgrzl/queues/actions/workflows/dependabot-updates)
[![ci](https://github.com/fgrzl/queues/actions/workflows/ci.yml/badge.svg)](https://github.com/fgrzl/queues/actions/workflows/ci.yml)

# Queues

A Go library providing a unified interface for various queue implementations, supporting multiple backends.

## Features

- **Pluggable Backends**: Easily switch between different queue providers.
- **Supported Providers**:
  - [Azure Storage Queues](https://azure.microsoft.com/en-us/services/storage/queues/)
  - [AWS SQS](https://aws.amazon.com/sqs/)
  - [Pebble](https://github.com/cockroachdb/pebble) (embedded key-value store)
- **Batch Processing**: Support for sending and receiving multiple messages at once.
- **Visibility Timeouts**: Ensures messages are processed at least once while preventing duplicates.

## Installation

To install the package, run:

```bash
go get github.com/fgrzl/queues
```

## Usage

### 1️⃣ Initializing a Queue Provider

```go
package main

import (
    "log"
    "github.com/fgrzl/queues/pebble"
)

func main() {
    // Initialize the Pebble queue provider
    provider, err := pebble.NewQueueProvider(pebble.QueueProviderOptions{Path: "path/to/db"})
    if err != nil {
        log.Fatalf("Failed to initialize Pebble provider: %v", err)
    }
    defer provider.Close()

    // Your code to send/receive messages
}
```

### 2️⃣ Sending Messages

#### 📌 Sending a Single Message

```go
package main

import (
    "context"
    "log"
    "github.com/fgrzl/queues"
)

func sendSingle(provider queues.QueueProvider) {
    ctx := context.Background()
    queueName := "example-queue"

    // Create a new queue item
    item := queues.NewQueueItem(queueName, "Hello, Queue!")

    // Send the item to the queue
    if err := provider.Send(ctx, item); err != nil {
        log.Fatalf("Failed to send item to queue: %v", err)
    }

    log.Println("Message sent successfully!")
}
```

#### 📌 Sending a Batch of Messages

```go
func sendBatch(provider queues.QueueProvider) {
    ctx := context.Background()
    queueName := "example-queue"

    // Create a batch of queue items
    batch := []*queues.QueueItem{
        queues.NewQueueItem(queueName, newDummyContent()),
        queues.NewQueueItem(queueName, newDummyContent()),
        queues.NewQueueItem(queueName, newDummyContent()),
    }

    // Send the batch to the queue
    if err := provider.SendBatch(ctx, batch); err != nil {
        log.Fatalf("Failed to send batch messages: %v", err)
    }

    log.Println("Batch messages sent successfully!")
}
```

### 3️⃣ Receiving Messages

```go
func receiveMessages(provider queues.QueueProvider) {
    ctx := context.Background()
    queueName := "example-queue"

    // Set up receive arguments
    args := queues.NewReceiveArgs(queueName)

    // Receive messages from the queue
    messages, err := provider.Receive(ctx, &args)
    if err != nil {
        log.Fatalf("Failed to receive items from queue: %v", err)
    }

    for _, msg := range messages {
        log.Printf("Received message: %v", msg.Content)
    }
}
```

### 4️⃣ Deleting Messages After Processing

```go
func deleteMessage(provider queues.QueueProvider, message *queues.QueueItem) {
    ctx := context.Background()

    // Remove the message from the queue
    success, err := provider.Remove(ctx, message)
    if err != nil {
        log.Fatalf("Failed to delete message: %v", err)
    }

    if success {
        log.Println("Message successfully deleted from the queue.")
    } else {
        log.Println("Message could not be deleted from the queue.")
    }
}
```

## Running Tests

To run the tests, first start the Docker Compose services:

```bash
docker compose -f test/compose.yml up -d
```

Then, use the following command to run the tests:

```bash
go test ./...
```

The tests are located in the `queues_test.go` file and cover various scenarios for different queue providers.


