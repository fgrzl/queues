package pebble

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/fgrzl/queues"
	"github.com/google/uuid"
)

// NewQueueProvider initializes a new PebbleQueueProvider.
func NewQueueProvider(options *QueueProviderOptions) (queues.QueueProvider, error) {
	db, err := pebble.Open(options.Path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &PebbleQueueProvider{
		options: options,
		db:      db,
	}, nil
}

// QueueProviderOptions holds configuration options for the Pebble queue.
type QueueProviderOptions struct {
	Prefix string
	Path   string
}

// PebbleQueueProvider implements the QueueProvider interface.
type PebbleQueueProvider struct {
	options *QueueProviderOptions
	db      *pebble.DB
	mu      sync.Mutex
}

// generateKey creates a lexicographically sortable key.
func (p *PebbleQueueProvider) generateKey(queue string, timestamp int64) []byte {
	return []byte(fmt.Sprintf("%s%s-%020d", p.options.Prefix, queue, timestamp))
}

// Send enqueues a single message into Pebble.
func (p *PebbleQueueProvider) Send(ctx context.Context, item *queues.QueueItem) error {
	timestamp := time.Now().UnixNano()
	if item.VisibleOn != nil {
		timestamp = item.VisibleOn.UnixNano()
	}

	key := p.generateKey(item.Queue, timestamp)
	item.MessageID = string(key) // Store key as MessageID
	value, err := json.Marshal(item)
	if err != nil {
		return err
	}

	return p.db.Set(key, value, pebble.Sync)
}

// SendBatch enqueues multiple messages into Pebble in a batch.
func (p *PebbleQueueProvider) SendBatch(ctx context.Context, envelopes []*queues.QueueItem) error {
	batch := p.db.NewBatch()

	for _, item := range envelopes {
		timestamp := time.Now().UnixNano()
		if item.VisibleOn != nil {
			timestamp = item.VisibleOn.UnixNano()
		}

		key := p.generateKey(item.Queue, timestamp)
		item.MessageID = string(key)
		value, err := json.Marshal(item)
		if err != nil {
			return err
		}
		_ = batch.Set(key, value, nil)
	}

	return batch.Commit(pebble.Sync)
}

// Receive retrieves messages while respecting visibility timeouts.
func (p *PebbleQueueProvider) Receive(ctx context.Context, args *queues.ReceiveArgs) ([]*queues.QueueItem, error) {
	prefix := p.options.Prefix + args.Queue
	var messages []*queues.QueueItem
	count := 0

	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: []byte(prefix + "\xff"),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	batch := p.db.NewBatch()

	for iter.First(); iter.Valid() && count < int(args.BatchSize); iter.Next() {
		var item queues.QueueItem
		if err := json.Unmarshal(iter.Value(), &item); err != nil {
			continue
		}

		// Skip messages that are not yet visible
		if item.VisibleOn != nil && item.VisibleOn.After(time.Now()) {
			continue
		}

		// Assign a handle and update visibility timeout
		handle := uuid.New().String()
		item.Handle = handle
		newVisibleTime := time.Now().Add(time.Duration(*args.VisibilityTimeout) * time.Second)
		item.VisibleOn = &newVisibleTime

		// Update the message in the queue
		updatedValue, _ := json.Marshal(item)
		_ = batch.Set(iter.Key(), updatedValue, nil)

		messages = append(messages, &item)
		count++
	}

	_ = batch.Commit(pebble.Sync)
	return messages, nil
}

// Remove deletes a message only if the handle matches.
func (p *PebbleQueueProvider) Remove(ctx context.Context, item *queues.QueueItem) (bool, error) {
	existing, closer, err := p.db.Get([]byte(item.MessageID))
	if err != nil {
		return false, fmt.Errorf("message not found")
	}
	defer closer.Close()

	var storedItem queues.QueueItem
	if err := json.Unmarshal(existing, &storedItem); err != nil {
		return false, err
	}

	// Ensure the handle matches before deleting
	if storedItem.Handle != item.Handle {
		return false, fmt.Errorf("invalid handle")
	}

	err = p.db.Delete([]byte(item.MessageID), pebble.Sync)
	return err == nil, err
}

// Renew extends the visibility timeout of a received message.
func (p *PebbleQueueProvider) Renew(ctx context.Context, item *queues.QueueItem) (bool, error) {
	existing, closer, err := p.db.Get([]byte(item.MessageID))
	if err != nil {
		return false, fmt.Errorf("message not found")
	}
	defer closer.Close()

	var storedItem queues.QueueItem
	if err := json.Unmarshal(existing, &storedItem); err != nil {
		return false, err
	}

	// Ensure the handle matches before renewing
	if storedItem.Handle != item.Handle {
		return false, fmt.Errorf("invalid handle")
	}

	// Extend visibility timeout
	newVisibleTime := time.Now().Add(time.Duration(*storedItem.VisibilityTimeout) * time.Second)
	storedItem.VisibleOn = &newVisibleTime

	// Save the updated message
	updatedValue, _ := json.Marshal(storedItem)
	err = p.db.Set([]byte(item.MessageID), updatedValue, pebble.Sync)
	return err == nil, err
}

// Purge removes all messages from a queue using optimized `DeleteRange`.
func (p *PebbleQueueProvider) Purge(ctx context.Context, queue string) error {
	prefix := p.options.Prefix + queue

	// Use optimized DeleteRange if supported
	err := p.db.DeleteRange([]byte(prefix), []byte(prefix+"\xff"), pebble.Sync)
	if err == nil {
		return nil
	}

	// Fallback to batch deletion if DeleteRange fails (older Pebble versions)
	batch := p.db.NewBatch()
	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: []byte(prefix + "\xff"),
	})
	if err != nil {
		return fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		_ = batch.Delete(iter.Key(), nil)
	}

	err = batch.Commit(pebble.Sync)
	return err
}

func (p *PebbleQueueProvider) GetMessageCounts(ctx context.Context, queue string) (*queues.MessageStatistics, error) {
	prefix := p.options.Prefix + queue
	total := 0
	inflight := 0

	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: []byte(prefix + "\xff"),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	now := time.Now()

	for iter.First(); iter.Valid(); iter.Next() {
		var item queues.QueueItem
		if err := json.Unmarshal(iter.Value(), &item); err != nil {
			continue
		}

		total++

		// A message is in-flight if:
		// 1. It has been received (has a Handle)
		// 2. It is still invisible due to its VisibilityTimeout
		if item.Handle != "" && item.VisibleOn != nil && item.VisibleOn.After(now) {
			inflight++
		}
	}

	return queues.NewMessageStatistics(total, inflight), nil
}

// Close shuts down the Pebble database.
func (p *PebbleQueueProvider) Close() error {
	return p.db.Close()
}
