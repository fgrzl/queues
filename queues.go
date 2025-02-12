package queues

import (
	"context"
	"time"

	"github.com/fgrzl/json/polymorphic"
	"github.com/google/uuid"
)

// QueueProvider defines an interface for interacting with a queue infrastructure.
type QueueProvider interface {
	// Receive messages from the queue.
	Receive(ctx context.Context, args *ReceiveArgs) ([]*QueueItem, error)

	// Send a job queue command.
	Send(ctx context.Context, item *QueueItem) error

	// Send a batch of job queue messages.
	SendBatch(ctx context.Context, items []*QueueItem) error

	// Remove a job queue command.
	Remove(ctx context.Context, item *QueueItem) (bool, error)

	// Renew a job queue command.
	Renew(ctx context.Context, item *QueueItem) (bool, error)

	// Purge the command queue.
	Purge(ctx context.Context, item string) error

	// Get approximate message counts.
	GetMessageCounts(ctx context.Context, item string) (*MessageStatistics, error)

	// Close releases any resources used by the provider.
	Close() error
}

// QueueItem represents a message in the queue.
type QueueItem struct {
	// The request ID.
	ID uuid.UUID `json:"id"`

	// The schedule ID.
	ScheduleID *uuid.UUID `json:"schedule_id,omitempty"`

	// The ID assigned by the queue provider.
	MessageID string `json:"message_id,omitempty"`

	// The handle assigned by the queue provider.
	Handle string `json:"handle,omitempty"`

	// The queue name.
	Queue string `json:"queue"`

	// The visibility timeout.
	VisibilityTimeout *time.Duration `json:"visibility_timeout"`

	// The next visible time.
	VisibleOn *time.Time `json:"visible_on,omitempty"`

	// The message content.
	Content *polymorphic.Envelope `json:"content"`

	// The number of times the message has been re-queued.
	RequeueCount int `json:"requeue_count"`
}

func (item *QueueItem) GetVisibilityTimeoutAsSeconds() int32 {
	if item == nil {
		return 0
	}
	return convertDurationToSeconds(item.VisibilityTimeout)
}

// NewQueueItem creates a new RequestEnvelope with default values.
func NewQueueItem(queue string, content *polymorphic.Envelope) *QueueItem {
	return &QueueItem{
		ID:      uuid.New(),
		Queue:   queue,
		Content: content,
	}
}

// ReceiveArgs represents the arguments for receiving messages from a queue.
type ReceiveArgs struct {
	// The queue name.
	Queue string `json:"queue"`

	// The batch size.
	BatchSize int32 `json:"batch_size,omitempty"`

	// The visibility timeout in seconds.
	VisibilityTimeout *time.Duration `json:"visibility_timeout,omitempty"`
}

func (args *ReceiveArgs) GetVisibilityTimeoutAsSeconds() int32 {
	if args == nil {
		return 0
	}
	return convertDurationToSeconds(args.VisibilityTimeout)
}

// NewReceiveArgs creates a new RequestReceiveArgs with default values.
func NewReceiveArgs(queue string) ReceiveArgs {
	return ReceiveArgs{
		Queue:             queue,
		BatchSize:         defaultBatchSize,
		VisibilityTimeout: defaultVisibilityTimeout,
	}
}

// MessageStatistics represents the approximate message counts in a queue.
type MessageStatistics struct {
	// The number of messages.
	ApproximateCount int `json:"approximate_count"`

	// The number of messages currently visible (in-flight).
	ApproximateCountInflight int `json:"approximate_count_inflight"`
}

// NewMessageStatistics creates a new instance with given counts.
func NewMessageStatistics(approximateCount, approximateCountInflight int) *MessageStatistics {
	return &MessageStatistics{
		ApproximateCount:         approximateCount,
		ApproximateCountInflight: approximateCountInflight,
	}
}

func NewDurationFromMinutes(minutes int) time.Duration {
	duration := time.Duration(minutes) * time.Minute
	return duration
}

func NewDurationFromSeconds(seconds int32) *time.Duration {
	duration := time.Duration(seconds) * time.Second
	return &duration
}

var defaultVisibilityTimeout = NewDurationFromSeconds(30)
var defaultBatchSize = int32(10)

func convertDurationToSeconds(duration *time.Duration) int32 {
	var result int32
	if duration != nil {
		seconds := int32(duration.Seconds())
		result = seconds
	}
	return result
}
