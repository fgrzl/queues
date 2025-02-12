package azure

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	"github.com/fgrzl/queues"
)

// AzureQueueProvider provides an Azure Storage Queue-based implementation of QueueProvider.
type AzureQueueProvider struct {
	options *QueueProviderOptions
	queues  sync.Map
}

// QueueProviderOptions holds configuration options for Azure Storage Queues.
type QueueProviderOptions struct {
	Prefix                    string
	Endpoint                  string
	UseDefaultAzureCredential bool
	SharedKeyCredential       *azqueue.SharedKeyCredential
}

func NewSharedKeyCredential(accountName, accountKey string) (*azqueue.SharedKeyCredential, error) {
	return azqueue.NewSharedKeyCredential(accountName, accountKey)
}

// NewQueueProvider initializes a new AzureQueueProvider.
func NewQueueProvider(options *QueueProviderOptions) (queues.QueueProvider, error) {

	// todo : validate options

	return &AzureQueueProvider{
		options: options,
	}, nil
}

// Receive fetches messages from the queue.
func (p *AzureQueueProvider) Receive(ctx context.Context, args *queues.ReceiveArgs) ([]*queues.QueueItem, error) {
	if args.Queue == "" {
		return nil, fmt.Errorf("queue name is required")
	}

	client, err := p.getClient(ctx, args.Queue)
	if err != nil {
		return nil, err
	}

	visibilityTimeout := args.GetVisibilityTimeoutAsSeconds()

	resp, err := client.DequeueMessages(ctx, &azqueue.DequeueMessagesOptions{
		NumberOfMessages:  &args.BatchSize,
		VisibilityTimeout: &visibilityTimeout,
	})
	if err != nil {
		return nil, err
	}

	var results []*queues.QueueItem
	for _, message := range resp.Messages {
		if *message.DequeueCount >= 10 {
			// Move to DLQ
			dlqClient, _ := p.getClient(ctx, args.Queue+"-dlq")
			_, _ = dlqClient.EnqueueMessage(ctx, *message.MessageText, nil)
			_, _ = client.DeleteMessage(ctx, *message.MessageID, *message.PopReceipt, nil)
			continue
		}

		var envelope *queues.QueueItem
		messageText, err := base64.StdEncoding.DecodeString(*message.MessageText)
		if err != nil {
			slog.Error("failed to decode message", "error", err)
			continue
		}

		if err := json.Unmarshal([]byte(messageText), &envelope); err != nil {
			slog.Error("failed to unmarshal message", "error", err)
			continue
		}

		envelope.MessageID = *message.MessageID
		envelope.Handle = *message.PopReceipt
		envelope.VisibilityTimeout = args.VisibilityTimeout
		visibleOn := time.Now().Add(time.Duration(*args.VisibilityTimeout) * time.Second)
		envelope.VisibleOn = &visibleOn

		results = append(results, envelope)
	}

	return results, nil
}

// Send enqueues a message to the queue.
func (p *AzureQueueProvider) Send(ctx context.Context, item *queues.QueueItem) error {
	if item.Queue == "" {
		return fmt.Errorf("queue name is required")
	}

	client, err := p.getClient(ctx, item.Queue)
	if err != nil {
		return err
	}

	data, err := json.Marshal(item)
	if err != nil {
		return err
	}

	messageText := base64.StdEncoding.EncodeToString(data)

	visibilityTimeout := item.GetVisibilityTimeoutAsSeconds()

	_, err = client.EnqueueMessage(ctx, messageText, &azqueue.EnqueueMessageOptions{
		VisibilityTimeout: &visibilityTimeout,
	})
	return err
}

// SendBatch sends multiple messages to the queue sequentially.
func (p *AzureQueueProvider) SendBatch(ctx context.Context, items []*queues.QueueItem) error {
	if len(items) == 0 {
		return nil
	}

	grouped := make(map[string][]*queues.QueueItem)
	for _, e := range items {
		grouped[e.Queue] = append(grouped[e.Queue], e)
	}

	for queue, items := range grouped {
		client, err := p.getClient(ctx, queue)
		if err != nil {
			return err
		}

		// Process messages sequentially
		for _, item := range items {
			data, err := json.Marshal(item)
			if err != nil {
				return err
			}

			messageText := base64.StdEncoding.EncodeToString(data)
			visibilityTimeout := item.GetVisibilityTimeoutAsSeconds()

			_, err = client.EnqueueMessage(ctx, messageText, &azqueue.EnqueueMessageOptions{
				VisibilityTimeout: &visibilityTimeout,
			})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Remove deletes a message from the queue.
func (p *AzureQueueProvider) Remove(ctx context.Context, item *queues.QueueItem) (bool, error) {
	if item.Queue == "" || item.MessageID == "" || item.Handle == "" {
		return false, fmt.Errorf("missing required fields to remove message")
	}

	client, err := p.getClient(ctx, item.Queue)
	if err != nil {
		return false, err
	}

	_, err = client.DeleteMessage(ctx, item.MessageID, item.Handle, nil)
	return err == nil, err
}

// Renew updates the visibility timeout of a message.
func (p *AzureQueueProvider) Renew(ctx context.Context, item *queues.QueueItem) (bool, error) {
	if item.Queue == "" || item.MessageID == "" || item.Handle == "" {
		return false, fmt.Errorf("missing required fields to renew message")
	}

	visibilityTimeout := item.GetVisibilityTimeoutAsSeconds()

	client, err := p.getClient(ctx, item.Queue)
	if err != nil {
		return false, err
	}

	_, err = client.UpdateMessage(ctx, item.MessageID, item.Handle, "", &azqueue.UpdateMessageOptions{
		VisibilityTimeout: &visibilityTimeout,
	})
	return err == nil, err
}

// Purge clears all messages in the queue.
func (p *AzureQueueProvider) Purge(ctx context.Context, queue string) error {
	if queue == "" {
		return fmt.Errorf("queue name is required")
	}

	client, err := p.getClient(ctx, queue)
	if err != nil {
		return err
	}

	_, err = client.ClearMessages(ctx, nil)
	return err
}

// GetMessageCounts retrieves the approximate message count.
func (p *AzureQueueProvider) GetMessageCounts(ctx context.Context, queue string) (*queues.MessageStatistics, error) {
	client, err := p.getClient(ctx, queue)
	if err != nil {
		return &queues.MessageStatistics{}, err
	}

	props, err := client.GetProperties(ctx, nil)
	if err != nil {
		return &queues.MessageStatistics{}, err
	}

	return &queues.MessageStatistics{
		ApproximateCount:         int(*props.ApproximateMessagesCount),
		ApproximateCountInflight: 0,
	}, nil
}

// Close releases any allocated resources.
func (p *AzureQueueProvider) Close() error {
	p.queues.Range(func(key, value interface{}) bool {
		p.queues.Delete(key)
		return true
	})
	return nil
}

// getClient retrieves or creates a new Azure Queue client.
func (p *AzureQueueProvider) getClient(ctx context.Context, queue string) (*azqueue.QueueClient, error) {
	prefixedQueueName := fmt.Sprintf("%s-%s", p.options.Prefix, queue)
	prefixedQueueName = toKebabCase(prefixedQueueName)

	clientIface, loaded := p.queues.Load(prefixedQueueName)
	if loaded {
		return clientIface.(*azqueue.QueueClient), nil
	}

	url := fmt.Sprintf("%s/%s", p.options.Endpoint, prefixedQueueName)

	var client *azqueue.QueueClient
	if p.options.UseDefaultAzureCredential {
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, err
		}
		client, _ = azqueue.NewQueueClient(url, cred, nil)
	} else if p.options.SharedKeyCredential != nil {
		client, _ = azqueue.NewQueueClientWithSharedKeyCredential(url, p.options.SharedKeyCredential, nil)
	} else {
		return nil, fmt.Errorf("please provide a valid Azure credential")
	}

	_, err := client.Create(ctx, nil)
	if err != nil {
		return nil, err
	}

	p.queues.Store(prefixedQueueName, client)
	return client, nil
}

// ToKebabCase converts a string to kebab-case.
func toKebabCase(s string) string {
	return strings.ToLower(strings.ReplaceAll(s, "_", "-"))
}
