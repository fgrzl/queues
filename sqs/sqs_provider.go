package sqs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/fgrzl/queues"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// queueProvider provides an SQS-based implementation of QueueProvider.
type queueProvider struct {
	options   *QueueProviderOptions
	queues    sync.Map
	sqsClient *sqs.Client
	mu        sync.Mutex
}

// QueueProviderOptions holds configuration options for SQS.
type QueueProviderOptions struct {
	Prefix            string
	Region            string
	AccessKey         string
	SecretKey         string
	SessionToken      string
	Endpoint          string
	UseDefaultAWS     bool // Use default AWS credentials if true
	EnableLongPolling bool // Enable long polling if true
}

// NewSQSClient creates a new SQS client with optional credentials
func newSQSClient(ctx context.Context, options *QueueProviderOptions) (*sqs.Client, error) {
	var awsCfg aws.Config
	var err error

	// Custom HTTP client with timeout control
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:      100,
			IdleConnTimeout:   90 * time.Second,
			DisableKeepAlives: false,
		},
	}

	// Load AWS config
	if options.UseDefaultAWS {
		// Use default AWS credentials (env vars, IAM, etc.)
		awsCfg, err = config.LoadDefaultConfig(ctx, config.WithHTTPClient(httpClient))
	} else {
		// Use static credentials for ElasticMQ or AWS
		awsCfg, err = config.LoadDefaultConfig(ctx,
			config.WithHTTPClient(httpClient),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(options.AccessKey, options.SecretKey, "")),
			config.WithRegion(options.Region),
		)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Override endpoint for ElasticMQ (local testing)
	if options.Endpoint != "" {
		awsCfg.BaseEndpoint = aws.String(options.Endpoint)
	}

	// Create and return SQS client
	return sqs.NewFromConfig(awsCfg), nil
}

// NewQueueProvider initializes a new SqsQueueProvider.
func NewQueueProvider(options *QueueProviderOptions) (queues.QueueProvider, error) {

	sqsClient, err := newSQSClient(context.Background(), options)
	if err != nil {
		return nil, err
	}

	return &queueProvider{
		options:   options,
		sqsClient: sqsClient,
	}, nil
}

// Receive fetches messages from the queue.
func (p *queueProvider) Receive(ctx context.Context, args *queues.ReceiveArgs) ([]*queues.QueueItem, error) {
	queueURL, err := p.registerQueue(ctx, args.Queue)
	if err != nil {
		return nil, err
	}

	visibilityTimeout := args.GetVisibilityTimeoutAsSeconds()
	params := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: args.BatchSize,
		VisibilityTimeout:   visibilityTimeout,
	}
	if p.options.EnableLongPolling {
		params.WaitTimeSeconds = int32(20)
	}

	resp, err := p.sqsClient.ReceiveMessage(ctx, params)
	if err != nil {
		return nil, err
	}

	var results []*queues.QueueItem
	for _, msg := range resp.Messages {
		data, err := base64.StdEncoding.DecodeString(*msg.Body)
		if err != nil {
			continue
		}

		var envelope *queues.QueueItem
		if err := json.Unmarshal(data, &envelope); err != nil {
			continue
		}

		envelope.Handle = *msg.ReceiptHandle
		envelope.VisibilityTimeout = args.VisibilityTimeout
		results = append(results, envelope)
	}

	return results, nil
}

// Send enqueues a message to SQS.
func (p *queueProvider) Send(ctx context.Context, item *queues.QueueItem) error {
	queueURL, err := p.registerQueue(ctx, item.Queue)
	if err != nil {
		return err
	}

	data, err := json.Marshal(item)
	if err != nil {
		return err
	}

	delay := item.GetVisibilityTimeoutAsSeconds()

	_, err = p.sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:     aws.String(queueURL),
		MessageBody:  aws.String(base64.StdEncoding.EncodeToString(data)),
		DelaySeconds: delay,
	})
	return err
}

// SendBatch sends multiple messages in batch.
func (p *queueProvider) SendBatch(ctx context.Context, envelopes []*queues.QueueItem) error {
	if len(envelopes) == 0 {
		return nil
	}

	grouped := make(map[string][]types.SendMessageBatchRequestEntry)
	for _, e := range envelopes {
		queueURL, err := p.registerQueue(ctx, e.Queue)
		if err != nil {
			return err
		}

		grouped[queueURL] = append(grouped[queueURL], types.SendMessageBatchRequestEntry{
			Id:          aws.String(e.ID.String()),
			MessageBody: aws.String(base64.StdEncoding.EncodeToString(mustMarshal(e))),
		})
	}

	for queueURL, batch := range grouped {
		if len(batch) > 10 {
			return errors.New("SQS batch size limit exceeded")
		}

		_, err := p.sqsClient.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
			QueueUrl: aws.String(queueURL),
			Entries:  batch,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// Remove deletes a message from SQS.
func (p *queueProvider) Remove(ctx context.Context, item *queues.QueueItem) (bool, error) {
	queueURL, err := p.getQueueURL(ctx, item.Queue)
	if err != nil {
		return false, err
	}

	_, err = p.sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: aws.String(item.Handle),
	})
	return err == nil, err
}

// Renew updates the visibility timeout of a message.
func (p *queueProvider) Renew(ctx context.Context, item *queues.QueueItem) (bool, error) {
	queueURL, err := p.getQueueURL(ctx, item.Queue)
	if err != nil {
		return false, err
	}

	visibilityTimeout := item.GetVisibilityTimeoutAsSeconds()

	_, err = p.sqsClient.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(queueURL),
		ReceiptHandle:     aws.String(item.Handle),
		VisibilityTimeout: visibilityTimeout,
	})
	return err == nil, err
}

// Purge clears all messages in the queue.
func (p *queueProvider) Purge(ctx context.Context, queue string) error {
	queueURL, err := p.getQueueURL(ctx, queue)
	if err != nil {
		return handleQueueError(err)
	}

	_, err = p.sqsClient.PurgeQueue(ctx, &sqs.PurgeQueueInput{
		QueueUrl: aws.String(queueURL),
	})
	return err
}

func handleQueueError(err error) error {
	var queueNotExistErr *types.QueueDoesNotExist
	if errors.As(err, &queueNotExistErr) {
		slog.Debug("Queue does not exist, handling gracefully.")
		return nil // Return nil to indicate no action needed
	}
	return err // Return other errors as usual
}

// GetMessageCounts retrieves the approximate message count.
func (p *queueProvider) GetMessageCounts(ctx context.Context, queue string) (*queues.MessageStatistics, error) {
	queueURL, err := p.getQueueURL(ctx, queue)
	if err != nil {
		return &queues.MessageStatistics{}, err
	}

	resp, err := p.sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	if err != nil {
		return &queues.MessageStatistics{}, err
	}

	counts := &queues.MessageStatistics{
		ApproximateCount:         parseInt(aws.String(resp.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)])),
		ApproximateCountInflight: parseInt(aws.String(resp.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)])),
	}
	return counts, nil
}

// Close releases any allocated resources.
func (p *queueProvider) Close() error {
	// no-op
	return nil
}

// registerQueue registers and retrieves the queue URL.
func (p *queueProvider) registerQueue(ctx context.Context, queue string) (string, error) {
	queueURL, err := p.getQueueURL(ctx, queue)
	if err == nil {
		return queueURL, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	prefixedQueue := p.getPrefixedQueueName(queue)
	resp, err := p.sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(prefixedQueue),
		Attributes: map[string]string{
			"MessageRetentionPeriod": "1209600",
		},
	})
	if err != nil {
		return "", err
	}

	p.queues.Store(queue, *resp.QueueUrl)
	return *resp.QueueUrl, nil
}

// getQueueURL retrieves the queue URL.
func (p *queueProvider) getQueueURL(ctx context.Context, queue string) (string, error) {
	if val, ok := p.queues.Load(queue); ok {
		return val.(string), nil
	}

	prefixedQueue := p.getPrefixedQueueName(queue)
	resp, err := p.sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(prefixedQueue),
	})
	if err != nil {
		return "", err
	}

	p.queues.Store(queue, *resp.QueueUrl)
	return *resp.QueueUrl, nil
}

// getPrefixedQueueName applies a prefix to queue names.
func (p *queueProvider) getPrefixedQueueName(queue string) string {
	return fmt.Sprintf("%s-%s", p.options.Prefix, queue)
}

// Utility functions
func parseInt(s *string) int {
	if s == nil {
		return 0
	}
	val, _ := strconv.Atoi(*s)
	return val
}

func mustMarshal(v interface{}) []byte {
	data, _ := json.Marshal(v)
	return data
}
