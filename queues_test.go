package queues_test

import (
	"context"
	"testing"
	"time"

	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/queues"
	"github.com/fgrzl/queues/azure"
	"github.com/fgrzl/queues/pebble"
	"github.com/fgrzl/queues/sqs"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type DummyMessage struct {
	ID uuid.UUID `json:"id"`
}

func newDummyContent() *polymorphic.Envelope {
	return polymorphic.NewEnvelope("dummy", &DummyMessage{ID: uuid.New()})
}

func setup(t *testing.T) map[string]queues.QueueProvider {
	polymorphic.Register("dummy", func() any { return &DummyMessage{} })

	providers := map[string]queues.QueueProvider{}

	for name, factory := range map[string]func() (queues.QueueProvider, error){
		"pebble": func() (queues.QueueProvider, error) {
			path := t.TempDir()
			options := &pebble.QueueProviderOptions{
				Prefix: "test",
				Path:   path}
			return pebble.NewQueueProvider(options)
		},
		"azure": func() (queues.QueueProvider, error) {

			// default azurite configuration for local testing
			// https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=windows
			accountName := "devstoreaccount1"
			accountKey := "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
			endpoint := "http://127.0.0.1:10001/devstoreaccount1"

			credential, err := azure.NewSharedKeyCredential(accountName, accountKey)
			if err != nil {
				panic(err)
			}

			options := &azure.QueueProviderOptions{
				Prefix:              "test",
				Endpoint:            endpoint,
				SharedKeyCredential: credential,
			}
			return azure.NewQueueProvider(options)
		},
		"sqs": func() (queues.QueueProvider, error) {

			options := &sqs.QueueProviderOptions{
				Prefix:            "test",
				Region:            "elasticmq",
				AccessKey:         "test", // ElasticMQ allows any value
				SecretKey:         "test",
				Endpoint:          "http://localhost:9324", // ElasticMQ local URL
				EnableLongPolling: false,
			}
			return sqs.NewQueueProvider(options)

		},
	} {
		provider, err := factory()
		require.NoError(t, err, "Failed to initialize provider: "+name)

		// Ensure the provider is closed after the test
		t.Cleanup(func() {
			err = provider.Close()
			require.NoError(t, err, "Failed to close provider: "+name)
		})

		providers[name] = provider
	}

	return providers
}

func TestEnqueueSingleMessage(t *testing.T) {
	providers := setup(t)
	for name, provider := range providers {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			queue := "test-queue"

			// Arrange
			err := provider.Purge(ctx, queue)
			require.NoError(t, err)

			item := queues.NewQueueItem(queue, newDummyContent())

			// Act
			err = provider.Send(ctx, item)

			// Assert
			assert.NoError(t, err)
		})
	}
}

func TestEnqueueBatchMessages(t *testing.T) {
	providers := setup(t)
	for name, provider := range providers {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			queue := "test-queue"

			// Arrange
			err := provider.Purge(ctx, queue)
			require.NoError(t, err)

			var batch []*queues.QueueItem
			for i := 0; i < 5; i++ {
				batch = append(batch, queues.NewQueueItem(queue, newDummyContent()))
			}

			// Act
			err = provider.SendBatch(ctx, batch)

			// Assert
			assert.NoError(t, err)
		})
	}
}

func TestReceiveSingleMessage(t *testing.T) {
	providers := setup(t)
	for name, provider := range providers {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			queue := "test-queue"

			// Arrange
			err := provider.Purge(ctx, queue)
			require.NoError(t, err)

			item := queues.NewQueueItem(queue, newDummyContent())
			err = provider.Send(ctx, item)
			require.NoError(t, err)

			// Act
			args := queues.NewReceiveArgs(queue)
			messages, err := provider.Receive(ctx, &args)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, messages, 1)
			assert.Equal(t, item.ID, messages[0].ID)
		})
	}
}

func TestReceiveMultipleMessagesInOrder(t *testing.T) {
	providers := setup(t)
	for name, provider := range providers {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			queue := "test-queue"

			// Arrange
			err := provider.Purge(ctx, queue)
			require.NoError(t, err)

			ids := []uuid.UUID{}
			for i := 0; i < 3; i++ {
				item := queues.NewQueueItem(queue, newDummyContent())
				ids = append(ids, item.ID)
				err = provider.Send(ctx, item)
				require.NoError(t, err)
			}

			// Act
			args := queues.NewReceiveArgs(queue)
			messages, err := provider.Receive(ctx, &args)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, messages, 3)
			for i := 0; i < 3; i++ {
				assert.Equal(t, ids[i], messages[i].ID)
			}
		})
	}
}

func TestDelayedMessageVisibility(t *testing.T) {
	providers := setup(t)
	for name, provider := range providers {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			queue := "test-queue"
			delay := queues.NewDurationFromSeconds(2)

			// Arrange
			err := provider.Purge(ctx, queue)
			require.NoError(t, err)

			item := queues.NewQueueItem(queue, newDummyContent())
			item.VisibilityTimeout = delay
			err = provider.Send(ctx, item)
			require.NoError(t, err)

			// Act & Assert
			args := queues.NewReceiveArgs(queue)
			require.Eventually(t, func() bool {
				messages, _ := provider.Receive(ctx, &args)
				return len(messages) == 1
			}, 3*time.Second, 100*time.Millisecond)
		})
	}
}

func TestRemoveMessage(t *testing.T) {
	providers := setup(t)
	for name, provider := range providers {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			queue := "test-queue"

			// Arrange
			err := provider.Purge(ctx, queue)
			require.NoError(t, err)

			item := queues.NewQueueItem(queue, newDummyContent())
			err = provider.Send(ctx, item)
			require.NoError(t, err)

			args := queues.NewReceiveArgs(queue)
			messages, err := provider.Receive(ctx, &args)
			require.NoError(t, err)
			require.Len(t, messages, 1)

			// Act
			success, err := provider.Remove(ctx, messages[0])

			// Assert
			assert.NoError(t, err)
			assert.True(t, success)

			messages, err = provider.Receive(ctx, &args)
			assert.NoError(t, err)
			assert.Empty(t, messages, "Message should be removed")
		})
	}
}

func TestPurgeQueue(t *testing.T) {
	providers := setup(t)
	for name, provider := range providers {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			queue := "test-queue"

			// Arrange
			err := provider.Purge(ctx, queue)
			require.NoError(t, err)

			// Act
			err = provider.Purge(ctx, queue)

			// Assert
			assert.NoError(t, err)
		})
	}
}
