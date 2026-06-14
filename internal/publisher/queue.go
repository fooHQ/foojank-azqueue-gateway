package publisher

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue/v2"
	protoagent "github.com/foohq/foojank-proto/go/agent"

	"github.com/foohq/foojank-azqueue-gateway/internal/message"
)

type QueuePublisherConfig struct {
	Connection *azqueue.ServiceClient
}

type QueuePublisher struct {
	conf QueuePublisherConfig
}

func NewQueuePublisher(conf QueuePublisherConfig) *QueuePublisher {
	return &QueuePublisher{
		conf: conf,
	}
}

func (p *QueuePublisher) Publish(ctx context.Context, msg message.Msg) error {
	data, err := protoagent.Marshal(protoagent.Envelope{
		Subject: msg.Subject(),
		Payload: msg.Data(),
	})
	if err != nil {
		return err
	}

	// TODO: find the proper queue name to publish messages on!
	// 	Initialize azqueue.QueueClient
	// TODO: consider caching queue so it must not be recreated for each client!
	conn := p.conf.Connection.NewQueueClient("") // TODO: get queueName!

	_, err = conn.EnqueueMessage(ctx, string(data), &azqueue.EnqueueMessageOptions{
		TimeToLive:        new(int32(-1)),
		VisibilityTimeout: nil, // TODO
	})
	if err != nil {
		return err
	}

	return nil
}
