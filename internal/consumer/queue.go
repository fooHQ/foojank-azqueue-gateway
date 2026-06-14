package consumer

import (
	"context"
	"iter"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue/v2"
	protoagent "github.com/foohq/foojank-proto/go/agent"

	"github.com/foohq/foojank-azqueue-gateway/internal/message"
)

type QueueConsumerConfig struct {
	Connection *azqueue.QueueClient
}

type QueueConsumer struct {
	conf QueueConsumerConfig
}

func NewQueueConsumer(conf QueueConsumerConfig) *QueueConsumer {
	return &QueueConsumer{
		conf: conf,
	}
}

func (c *QueueConsumer) Messages(ctx context.Context) iter.Seq2[message.Msg, error] {
	return func(yield func(message.Msg, error) bool) {
		for ctx.Err() == nil {
			resp, err := c.conf.Connection.DequeueMessages(ctx, &azqueue.DequeueMessagesOptions{
				NumberOfMessages:  new(int32(1)), // TODO: increase number
				VisibilityTimeout: nil,           // TODO: configure
			})
			if err != nil {
				yield(nil, err)
				return
			}

			for _, msg := range resp.Messages {
				msgID := *msg.MessageID
				popReceipt := *msg.PopReceipt
				msgText := *msg.MessageText
				data, err := protoagent.Unmarshal([]byte(msgText))
				if err != nil {
					_, err := c.conf.Connection.DeleteMessage(ctx, msgID, popReceipt, nil)
					if err != nil {
						yield(nil, err)
						return
					}
					// Invalid messages are not propagated to a caller (do not call yield!).
					continue
				}

				yield(QueueConsumerMessage{
					id:      msgID,
					subject: "", // TODO
					ack: func() error {
						_, err := c.conf.Connection.DeleteMessage(ctx, msgID, popReceipt, nil)
						return err
					},
					data: data,
				}, nil)
			}
		}
	}
}

type QueueConsumerMessage struct {
	id      string
	subject string
	ack     func() error
	data    any
}

func (m QueueConsumerMessage) ID() string {
	return m.id
}

func (m QueueConsumerMessage) Subject() string {
	return m.subject
}

func (m QueueConsumerMessage) Data() any {
	return m.data
}

func (m QueueConsumerMessage) Ack() error {
	return m.ack()
}
