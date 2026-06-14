package publisher

import (
	"context"
	"time"

	protoagent "github.com/foohq/foojank-proto/go/agent"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"

	"github.com/foohq/foojank-azqueue-gateway/internal/message"
)

type StreamPublisherConfig struct {
	Connection jetstream.JetStream
}

type StreamPublisher struct {
	conf StreamPublisherConfig
}

func NewStreamPublisher(args StreamPublisherConfig) *StreamPublisher {
	return &StreamPublisher{
		conf: args,
	}
}

func (p *StreamPublisher) Publish(ctx context.Context, msg message.Msg) error {
	data, err := protoagent.Marshal(protoagent.Envelope{
		Payload: msg.Data(),
	})
	if err != nil {
		return err
	}

	opts := []jetstream.PublishOpt{
		jetstream.WithRetryAttempts(3),
		jetstream.WithRetryWait(250 * time.Millisecond),
	}
	_, err = p.conf.Connection.PublishMsg(
		ctx,
		&nats.Msg{
			Subject: msg.Subject(),
			Header: map[string][]string{
				jetstream.MsgIDHeader: {
					nuid.Next(),
				},
			},
			Data: data,
		},
		opts...,
	)
	if err != nil {
		return err
	}

	return nil
}
