package consumer

import (
	"context"
	"errors"
	"iter"

	protoagent "github.com/foohq/foojank-proto/go/agent"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/foohq/foojank-azqueue-gateway/internal/message"
)

type StreamConsumerConfig struct {
	Connection jetstream.JetStream
	Stream     string
	Consumer   string
}

type StreamConsumer struct {
	conf StreamConsumerConfig
}

func NewStreamConsumer(args StreamConsumerConfig) *StreamConsumer {
	return &StreamConsumer{
		conf: args,
	}
}

func (s *StreamConsumer) Messages(ctx context.Context) iter.Seq2[message.Msg, error] {
	consumer, err := s.conf.Connection.Consumer(ctx, s.conf.Stream, s.conf.Consumer)
	if err != nil {
		return func(yield func(message.Msg, error) bool) {
			yield(nil, err)
		}
	}

	return func(yield func(message.Msg, error) bool) {
		for ctx.Err() == nil {
			msg, err := consumer.Next(jetstream.FetchContext(ctx))
			if err != nil {
				if errors.Is(err, jetstream.ErrMsgIteratorClosed) {
					yield(nil, err)
					return
				}
				continue
			}

			data, err := protoagent.Unmarshal(msg.Data())
			if err != nil {
				err := msg.Ack()
				if err != nil {
					yield(nil, err)
					return
				}
				continue
			}

			yield(StreamConsumerMessage{
				msg:  msg,
				data: data,
			}, nil)
		}
	}
}

type StreamConsumerMessage struct {
	msg  jetstream.Msg
	data any
}

func (m StreamConsumerMessage) ID() string {
	return m.msg.Headers().Get(nats.MsgIdHdr)
}

func (m StreamConsumerMessage) Subject() string {
	return m.msg.Subject()
}

func (m StreamConsumerMessage) Data() any {
	return m.data
}

func (m StreamConsumerMessage) Ack() error {
	return m.msg.Ack()
}
