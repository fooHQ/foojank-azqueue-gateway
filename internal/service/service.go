package service

import (
	"context"
	"errors"
	"iter"
	"sync"
	"time"

	"github.com/foohq/foojank-azqueue-gateway/log"

	"github.com/foohq/foojank-azqueue-gateway/internal/message"
)

type Arguments struct {
	ID                string
	StreamConsumer    Consumer
	CmdQueuePublisher Publisher
	EvtQueueConsumer  Consumer
	StreamPublisher   Publisher
	Handler           Handler
}

type Service struct {
	args Arguments
}

func New(args Arguments) *Service {
	return &Service{
		args: args,
	}
}

func (s *Service) Start(ctx context.Context) error {
	log.Debug("Service started", "service", "vessel", "id", s.args.ID)
	defer log.Debug("Service stopped", "service", "vessel", "id", s.args.ID)

	streamConsumerOutCh := make(chan message.Msg)
	queuePublisherInCh := make(chan message.Msg, 128)
	queueConsumerOutCh := make(chan message.Msg)
	streamPublisherInCh := make(chan message.Msg, 128)
	// Capacity must match the total number of goroutines tracked by the WaitGroup.
	termCh := make(chan struct{}, 5)

	var wg sync.WaitGroup

	streamConsumerCtx, streamConsumerCancel := context.WithCancel(context.Background())
	defer streamConsumerCancel()

	wg.Go(func() {
		err := consumer(streamConsumerCtx, s.args.StreamConsumer, streamConsumerOutCh)
		if err != nil {
			log.Debug("Stream consumer error", "error", err)
		}
		termCh <- struct{}{}
	})

	routerCtx, routerCancel := context.WithCancel(context.Background())
	defer routerCancel()

	wg.Go(func() {
		err := router(routerCtx, s.args.Handler, streamConsumerOutCh, queuePublisherInCh)
		if err != nil {
			log.Debug("Router error", "error", err)
		}
		termCh <- struct{}{}
	})

	queuePublisherCtx, queuePublisherCancel := context.WithCancel(context.Background())
	defer queuePublisherCancel()

	wg.Go(func() {
		err := publisher(queuePublisherCtx, s.args.CmdQueuePublisher, queuePublisherInCh)
		if err != nil {
			log.Debug("Queue publisher error", "error", err)
		}
		termCh <- struct{}{}
	})

	queueConsumerCtx, queueConsumerCancel := context.WithCancel(context.Background())
	defer queueConsumerCancel()

	wg.Go(func() {
		err := consumer(queueConsumerCtx, s.args.EvtQueueConsumer, queueConsumerOutCh)
		if err != nil {
			log.Debug("Queue consumer error", "error", err)
		}
		termCh <- struct{}{}
	})

	streamPublisherCtx, streamPublisherCancel := context.WithCancel(context.Background())
	defer streamPublisherCancel()

	wg.Go(func() {
		err := publisher(streamPublisherCtx, s.args.StreamPublisher, streamPublisherInCh)
		if err != nil {
			log.Debug("Stream publisher error", "error", err)
		}
		termCh <- struct{}{}
	})

	cancels := []context.CancelFunc{
		streamConsumerCancel,
		queuePublisherCancel,
		queueConsumerCancel,
		streamPublisherCancel,
	}

	select {
	case <-ctx.Done():
		for _, cancel := range cancels {
			cancel()
			<-termCh
		}
	case <-termCh:
		// If an error occurs in one of the services, cancel all services without waiting for them to finish.
		// Some messages may be lost in the process.
		for _, cancel := range cancels {
			cancel()
		}
	}

	wg.Wait()

	return nil
}

type Consumer interface {
	Messages(context.Context) iter.Seq2[message.Msg, error]
}

func consumer(ctx context.Context, consumer Consumer, outputCh chan message.Msg) error {
	log.Debug("Service started", "service", "vessel.consumer")
	defer log.Debug("Service stopped", "service", "vessel.consumer")

	for msg, err := range consumer.Messages(ctx) {
		if err != nil {
			log.Debug("Cannot read a message", "error", err)
			return err
		}

		err = forwardMessage(outputCh, msg)
		if err != nil {
			log.Debug("Cannot forward a message", "error", err)
			continue
		}
	}

	return nil
}

type Publisher interface {
	Publish(context.Context, message.Msg) error
}

func publisher(ctx context.Context, publisher Publisher, inputCh <-chan message.Msg) error {
	log.Debug("Service started", "service", "vessel.publisher")
	defer log.Debug("Service stopped", "service", "vessel.publisher")

	var exit bool
	var cancel context.CancelFunc

loop:
	for {
		select {
		case msg := <-inputCh:
			err := publisher.Publish(ctx, msg)
			if err != nil {
				log.Debug("Cannot publish a message", "subject", msg.Subject(), "error", err)
				continue
			}

			_ = msg.Ack()

		case <-ctx.Done():
			if !exit {
				ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
				exit = true
				continue loop
			}
			break loop
		}
	}

	cancel()

	if len(inputCh) != 0 {
		log.Debug("Some messages were lost", "count", len(inputCh))
	}

	return nil
}

type Handler interface {
	Match(message.Msg) (func(context.Context) (message.Msg, error), bool)
}

func router(ctx context.Context, handler Handler, inputCh <-chan message.Msg, outputCh chan<- message.Msg) error {
	log.Debug("Service started", "service", "vessel.router")
	defer log.Debug("Service stopped", "service", "vessel.router")

	for {
		for {
			select {
			case msg := <-inputCh:
				fn, ok := handler.Match(msg)
				if !ok {
					_ = msg.Ack()
					continue
				}

				resp, err := fn(ctx)
				if err != nil {
					_ = msg.Ack()
					continue
				}

				err = forwardMessage(outputCh, resp)
				if err != nil {
					log.Debug("Cannot forward a message", "error", err)
					continue
				}

			case <-ctx.Done():
				return nil
			}
		}
	}
}

func forwardMessage(outputCh chan<- message.Msg, msg message.Msg) error {
	select {
	case outputCh <- msg:
		return nil
	case <-time.After(10 * time.Second):
		return errors.New("timeout")
	}
}
