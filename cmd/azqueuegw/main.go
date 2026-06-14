package main

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue/v2"
	"github.com/nats-io/nats.go"
	"github.com/urfave/cli/v3"

	"github.com/foohq/foojank-azqueue-gateway"
	"github.com/foohq/foojank-azqueue-gateway/internal/consumer"
	"github.com/foohq/foojank-azqueue-gateway/internal/handler"
	"github.com/foohq/foojank-azqueue-gateway/internal/publisher"
	"github.com/foohq/foojank-azqueue-gateway/internal/service"
	"github.com/foohq/foojank-azqueue-gateway/log"

	"github.com/nats-io/nats.go/jetstream"
)

var app = &cli.Command{
	Name:    "foojank-azqueuegw",
	Usage:   "Foojank Azure Queue Storage Gateway",
	Version: azqueuegw.Version(),
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "azure-url",
			Usage: "set Azure Queue Storage URL",
		},
		&cli.StringFlag{
			Name:  "azure-tenant-id",
			Usage: "set Azure Tenant ID",
		},
		&cli.StringFlag{
			Name:  "azure-client-id",
			Usage: "set Azure Client ID",
		},
		&cli.StringFlag{
			Name:  "azure-client-secret",
			Usage: "set Azure Client Secret",
		},
		&cli.StringFlag{
			Name:  "nats-url",
			Usage: "set NATS server URL",
		},
		&cli.StringFlag{
			Name:  "nats-certificate",
			Usage: "set NATS server certificate",
		},
		&cli.StringFlag{
			Name:  "nats-user-jwt",
			Usage: "set NATS user JWT",
		},
		&cli.StringFlag{
			Name:  "nats-user-key",
			Usage: "set NATS user key",
		},
	},
	Before: func(ctx context.Context, command *cli.Command) (context.Context, error) {
		return ctx, nil
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		log.Debug("Azure Queue Gateway started")
		defer log.Debug("Azure Queue Gateway stopped")

		natsURL := command.String("nats-url")
		natsCertificate := command.String("nats-certificate")
		userJWT := command.String("nats-user-jwt")
		userKey := command.String("nats-user-key")

		// TODO: get inbox prefix!
		inboxPrefix := "!prefix!"

		connNats, err := connectNATS(ctx, natsURL, natsCertificate, userJWT, userKey, inboxPrefix)
		if err != nil {
			log.Debug("Cannot connect to the server", "server", natsURL, "error", err)
			return err
		}
		defer connNats.Conn().Close()

		azureURL := command.String("azure-url")
		azureTenantID := command.String("azure-tenant-id")
		azureClientID := command.String("azure-client-id")
		azureClientSecret := command.String("azure-client-secret")

		connAzure, err := connectAzure(azureURL, azureTenantID, azureClientID, azureClientSecret)
		if err != nil {
			log.Debug("Cannot connect to the server", "server", azureURL, "error", err)
			return err
		}

		// TODO: extract pubkey from user JWT!
		gatewayID := "!gatewayID!"
		// TODO: get stream name!
		streamName := "!streamName!"
		// TODO: get consumer name!
		consumerName := "!consumerName!"
		// TODO: get queue name (EVT queue is shared by all agents)!
		queueName := "!queueName!"

		err = service.New(service.Arguments{
			ID: gatewayID,
			StreamConsumer: consumer.NewStreamConsumer(consumer.StreamConsumerConfig{
				Connection: connNats,
				Stream:     streamName,
				Consumer:   consumerName,
			}),
			CmdQueuePublisher: publisher.NewQueuePublisher(publisher.QueuePublisherConfig{
				Connection: connAzure,
			}),
			EvtQueueConsumer: consumer.NewQueueConsumer(consumer.QueueConsumerConfig{
				Connection: connAzure.NewQueueClient(queueName),
			}),
			StreamPublisher: publisher.NewStreamPublisher(publisher.StreamPublisherConfig{
				Connection: connNats,
			}),
			Handler: handler.New(), // TODO: pass configuration
		}).Start(ctx)
		if err != nil {
			log.Debug("Cannot start the gateway", "error", err)
			return err
		}

		return nil
	},
	CommandNotFound: func(_ context.Context, c *cli.Command, s string) {
		err := fmt.Errorf("%q is not a valid command", s)
		_, _ = fmt.Fprintf(os.Stderr, "%s: %v\n", c.FullName(), err.Error())
		os.Exit(1)
	},
	OnUsageError: func(ctx context.Context, c *cli.Command, err error, _ bool) error {
		_, _ = fmt.Fprintf(os.Stderr, "%s: %v\n", c.FullName(), err.Error())
		return nil // TODO: should return error?
	},
	HideHelpCommand: true,
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	err := app.Run(ctx, os.Args)
	if err != nil {
		os.Exit(1)
	}
}

func connectNATS(ctx context.Context, server, serverCertificate, userJWT, userKey, inboxPrefix string) (jetstream.JetStream, error) {
	opts := []nats.Option{
		nats.CustomInboxPrefix(inboxPrefix),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ConnectHandler(connected),
		nats.ReconnectHandler(connected),
		nats.DisconnectErrHandler(disconnected),
		nats.ErrorHandler(failed),
	}

	if userJWT != "" && userKey != "" {
		opts = append(opts, nats.UserJWTAndSeed(userJWT, userKey))
	}

	if serverCertificate != "" {
		opts = append(
			opts,
			nats.TLSHandshakeFirst(),
			nats.ClientTLSConfig(nil, decodeCertificateHandler(serverCertificate)),
		)
	}

	nc, err := nats.Connect(server, opts...)
	if err != nil {
		return nil, err
	}

	for !nc.IsConnected() {
		select {
		case <-time.After(3 * time.Second):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	jetStream, err := jetstream.New(
		nc,
		jetstream.WithDefaultTimeout(10*time.Second),
		jetstream.WithPublishAsyncMaxPending(120),
	)
	if err != nil {
		return nil, err
	}

	return jetStream, nil
}

func connectAzure(serviceURL, tenantID, clientID, clientSecret string) (*azqueue.ServiceClient, error) {
	cred, err := azidentity.NewClientSecretCredential(tenantID, clientID, clientSecret, nil)
	if err != nil {
		return nil, err
	}

	client, err := azqueue.NewServiceClient(serviceURL, cred, nil)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func decodeCertificateHandler(s string) func() (*x509.CertPool, error) {
	return func() (*x509.CertPool, error) {
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, err
		}

		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(b)
		return pool, nil
	}
}

func connected(_ *nats.Conn) {
	log.Debug("Connected to the server")
}

func disconnected(_ *nats.Conn, err error) {
	if err != nil {
		log.Debug("Disconnected from the server", "error", err)
	} else {
		log.Debug("Disconnected from the server")
	}
}

func failed(_ *nats.Conn, _ *nats.Subscription, err error) {
	log.Debug("Connection error", "error", err)
}
