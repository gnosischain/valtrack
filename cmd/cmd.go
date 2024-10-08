package cmd

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/chainbound/valtrack/clickhouse"
	"github.com/chainbound/valtrack/consumer"
	"github.com/chainbound/valtrack/discovery"
	"github.com/google/uuid"

	"github.com/rs/zerolog"
	"github.com/urfave/cli/v2"
)

var ConsumerCommand = &cli.Command{
	Name:   "consumer",
	Usage:  "run the consumer",
	Action: runConsumer,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "log-level",
			Usage:   "Log level",
			Aliases: []string{"l"},
			Value:   "info",
		},
		&cli.StringFlag{
			Name:    "nats-url",
			Usage:   "NATS server URL (needs JetStream)",
			Aliases: []string{"n"},
			Value:   "nats://localhost:4222",
		},
		&cli.StringFlag{
			Name:  "name",
			Usage: "Consumer name",
			Value: "consumer-" + uuid.New().String(),
		},
		&cli.StringFlag{
			Name:    "endpoint",
			Usage:   "Clickhouse server endpoint",
			Value:   "", // If empty URL, run the consumer without Clickhouse
			EnvVars: []string{"VALTRACK_CH_URL"},
		},
		&cli.StringFlag{
			Name:  "db",
			Usage: "Clickhouse database name",
			Value: "default",
		},
		&cli.StringFlag{
			Name:    "username",
			Usage:   "Clickhouse username",
			Value:   "default",
			EnvVars: []string{"CLICKHOUSE_USER"},
		},
		&cli.StringFlag{
			Name:    "password",
			Usage:   "Clickhouse password",
			Value:   "",
			EnvVars: []string{"CLICKHOUSE_PASSWORD"},
		},
		&cli.StringFlag{
			Name:  "dune.namespace",
			Usage: "Dune namespace",
		},
		&cli.StringFlag{
			Name:  "dune.api-key",
			Usage: "Dune API key",
		},
		&cli.Uint64Flag{
			Name:  "max-validator-batch-size",
			Usage: "Clickhouse max validator batch size",
			Value: 16,
		},
		&cli.Uint64Flag{
			Name:  "max-ip-metadata-batch-size",
			Usage: "Clickhouse max IP metadata batch size",
			Value: 16,
		},
		&cli.Uint64Flag{
			Name:  "max-peer-discovered-events-batch-size",
			Usage: "Clickhouse max peer discovered events batch size",
			Value: 512,
		},
		&cli.Uint64Flag{
			Name:  "max-metadata-received-events-batch-size",
			Usage: "Clickhouse max metadata received events batch size",
			Value: 512,
		},
	},
}

var SentryCommand = &cli.Command{
	Name:   "sentry",
	Usage:  "run the sentry node",
	Action: runSentry,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "log-level",
			Usage:   "log level",
			Aliases: []string{"l"},
			Value:   "info",
		},
		&cli.StringFlag{
			Name:    "nats-url",
			Usage:   "NATS server URL (needs JetStream)",
			Aliases: []string{"n"},
			Value:   "", // If empty URL, run the sentry without NATS
		},
	},
}

func runConsumer(c *cli.Context) error {
	cfg := consumer.ConsumerConfig{
		LogLevel:      c.String("log-level"),
		NatsURL:       c.String("nats-url"),
		Name:          c.String("name"),
		DuneNamespace: c.String("dune.namespace"),
		DuneApiKey:    c.String("dune.api-key"),
		ChCfg: clickhouse.ClickhouseConfig{
			Endpoint:                           c.String("endpoint"),
			DB:                                 c.String("db"),
			Username:                           c.String("username"),
			Password:                           c.String("password"),
			MaxValidatorBatchSize:              c.Uint64("max-validator-batch-size"),
			MaxIPMetadataBatchSize:             c.Uint64("max-ip-metadata-batch-size"),
			MaxPeerDiscoveredEventsBatchSize:   c.Uint64("max-peer-discovered-events-batch-size"),
			MaxMetadataReceivedEventsBatchSize: c.Uint64("max-metadata-received-events-batch-size"),
		},
	}

	level, _ := zerolog.ParseLevel(cfg.LogLevel)
	zerolog.SetGlobalLevel(level)

	consumer.RunConsumer(&cfg)
	return nil
}

func runSentry(c *cli.Context) error {
	level, _ := zerolog.ParseLevel(c.String("log-level"))
	zerolog.SetGlobalLevel(level)

	disc, err := discovery.NewDiscovery(c.String("nats-url"))
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	go func() {
		if err := disc.Start(ctx); err != nil {
			panic(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit

	return nil
}
