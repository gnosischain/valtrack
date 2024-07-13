package clickhouse

import (
	"context"
	//"crypto/tls"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/chainbound/valtrack/log"
	"github.com/chainbound/valtrack/types"
	"github.com/rs/zerolog"
)

func ValidatorMetadataDDL(db string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.validator_metadata (
		enr String,
		id String,
		multiaddr String,
		epoch Int32,
		seq_number Int64,
		syncnets String,
		attnets String,
		long_lived_subnets Array(Int64),
		subscribed_subnets Array(Int64),
		client_version String,
		crawler_id String,
		crawler_location String,
		timestamp Int64,
	) ENGINE = MergeTree()
PRIMARY KEY (id, timestamp)`, db)
}

func IPMetadataDDL(db string) string {
    return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.ip_metadata (
			ip String,
			hostname String,
			city String,
			region String,
			country String,
			latitude Float64,
			longitude Float64,
			postal_code String,
			asn String,
			asn_organization String,
			asn_type String
		) ENGINE = MergeTree()
		ORDER BY ip;
	`, db)
}

func PeerDiscoveredEventsDDL(db string) string {
    return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.peer_discovered_events (
			enr String,
			id String,
			ip String,
			port Int32,
			crawler_id String,
			crawler_location String,
			timestamp DateTime64(3, 'UTC')
		) ENGINE = MergeTree()
		ORDER BY (timestamp, id);
	`, db)
}

func MetadataReceivedEventsDDL(db string) string {
    return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.metadata_received_events (
			enr String,
			id String,
			multiaddr String,
			epoch Int32,
			subscribed_subnets Array(Int64),
			client_version String,
			crawler_id String,
			crawler_location String,
			timestamp DateTime64(3, 'UTC')
		) ENGINE = MergeTree()
		ORDER BY (timestamp, id);
	`, db)
}

type ClickhouseConfig struct {
	Endpoint string
	DB       string
	Username string
	Password string

	MaxValidatorBatchSize       uint64
    MaxIPMetadataBatchSize      uint64 
	MaxPeerDiscoveredEventsBatchSize uint64
	MaxMetadataReceivedEventsBatchSize uint64
}

type ClickhouseClient struct {
	cfg *ClickhouseConfig
	log zerolog.Logger

	chConn driver.Conn

	ValidatorEventChan      chan *types.ValidatorEvent      
    IPMetadataEventChan     chan *types.IPMetadataEvent     
	PeerDiscoveredEventChan chan *types.PeerDiscoveredEvent
	MetadataReceivedEventChan chan *types.MetadataReceivedEvent
}


func NewClickhouseClient(cfg *ClickhouseConfig) (*ClickhouseClient, error) {
	log := log.NewLogger("clickhouse")

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:        []string{cfg.Endpoint},
		DialTimeout: time.Second * 60,
		Auth: clickhouse.Auth{
			Database: cfg.DB,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Debugf: func(format string, v ...interface{}) {
			log.Debug().Str("module", "clickhouse").Msgf(format, v)
		},
	// TLS is not configured
	//	TLS: &tls.Config{
	//		InsecureSkipVerify: true,
	//	},
	})

	if err != nil {
		return nil, err
	}

	return &ClickhouseClient{
		cfg:    cfg,
		log:    log,
		chConn: conn,

		ValidatorEventChan: make(chan *types.ValidatorEvent, 16384),
		IPMetadataEventChan:     make(chan *types.IPMetadataEvent, 16384),     
		PeerDiscoveredEventChan: make(chan *types.PeerDiscoveredEvent, 16384),
		MetadataReceivedEventChan: make(chan *types.MetadataReceivedEvent, 16384),
	}, nil
}

func (c *ClickhouseClient) initializeTables() error {
    // Create validator_metadata table
    if err := c.chConn.Exec(context.Background(), ValidatorMetadataDDL(c.cfg.DB)); err != nil {
        c.log.Error().Err(err).Msg("creating validator_metadata table")
        return err
    }

    // Create ip_metadata table
    if err := c.chConn.Exec(context.Background(), IPMetadataDDL(c.cfg.DB)); err != nil {
        c.log.Error().Err(err).Msg("creating ip_metadata table")
        return err
    }


	// Create peer_discovered_events table
    if err := c.chConn.Exec(context.Background(), PeerDiscoveredEventsDDL(c.cfg.DB)); err != nil {
        c.log.Error().Err(err).Msg("creating peer_discovered_events table")
        return err
    }

	// Create metadata_received_events table
    if err := c.chConn.Exec(context.Background(), MetadataReceivedEventsDDL(c.cfg.DB)); err != nil {
        c.log.Error().Err(err).Msg("creating metadata_received_events table")
        return err
    }

    return nil
}

func (c *ClickhouseClient) Start() error {
    c.log.Info().
        Str("endpoint", c.cfg.Endpoint).
        Uint64("MaxValidatorBatchSize", c.cfg.MaxValidatorBatchSize).
        Uint64("MaxIPMetadataBatchSize", c.cfg.MaxIPMetadataBatchSize).
        Uint64("MaxPeerDiscoveredEventsBatchSize", c.cfg.MaxPeerDiscoveredEventsBatchSize).
        Uint64("MaxMetadataReceivedEventsBatchSize", c.cfg.MaxMetadataReceivedEventsBatchSize).
        Msg("Setting up ClickHouse database")

    if err := c.initializeTables(); err != nil {
        return err
    }

    go batchProcessor(c, "validator_metadata", c.ValidatorEventChan, c.cfg.MaxValidatorBatchSize)
    go batchProcessor(c, "ip_metadata", c.IPMetadataEventChan, c.cfg.MaxIPMetadataBatchSize)
    go batchProcessor(c, "peer_discovered_events", c.PeerDiscoveredEventChan, c.cfg.MaxPeerDiscoveredEventsBatchSize)
    go batchProcessor(c, "metadata_received_events", c.MetadataReceivedEventChan, c.cfg.MaxMetadataReceivedEventsBatchSize)

    return nil
}



// BatchProcessor processes events in batches for a specified table in ClickHouse.
func batchProcessor[T any](client *ClickhouseClient, tableName string, eventChan <-chan T, maxSize uint64) {
	// Prepare the initial batch.
	batch, err := client.chConn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s", tableName))
	if err != nil {
		client.log.Error().Err(err).Msg("Failed to prepare batch")
		return
	}

	var count uint64 = 0

	// Process events from the channel.
	for event := range eventChan {
		client.log.Debug().Interface("event", event).Msg("Event received")

		if err := batch.AppendStruct(event); err != nil {
			client.log.Error().Err(err).Msg("Failed to append event to batch")
			continue
		}

		count++

		// Check if the batch size has reached the maximum size.
		if count >= maxSize {
			client.log.Info().Uint64("batch_size", count).Msg("Max batch size reached, sending batch")
			if err := sendBatch(client, batch); err != nil {
				client.log.Error().Err(err).Msg("Failed to send batch, will retry")
				continue
			}

			// Prepare a new batch after sending the current batch.
			batch, err = client.chConn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s", tableName))
			if err != nil {
				client.log.Error().Err(err).Msg("Failed to prepare new batch after sending")
				return
			}
			count = 0
		}
	}

	// Handle any remaining events in the final batch when the channel closes.
	if count > 0 {
		client.log.Info().Uint64("batch_size", count).Msg("Sending final batch")
		if err := sendBatch(client, batch); err != nil {
			client.log.Error().Err(err).Msg("Failed to send final batch")
		}
	}
}

// sendBatch sends the batch to ClickHouse and logs the operation.
func sendBatch(client *ClickhouseClient, batch driver.Batch) error {
	if err := batch.Send(); err != nil {
		client.log.Error().Err(err).Msg("Failed to send batch")
		return err
	}
	client.log.Info().Msg("Batch sent successfully")
	return nil
}
