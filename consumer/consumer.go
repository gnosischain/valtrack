package consumer

import (
    "context"
    "database/sql"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "net/http"
    "os"
    "os/signal"
    "syscall"
    "time"
	//"sync"
    ma "github.com/multiformats/go-multiaddr"

    ch "github.com/chainbound/valtrack/clickhouse"
    "github.com/chainbound/valtrack/log"
    "github.com/chainbound/valtrack/types"
    _ "github.com/mattn/go-sqlite3"
    "github.com/nats-io/nats.go"
    "github.com/nats-io/nats.go/jetstream"
    "github.com/rs/zerolog"
    "github.com/xitongsys/parquet-go-source/local"
    "github.com/xitongsys/parquet-go/writer"
)

const basePath = "/data"
const BATCH_SIZE = 1024

type ConsumerConfig struct {
    LogLevel      string
    NatsURL       string
    Name          string
    ChCfg         ch.ClickhouseConfig
    DuneNamespace string
    DuneApiKey    string
}

type Consumer struct {
    log              zerolog.Logger
    discoveryWriter  *writer.ParquetWriter
    metadataWriter   *writer.ParquetWriter
    validatorWriter  *writer.ParquetWriter
    ipMetadataWriter *writer.ParquetWriter
    js               jetstream.JetStream

    peerDiscoveredChan       chan *types.PeerDiscoveredEvent
    metadataReceivedChan     chan *types.MetadataReceivedEvent
    validatorMetadataChan    chan *types.MetadataReceivedEvent
	ipMetadataChan 			 chan *types.IPMetadataEvent

    chClient *ch.ClickhouseClient
    db       *sql.DB
    dune     *Dune
}

func RunConsumer(cfg *ConsumerConfig) {
    log := log.NewLogger("consumer")

    dbPath := fmt.Sprintf("%s/validator_tracker.sqlite", basePath)
    db, err := sql.Open("sqlite3", dbPath)
    if err != nil {
        log.Error().Err(err).Msg("Error opening database")
    }
    defer db.Close()

    err = setupDatabase(db)
    if err != nil {
        log.Error().Err(err).Msg("Error setting up database")
    }

  //  err = loadIPMetadataFromCSV(db, "ip_metadata.csv")
//	if err != nil {
//		log.Error().Err(err).Msg("Error setting up database")
//	}

    log.Info().Msg("Sqlite DB setup complete")

    nc, err := nats.Connect(cfg.NatsURL)
    if err != nil {
        log.Error().Err(err).Msg("Error connecting to NATS")
    }
    defer nc.Close()

    js, err := jetstream.New(nc)
    if err != nil {
        log.Error().Err(err).Msg("Error creating JetStream context")
    }

    discoveryFilePath := fmt.Sprintf("%s/discovery_events.parquet", basePath)
    w_discovery, err := local.NewLocalFileWriter(discoveryFilePath)
    if err != nil {
        log.Error().Err(err).Msg("Error creating discovery events parquet file")
    }
    defer w_discovery.Close()

    metadataFilePath := fmt.Sprintf("%s/metadata_events.parquet", basePath)
    w_metadata, err := local.NewLocalFileWriter(metadataFilePath)    
    if err != nil {
        log.Error().Err(err).Msg("Error creating metadata events parquet file")
    }
    defer w_metadata.Close()

    validatorFilePath := fmt.Sprintf("%s/validator_metadata_events.parquet", basePath)
    w_validator, err := local.NewLocalFileWriter(validatorFilePath)
    if err != nil {
        log.Error().Err(err).Msg("Error creating validator parquet file")
    }
    defer w_validator.Close()

    ipMetadataFilePath := fmt.Sprintf("%s/ip_metadata_events.parquet", basePath)
    w_ipMetadata, err := local.NewLocalFileWriter(ipMetadataFilePath)
    if err != nil {
        log.Error().Err(err).Msg("Error creating IP metadata events parquet file")
    }
    defer w_ipMetadata.Close()

    discoveryWriter, err := writer.NewParquetWriter(w_discovery, new(types.PeerDiscoveredEvent), 4)
    if err != nil {
        log.Error().Err(err).Msg("Error creating Peer discovered Parquet writer")
    }
    defer discoveryWriter.WriteStop()

    metadataWriter, err := writer.NewParquetWriter(w_metadata, new(types.MetadataReceivedEvent), 4)
    if err != nil {
        log.Error().Err(err).Msg("Error creating Metadata Parquet writer")
    }
    defer metadataWriter.WriteStop()

    validatorWriter, err := writer.NewParquetWriter(w_validator, new(types.ValidatorEvent), 4)
    if err != nil {
        log.Error().Err(err).Msg("Error creating Validator Parquet writer")
    }
    defer validatorWriter.WriteStop()

    ipMetadataWriter, err := writer.NewParquetWriter(w_ipMetadata, new(types.IPMetadataEvent), 4)
    if err != nil {
        log.Error().Err(err).Msg("Error creating IP Metadata Parquet writer")
    }
    defer ipMetadataWriter.WriteStop()

    chCfg := ch.ClickhouseConfig{
        Endpoint: cfg.ChCfg.Endpoint,
        DB:       cfg.ChCfg.DB,
        Username: cfg.ChCfg.Username,
        Password: cfg.ChCfg.Password,
        MaxValidatorBatchSize: cfg.ChCfg.MaxValidatorBatchSize,
        MaxIPMetadataBatchSize: cfg.ChCfg.MaxIPMetadataBatchSize,
	    MaxPeerDiscoveredEventsBatchSize: cfg.ChCfg.MaxPeerDiscoveredEventsBatchSize,
	    MaxMetadataReceivedEventsBatchSize: cfg.ChCfg.MaxMetadataReceivedEventsBatchSize,
    }

    var chClient *ch.ClickhouseClient
    if chCfg.Endpoint != "" {
        chClient, err = ch.NewClickhouseClient(&chCfg)
        if err != nil {
            log.Error().Err(err).Msg("Error creating Clickhouse client")
        }

        err = chClient.Start()
        if err != nil {
            log.Error().Err(err).Msg("Error starting Clickhouse client")
        }
    }

    var dune *Dune
    if cfg.DuneApiKey != "" {
        dune = NewDune(cfg.DuneNamespace, cfg.DuneApiKey)
    }

    consumer := Consumer{
        log:             log,
        discoveryWriter: discoveryWriter,
        metadataWriter:  metadataWriter,
        validatorWriter: validatorWriter,
        ipMetadataWriter: ipMetadataWriter,
        js:              js,

        peerDiscoveredChan:       make(chan *types.PeerDiscoveredEvent, 16384),
        metadataReceivedChan:     make(chan *types.MetadataReceivedEvent, 16384),
        validatorMetadataChan:    make(chan *types.MetadataReceivedEvent, 16384),
		ipMetadataChan: 		  make(chan *types.IPMetadataEvent, 16384),

        chClient: chClient,
        db:       db,
        dune:     dune,
    }

    go func() {
        if err := consumer.Start(cfg.Name); err != nil {
            log.Error().Err(err).Msg("Error in consumer")
        }
    }()

    ipInfoToken := os.Getenv("IPINFO_TOKEN")
	if ipInfoToken == "" {
		log.Error().Msg("IPINFO_TOKEN environment variable is required")
	}

	go consumer.runValidatorMetadataEventHandler(ipInfoToken)

	go consumer.processIPMetadataEvents()

   

    server := &http.Server{Addr: ":8080", Handler: nil}
    http.HandleFunc("/validators", createGetValidatorsHandler(db))

    go func() {
        if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            log.Error().Err(err).Msg("Error starting HTTP server")
        }
    }()
    defer server.Shutdown(context.Background())

    if dune != nil {
        log.Info().Msg("Starting to publish to Dune")
        go func() {
            if err := consumer.publishToDune(); err != nil {
                log.Error().Err(err).Msg("Error publishing to Dune")
            }

            ticker := time.NewTicker(24 * time.Hour)
            defer ticker.Stop()

            for range ticker.C {
                if err := consumer.publishToDune(); err != nil {
                    log.Error().Err(err).Msg("Error publishing to Dune")
                }
                log.Info().Msg("Published to Dune")
            }
        }()
    }

    quit := make(chan os.Signal, 1)
    signal.Notify(quit, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
    
    <-quit
    close(consumer.ipMetadataChan)

	//wg := &sync.WaitGroup{}
    //wg.Add(1)
   // go func() {
   //     defer wg.Done()
   //     consumer.processIPMetadataEvents()
   // }()

   // <-quit
   // close(consumer.ipMetadataChan)  // Signal goroutine to finish
   // wg.Wait()  // Wait for the goroutine to clean up
}

func (c *Consumer) Start(name string) error {
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    consumerCfg := jetstream.ConsumerConfig{
        Name:        name,
        Durable:     name,
        Description: "Consumes valtrack events",
        AckPolicy:   jetstream.AckExplicitPolicy,
    }

    stream, err := c.js.Stream(ctx, "EVENTS")
    if err != nil {
        c.log.Error().Err(err).Msg("Error opening valtrack jetstream")
        return err
    }

    consumer, err := stream.CreateOrUpdateConsumer(ctx, consumerCfg)
    if err != nil {
        c.log.Error().Err(err).Msg("Error creating consumer")
        return err
    }

    // Load IP metadata from CSV
    if err := c.chClient.LoadIPMetadataFromCSV(); err != nil {
        c.log.Error().Err(err).Msg("Failed to load IP metadata from CSV")
    }

    go func() {
        for {
            batch, err := consumer.FetchNoWait(BATCH_SIZE)
            if err != nil {
                c.log.Error().Err(err).Msg("Error fetching batch of messages")
                return
            }
            if err = batch.Error(); err != nil {
                c.log.Error().Err(err).Msg("Error in messages batch")
                return
            }

            //c.log.Debug().Int("batch_size", len(batch.Messages())).Msg("Processing batch of messages")

            for msg := range batch.Messages() {
                handleMessage(c, msg)
                c.log.Debug().Str("msg_subject", msg.Subject()).Msg("Message handled")
            }
        }
    }()

    return nil
}

func handleMessage(c *Consumer, msg jetstream.Msg) {
    md, _ := msg.Metadata()
    progress := float64(md.Sequence.Stream) / (float64(md.NumPending) + float64(md.Sequence.Stream)) * 100

    switch msg.Subject() {
    case "events.ip_metadata":
        var ipEvent types.IPMetadataEvent
        if err := json.Unmarshal(msg.Data(), &ipEvent); err != nil {
            c.log.Err(err).Msg("Error unmarshaling IPMetadataEvent")
            msg.Term()
            return
        }
        c.log.Info().Str("IP", ipEvent.IP).Msg("IP metadata received")
        c.ipMetadataChan <- &ipEvent

        // Send to ClickHouse if client is initialized
        if c.chClient != nil {
            c.chClient.IPMetadataEventChan <- &ipEvent
        }

    case "events.peer_discovered":
        var event types.PeerDiscoveredEvent
        if err := json.Unmarshal(msg.Data(), &event); err != nil {
            c.log.Err(err).Msg("Error unmarshaling PeerDiscoveredEvent")
            msg.Term()
            return
        }

        c.log.Info().Time("timestamp", md.Timestamp).Uint64("pending", md.NumPending).Str("progress", fmt.Sprintf("%.2f%%", progress)).Msg("peer_discovered")
        c.storeDiscoveryEvent(event)

        // Send to ClickHouse if client is initialized
        if c.chClient != nil {
            c.chClient.PeerDiscoveredEventChan <- &event
        }

    case "events.metadata_received":
        var event types.MetadataReceivedEvent
        if err := json.Unmarshal(msg.Data(), &event); err != nil {
            c.log.Err(err).Msg("Error unmarshaling MetadataReceivedEvent")
            msg.Term()
            return
        }

        c.log.Info().Time("timestamp", md.Timestamp).Uint64("pending", md.NumPending).Str("progress", fmt.Sprintf("%.2f%%", progress)).Msg("metadata_received")
        c.handleMetadataEvent(event)
        c.storeMetadataEvent(event)

        // Send to ClickHouse if client is initialized
        if c.chClient != nil {
            c.chClient.MetadataReceivedEventChan <- &event
        }

    default:
        c.log.Warn().Str("subject", msg.Subject()).Msg("Unknown event type")
    }

    if err := msg.Ack(); err != nil {
        c.log.Err(err).Msg("Error acknowledging message")
    }
}

func (c *Consumer) handleMetadataEvent(event types.MetadataReceivedEvent) {
    longLived := indexesFromBitfield(event.MetaData.Attnets)

    c.log.Info().Str("peer", event.ID).Any("long_lived_subnets", longLived).Any("subscribed_subnets", event.SubscribedSubnets).Msg("Checking for validator")

    if len(extractShortLivedSubnets(event.SubscribedSubnets, longLived)) == 0 || len(longLived) != 2 {
        return
    }

    c.validatorMetadataChan <- &event

    validatorEvent := types.ValidatorEvent{
        ENR:               event.ENR,
        ID:                event.ID,
        Multiaddr:         event.Multiaddr,
        Epoch:             event.Epoch,
        SeqNumber:         event.MetaData.SeqNumber,
        Attnets:           hex.EncodeToString(event.MetaData.Attnets),
        Syncnets:          hex.EncodeToString(event.MetaData.Syncnets),
        ClientVersion:     event.ClientVersion,
        CrawlerID:         event.CrawlerID,
        CrawlerLoc:        event.CrawlerLoc,
        Timestamp:         event.Timestamp,
        LongLivedSubnets:  longLived,
        SubscribedSubnets: event.SubscribedSubnets,
    }

    if c.chClient != nil {
        c.chClient.ValidatorEventChan <- &validatorEvent
        c.log.Info().Any("validator_event", validatorEvent).Msg("Inserted validator event")
    }

    if err := c.validatorWriter.Write(validatorEvent); err != nil {
        c.log.Err(err).Msg("Failed to write validator event to Parquet file")
    } else {
        c.log.Trace().Msg("Wrote validator event to Parquet file")
    }

    maAddr, err := ma.NewMultiaddr(event.Multiaddr)
    if err != nil {
        c.log.Error().Err(err).Msg("Invalid multiaddr")
        return
    }
    ip, err := maAddr.ValueForProtocol(ma.P_IP4)
    if err != nil {
        c.log.Error().Err(err).Msg("Invalid IP in multiaddr")
        return
    }

    c.peerDiscoveredChan <- &types.PeerDiscoveredEvent{
        ENR: event.ENR,
        ID: event.ID,
        IP: ip,
        Port: 0, // Adjust accordingly
        CrawlerID: event.CrawlerID,
        CrawlerLoc: event.CrawlerLoc,
        Timestamp: event.Timestamp,
    }
}

func (c *Consumer) storeDiscoveryEvent(event types.PeerDiscoveredEvent) {
    // Check if the event is properly populated
    if event.ID == "" || event.ENR == "" {
        c.log.Warn().Interface("event", event).Msg("Received incomplete discovery event")
        return
    }

    // Attempt to write to the Parquet file
    if err := c.discoveryWriter.Write(event); err != nil {
        c.log.Err(err).Msg("Failed to write discovery event to Parquet file")
    } else {
        c.log.Trace().Msg("Wrote discovery event to Parquet file")
    }

    // Send event to ClickHouse if the client is initialized
    if c.chClient != nil {
        // Ensure the channel is not closed
        if c.chClient.PeerDiscoveredEventChan != nil {
            c.chClient.PeerDiscoveredEventChan <- &event
            c.log.Info().Str("ID", event.ID).Msg("Inserted peer discovered event into ClickHouse channel")
        } else {
            c.log.Error().Msg("Attempted to send event to a nil or closed channel")
        }
    } else {
        c.log.Warn().Msg("ClickHouse client is nil; cannot send event")
    }
}


func (c *Consumer) storeMetadataEvent(event types.MetadataReceivedEvent) {
    if err := c.metadataWriter.Write(event); err != nil {
        c.log.Err(err).Msg("Failed to write metadata event to Parquet file")
    } else {
        c.log.Trace().Msg("Wrote metadata event to Parquet file")
    }

	// Send event to ClickHouse
    if c.chClient != nil {
        c.chClient.MetadataReceivedEventChan <- &event
        c.log.Info().Str("ID", event.ID).Msg("Inserted metadata received event into ClickHouse channel")
    }
}

func (c *Consumer) processIPMetadataEvents() {
    for ipEvent := range c.ipMetadataChan {
        c.log.Info().Msgf("Received IP metadata event for processing: %s", ipEvent.IP)
        // Write to Parquet file before sending to ClickHouse
        if err := c.ipMetadataWriter.Write(ipEvent); err != nil {
            c.log.Err(err).Msg("Failed to write IP metadata event to Parquet file")
            continue // Proceed to the next event in case of failure
        }
        c.log.Trace().Msg("Wrote IP metadata event to Parquet file")

        // Send event to ClickHouse
        if c.chClient != nil {
            if err := c.sendIPMetadataToClickHouse(ipEvent); err != nil {
                c.log.Error().Err(err).Str("IP", ipEvent.IP).Msg("Failed to send IP metadata event to ClickHouse")
                // Handle or retry as necessary
                continue // You might choose to retry or simply log and move on
            }
            c.log.Info().Str("IP", ipEvent.IP).Msg("IP metadata event sent to ClickHouse successfully")
        }
    }
}

func (c *Consumer) sendIPMetadataToClickHouse(ipEvent *types.IPMetadataEvent) error {
    select {
    case c.chClient.IPMetadataEventChan <- ipEvent:
        return nil
    default:
        return fmt.Errorf("ClickHouse channel is full or unavailable")
    }
}
