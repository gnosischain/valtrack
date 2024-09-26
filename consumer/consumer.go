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
    "sync"
    "net"
    "strconv"

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
    "github.com/ipinfo/go/v2/ipinfo"
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
    ipMetadataChan           chan *types.IPMetadataEvent

    chClient *ch.ClickhouseClient
    db       *sql.DB
    dune     *Dune

    ipCache     map[string]*types.IPMetadataEvent
    ipCacheMu   sync.RWMutex
    ipCacheTTL  time.Duration
    ipInfoToken string
}

func NewConsumer(cfg *ConsumerConfig, log zerolog.Logger, js jetstream.JetStream, chClient *ch.ClickhouseClient, db *sql.DB, dune *Dune) (*Consumer, error) {
    discoveryFilePath := fmt.Sprintf("%s/discovery_events.parquet", basePath)
    w_discovery, err := local.NewLocalFileWriter(discoveryFilePath)
    if err != nil {
        return nil, fmt.Errorf("error creating discovery events parquet file: %w", err)
    }

    metadataFilePath := fmt.Sprintf("%s/metadata_events.parquet", basePath)
    w_metadata, err := local.NewLocalFileWriter(metadataFilePath)    
    if err != nil {
        return nil, fmt.Errorf("error creating metadata events parquet file: %w", err)
    }

    validatorFilePath := fmt.Sprintf("%s/validator_metadata_events.parquet", basePath)
    w_validator, err := local.NewLocalFileWriter(validatorFilePath)
    if err != nil {
        return nil, fmt.Errorf("error creating validator parquet file: %w", err)
    }

    ipMetadataFilePath := fmt.Sprintf("%s/ip_metadata_events.parquet", basePath)
    w_ipMetadata, err := local.NewLocalFileWriter(ipMetadataFilePath)
    if err != nil {
        return nil, fmt.Errorf("error creating IP metadata events parquet file: %w", err)
    }

    discoveryWriter, err := writer.NewParquetWriter(w_discovery, new(types.PeerDiscoveredEvent), 4)
    if err != nil {
        return nil, fmt.Errorf("error creating Peer discovered Parquet writer: %w", err)
    }

    metadataWriter, err := writer.NewParquetWriter(w_metadata, new(types.MetadataReceivedEvent), 4)
    if err != nil {
        return nil, fmt.Errorf("error creating Metadata Parquet writer: %w", err)
    }

    validatorWriter, err := writer.NewParquetWriter(w_validator, new(types.ValidatorEvent), 4)
    if err != nil {
        return nil, fmt.Errorf("error creating Validator Parquet writer: %w", err)
    }

    ipMetadataWriter, err := writer.NewParquetWriter(w_ipMetadata, new(types.IPMetadataEvent), 4)
    if err != nil {
        return nil, fmt.Errorf("error creating IP Metadata Parquet writer: %w", err)
    }

    return &Consumer{
        log:              log,
        discoveryWriter:  discoveryWriter,
        metadataWriter:   metadataWriter,
        validatorWriter:  validatorWriter,
        ipMetadataWriter: ipMetadataWriter,
        js:               js,

        peerDiscoveredChan:       make(chan *types.PeerDiscoveredEvent, 16384),
        metadataReceivedChan:     make(chan *types.MetadataReceivedEvent, 16384),
        validatorMetadataChan:    make(chan *types.MetadataReceivedEvent, 16384),
        ipMetadataChan:           make(chan *types.IPMetadataEvent, 16384),

        chClient: chClient,
        db:       db,
        dune:     dune,

        ipCache:    make(map[string]*types.IPMetadataEvent),
        ipCacheTTL: 1 * time.Hour,
        ipInfoToken: os.Getenv("IPINFO_TOKEN"),
    }, nil
}

func RunConsumer(cfg *ConsumerConfig) {
    log := log.NewLogger("consumer")

    dbPath := fmt.Sprintf("%s/validator_tracker.sqlite", basePath)
    db, err := sql.Open("sqlite3", dbPath)
    if err != nil {
        log.Error().Err(err).Msg("Error opening database")
        return
    }
    defer db.Close()

    err = setupDatabase(db)
    if err != nil {
        log.Error().Err(err).Msg("Error setting up database")
        return
    }

    log.Info().Msg("Sqlite DB setup complete")

    nc, err := nats.Connect(cfg.NatsURL)
    if err != nil {
        log.Error().Err(err).Msg("Error connecting to NATS")
        return
    }
    defer nc.Close()

    js, err := jetstream.New(nc)
    if err != nil {
        log.Error().Err(err).Msg("Error creating JetStream context")
        return
    }

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
            return
        }
        defer chClient.Close() // Add this line to ensure the client is closed

        err = chClient.Start()
        if err != nil {
            log.Error().Err(err).Msg("Error starting Clickhouse client")
            return
        }
    }

    var dune *Dune
    if cfg.DuneApiKey != "" {
        dune = NewDune(cfg.DuneNamespace, cfg.DuneApiKey)
    }

    consumer, err := NewConsumer(cfg, log, js, chClient, db, dune)
    if err != nil {
        log.Error().Err(err).Msg("Error creating consumer")
        return
    }

    go func() {
        if err := consumer.Start(cfg.Name); err != nil {
            log.Error().Err(err).Msg("Error in consumer")
        }
    }()

    ipInfoToken := os.Getenv("IPINFO_TOKEN")
    if ipInfoToken == "" {
        log.Error().Msg("IPINFO_TOKEN environment variable is required")
        return
    }

    go func() {
        if err := consumer.runValidatorMetadataEventHandler(ipInfoToken); err != nil {
            log.Error().Err(err).Msg("Error in validator metadata handler")
        }
    }()
    go consumer.processIPMetadataEvents()

    server := &http.Server{Addr: ":8080", Handler: nil}
    http.HandleFunc("/validators", createGetValidatorsHandler(db))

    go func() {
        if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            log.Error().Err(err).Msg("Error starting HTTP server")
        }
    }()

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

    // Shutdown process
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    select {
    case <-quit:
        log.Info().Msg("Shutdown signal received")
    case <-ctx.Done():
        log.Info().Msg("Shutdown timeout")
    }

    close(consumer.ipMetadataChan)
    log.Info().Msg("Shutting down consumer")

    if err := server.Shutdown(ctx); err != nil {
        log.Error().Err(err).Msg("Error shutting down HTTP server")
    }

    // If you have a Clickhouse client, close it here
    if chClient != nil {
        if err := chClient.Close(); err != nil {
            log.Error().Err(err).Msg("Error closing Clickhouse client")
        }
    }

    // Any other cleanup can go here

    log.Info().Msg("Consumer shutdown complete")
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

            for msg := range batch.Messages() {
                c.handleMessage(msg)
                c.log.Debug().Str("msg_subject", msg.Subject()).Msg("Message handled")
            }
        }
    }()

    return nil
}

func (c *Consumer) handleMessage(msg jetstream.Msg) {
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
        
        // Fetch IP metadata synchronously
        ipMetadata, err := c.getIPMetadata(event.IP)
        if err != nil {
            c.log.Error().Err(err).Str("ip", event.IP).Msg("Failed to fetch IP metadata")
        } else {
            // Insert IP metadata into ClickHouse
            if err := c.ensureIPMetadataInClickHouse(ipMetadata); err != nil {
                c.log.Error().Err(err).Str("ip", event.IP).Msg("Failed to ensure IP metadata in ClickHouse")
            }
        }

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

func (c *Consumer) getIPMetadata(ip string) (*types.IPMetadataEvent, error) {
    // Check cache first
    c.ipCacheMu.RLock()
    if metadata, found := c.ipCache[ip]; found {
        c.ipCacheMu.RUnlock()
        return metadata, nil
    }
    c.ipCacheMu.RUnlock()

    // Check ClickHouse
    metadata, err := c.getIPMetadataFromClickHouse(ip)
    if err == nil {
        // Found in ClickHouse, cache and return
        c.cacheIPMetadata(ip, metadata)
        return metadata, nil
    }

    // Not found in ClickHouse, fetch from IPInfo API
    ipInfo, err := c.fetchIPInfoFromAPI(ip)
    if err != nil {
        return nil, fmt.Errorf("failed to fetch IP info: %w", err)
    }

    metadata = convertIPInfoToMetadata(ipInfo)
    c.cacheIPMetadata(ip, metadata)

    return metadata, nil
}

func (c *Consumer) getIPMetadataFromClickHouse(ip string) (*types.IPMetadataEvent, error) {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    var metadata types.IPMetadataEvent
    query := fmt.Sprintf("SELECT ip, hostname, city, region, country, latitude, longitude, postal_code, asn, asn_organization, asn_type FROM ip_metadata WHERE ip = '%s'", ip)
    
    err := c.chClient.QueryRow(ctx, query).Scan(
        &metadata.IP, &metadata.Hostname, &metadata.City, &metadata.Region,
        &metadata.Country, &metadata.Latitude, &metadata.Longitude,
        &metadata.PostalCode, &metadata.ASN, &metadata.ASNOrganization, &metadata.ASNType,
    )
    
    if err != nil {
        if err == sql.ErrNoRows {
            return nil, nil // No metadata found for this IP
        }
        return nil, fmt.Errorf("error querying ClickHouse: %w", err)
    }

    return &metadata, nil
}

func (c *Consumer) fetchIPInfoFromAPI(ip string) (*ipinfo.Info, error) {
    client := ipinfo.NewClient(nil, nil, c.ipInfoToken)
    info, err := client.GetIPInfo(net.ParseIP(ip))
    if err != nil {
        return nil, fmt.Errorf("IPInfo API error: %w", err)
    }
    return info, nil
}

func convertIPInfoToMetadata(info *ipinfo.Info) *types.IPMetadataEvent {
    lat, _ := strconv.ParseFloat(info.Latitude, 64)
    lon, _ := strconv.ParseFloat(info.Longitude, 64)
    
    return &types.IPMetadataEvent{
        IP:              info.IP.String(),
        Hostname:        info.Hostname,
        City:            info.City,
        Region:          info.Region,
        Country:         info.CountryName,
        Latitude:        lat,
        Longitude:       lon,
        PostalCode:      info.Postal,
        ASN:             info.ASN,
        ASNOrganization: info.Org,
        ASNType:         "", // IPInfo doesn't provide ASN type directly
    }
}
func (c *Consumer) cacheIPMetadata(ip string, metadata *types.IPMetadataEvent) {
    c.ipCacheMu.Lock()
    defer c.ipCacheMu.Unlock()
    c.ipCache[ip] = metadata
    go func() {
        time.Sleep(c.ipCacheTTL)
        c.ipCacheMu.Lock()
        delete(c.ipCache, ip)
        c.ipCacheMu.Unlock()
    }()
}

func (c *Consumer) ensureIPMetadataInClickHouse(metadata *types.IPMetadataEvent) error {
    select {
    case c.chClient.IPMetadataEventChan <- metadata:
        return nil
    default:
        return fmt.Errorf("ClickHouse channel is full or unavailable")
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
        ENR:        event.ENR,
        ID:         event.ID,
        IP:         ip,
        Port:       0, // Adjust accordingly
        CrawlerID:  event.CrawlerID,
        CrawlerLoc: event.CrawlerLoc,
        Timestamp:  event.Timestamp,
    }
}

func (c *Consumer) storeDiscoveryEvent(event types.PeerDiscoveredEvent) {
    if event.ID == "" || event.ENR == "" {
        c.log.Warn().Interface("event", event).Msg("Received incomplete discovery event")
        return
    }

    if err := c.discoveryWriter.Write(event); err != nil {
        c.log.Err(err).Msg("Failed to write discovery event to Parquet file")
    } else {
        c.log.Trace().Msg("Wrote discovery event to Parquet file")
    }

    if c.chClient != nil && c.chClient.PeerDiscoveredEventChan != nil {
        c.chClient.PeerDiscoveredEventChan <- &event
        c.log.Info().Str("ID", event.ID).Msg("Inserted peer discovered event into ClickHouse channel")
    } else {
        c.log.Warn().Msg("ClickHouse client is nil or channel is closed; cannot send event")
    }
}

func (c *Consumer) storeMetadataEvent(event types.MetadataReceivedEvent) {
    if err := c.metadataWriter.Write(event); err != nil {
        c.log.Err(err).Msg("Failed to write metadata event to Parquet file")
    } else {
        c.log.Trace().Msg("Wrote metadata event to Parquet file")
    }

    if c.chClient != nil {
        c.chClient.MetadataReceivedEventChan <- &event
        c.log.Info().Str("ID", event.ID).Msg("Inserted metadata received event into ClickHouse channel")
    }
}

func (c *Consumer) processIPMetadataEvents() {
    for ipEvent := range c.ipMetadataChan {
        c.log.Info().Msgf("Received IP metadata event for processing: %s", ipEvent.IP)
        if err := c.ipMetadataWriter.Write(ipEvent); err != nil {
            c.log.Err(err).Msg("Failed to write IP metadata event to Parquet file")
            continue
        }
        c.log.Trace().Msg("Wrote IP metadata event to Parquet file")

        if c.chClient != nil {
            if err := c.sendIPMetadataToClickHouse(ipEvent); err != nil {
                c.log.Error().Err(err).Str("IP", ipEvent.IP).Msg("Failed to send IP metadata event to ClickHouse")
                continue
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
