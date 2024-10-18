package clickhouse

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/chainbound/valtrack/log"
	"github.com/chainbound/valtrack/types"
	"github.com/rs/zerolog"

    "encoding/csv"
	"encoding/json"
    "os"
	"io"
    "strconv"
    "strings"
	"regexp"
)

func (c *ClickhouseClient) LoadIPMetadataFromCSV() error {
	// Check if the table is empty
    isEmpty, err := c.isTableEmpty("ip_metadata")
    if err != nil {
        return fmt.Errorf("failed to check if table is empty: %w", err)
    }

    if !isEmpty {
        c.log.Info().Msg("ip_metadata table is not empty, skipping CSV load")
        return nil
    }

    csvPath := "/app/data/ip_metadata.csv"
    
    file, err := os.Open(csvPath)
    if err != nil {
        c.log.Error().Err(err).Str("path", csvPath).Msg("Failed to open IP metadata CSV file")
        return err
    }
    defer file.Close()

    c.log.Info().Str("path", csvPath).Msg("Successfully opened IP metadata CSV file")

    reader := csv.NewReader(file)
    reader.FieldsPerRecord = -1 // Allow variable number of fields

    batch, err := c.ChConn.PrepareBatch(context.Background(), "INSERT INTO ip_metadata")
    if err != nil {
        return fmt.Errorf("failed to prepare batch: %w", err)
    }

    for {
        record, err := reader.Read()
        if err == io.EOF {
            break
        }
        if err != nil {
            return fmt.Errorf("error reading CSV record: %w", err)
        }

        // Ensure we have at least the minimum required fields
        if len(record) < 9 {
            c.log.Warn().Str("row", strings.Join(record, ",")).Msg("Skipping row with insufficient columns")
            continue
        }

        // Parse latitude and longitude
        latLon := strings.Split(record[5], ",")
        lat, err := parseFloat(latLon[0])
        if err != nil {
            c.log.Warn().Str("latitude", latLon[0]).Err(err).Msg("Invalid latitude, using 0")
            lat = 0
        }
        lon, err := parseFloat(latLon[1])
        if err != nil {
            c.log.Warn().Str("longitude", latLon[1]).Err(err).Msg("Invalid longitude, using 0")
            lon = 0
        }

        // Parse ASN info
        var asnInfo struct {
            ASN    string `json:"asn"`
                Name   string `json:"name"`
                Type   string `json:"type"`
                Domain string `json:"domain"`
                Route  string `json:"route"`
        }
		// Preprocess the JSON string
        asnJSONString := record[8]

        // Handle "nan" case
        if strings.ToLower(asnJSONString) == "nan" {
            c.log.Warn().Str("original_asn_json", asnJSONString).Msg("ASN JSON is 'nan', using default values")
            asnInfo = struct {
                ASN    string `json:"asn"`
                Name   string `json:"name"`
                Type   string `json:"type"`
                Domain string `json:"domain"`
                Route  string `json:"route"`
            }{ASN: "", Name: "", Type: "", Domain: "", Route: ""}
            continue // Skip to the next record
        }

        // Replace single quotes with double quotes
        asnJSONString = strings.ReplaceAll(asnJSONString, "'", "\"")
        
        // Use regex to find the "name" field and properly escape its value
        re := regexp.MustCompile(`"name":\s*"((?:[^"\\]|\\.)*)"`)
        asnJSONString = re.ReplaceAllStringFunc(asnJSONString, func(match string) string {
            parts := re.FindStringSubmatch(match)
            if len(parts) < 2 {
                return match
            }
            // Escape any double quotes within the value
            escapedValue := strings.ReplaceAll(parts[1], "\"", "\\\"")
            return fmt.Sprintf(`"name": "%s"`, escapedValue)
        })

        // Ensure the JSON string is complete
        if !strings.HasSuffix(asnJSONString, "}") {
            asnJSONString += "}"
        }

        c.log.Debug().Str("processed_asn_json", asnJSONString).Msg("Processed ASN JSON before parsing")

        if err := json.Unmarshal([]byte(asnJSONString), &asnInfo); err != nil {
            c.log.Warn().Str("original_asn_json", record[8]).Str("processed_asn_json", asnJSONString).Err(err).Msg("Failed to parse ASN JSON, using default values")
            asnInfo = struct {
                ASN    string `json:"asn"`
                Name   string `json:"name"`
                Type   string `json:"type"`
                Domain string `json:"domain"`
                Route  string `json:"route"`
            }{ASN: "", Name: "", Type: "", Domain: "", Route: ""}
        }

        err = batch.Append(
            record[0],                        // IP
            record[1],                        // Hostname
            record[2],                        // City
            record[3],                        // Region
            record[4],                        // Country
            lat,                              // Latitude
            lon,                              // Longitude
            record[7],                        // Postal Code
            strings.TrimPrefix(asnInfo.ASN, "AS"), // ASN
            asnInfo.Name,                     // ASN Organization
            asnInfo.Type,                     // ASN Type
        )
        if err != nil {
            return fmt.Errorf("failed to append to batch: %w", err)
        }
    }

    if err := batch.Send(); err != nil {
        return fmt.Errorf("failed to send batch: %w", err)
    }
    

    c.log.Info().Msg("Successfully loaded IP metadata from CSV")
    return nil
}

// Helper function to parse float values, treating "nan" as 0
func parseFloat(s string) (float64, error) {
    s = strings.TrimSpace(s)
    if s == "" || strings.ToLower(s) == "nan" {
        return 0, nil
    }
    return strconv.ParseFloat(s, 64)
}

func (c *ClickhouseClient) isTableEmpty(tableName string) (bool, error) {
    query := fmt.Sprintf("SELECT count(*) FROM %s", tableName)
    var count uint64 
    err := c.ChConn.QueryRow(context.Background(), query).Scan(&count)
    if err != nil {
        return false, err
    }
    return count == 0, nil
}

func ValidatorMetadataDDL(db string) string {
	return fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s.validator_metadata (
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
	) ENGINE = ReplacingMergeTree()
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
		) ENGINE = ReplacingMergeTree()
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
		) ENGINE = ReplacingMergeTree()
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
		) ENGINE = ReplacingMergeTree()
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
    cfg    *ClickhouseConfig
    log    zerolog.Logger
    ChConn clickhouse.Conn

    ValidatorEventChan        chan *types.ValidatorEvent
    IPMetadataEventChan       chan *types.IPMetadataEvent
    PeerDiscoveredEventChan   chan *types.PeerDiscoveredEvent
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
        Protocol: clickhouse.Native,
        TLS:      &tls.Config{},
    })

    if err != nil {
        return nil, err
    }

    return &ClickhouseClient{
        cfg:    cfg,
        log:    log,
        ChConn: conn,

        ValidatorEventChan:        make(chan *types.ValidatorEvent, 16384),
        IPMetadataEventChan:       make(chan *types.IPMetadataEvent, 16384),
        PeerDiscoveredEventChan:   make(chan *types.PeerDiscoveredEvent, 16384),
        MetadataReceivedEventChan: make(chan *types.MetadataReceivedEvent, 16384),
    }, nil
}


func (c *ClickhouseClient) Close() error {
    return c.ChConn.Close()
}

func (c *ClickhouseClient) initializeTables() error {
    // Create validator_metadata table
    if err := c.ChConn.Exec(context.Background(), ValidatorMetadataDDL(c.cfg.DB)); err != nil {
        c.log.Error().Err(err).Msg("creating validator_metadata table")
        return err
    }

    // Create ip_metadata table
    if err := c.ChConn.Exec(context.Background(), IPMetadataDDL(c.cfg.DB)); err != nil {
        c.log.Error().Err(err).Msg("creating ip_metadata table")
        return err
    }


	// Create peer_discovered_events table
    if err := c.ChConn.Exec(context.Background(), PeerDiscoveredEventsDDL(c.cfg.DB)); err != nil {
        c.log.Error().Err(err).Msg("creating peer_discovered_events table")
        return err
    }

	// Create metadata_received_events table
    if err := c.ChConn.Exec(context.Background(), MetadataReceivedEventsDDL(c.cfg.DB)); err != nil {
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
	batch, err := client.ChConn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s", tableName))
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
			batch, err = client.ChConn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s", tableName))
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