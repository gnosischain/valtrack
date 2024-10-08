package consumer

import (
    "database/sql"
    "encoding/csv"
    "encoding/json"
    "fmt"
    "net"
    "os"
    "strconv"
    "strings"
    "time"

    "github.com/ipinfo/go/v2/ipinfo"
    ma "github.com/multiformats/go-multiaddr"
    "github.com/pkg/errors"
    "github.com/chainbound/valtrack/types"
)

type asnJSON struct {
	Asn             string `json:"asn"`
	AsnOrganization string `json:"name"`
	Type            string `json:"type"`
}

var (
    createTrackerTableQuery = `
    CREATE TABLE IF NOT EXISTS validator_tracker (
        peer_id TEXT PRIMARY KEY,
        enr TEXT,
        multiaddr TEXT,
        ip TEXT,
        port INTEGER,
        last_seen INTEGER,
        last_epoch INTEGER,
        client_version TEXT,
        total_observations INTEGER
    );
    `

    createIpMetadataTableQuery = `
    CREATE TABLE IF NOT EXISTS ip_metadata (
        ip TEXT PRIMARY KEY,
        hostname TEXT,
        city TEXT,
        region TEXT,
        country TEXT,
        latitude REAL,
        longitude REAL,
        postal_code TEXT,
        asn TEXT,  
        asn_organization TEXT,
        asn_type TEXT
    );
    `

    createValidatorCountsTableQuery = `
    CREATE TABLE IF NOT EXISTS validator_counts (
        peer_id TEXT,
        validator_count INTEGER,
        n_observations INTEGER DEFAULT 1,
        PRIMARY KEY (peer_id, validator_count)
    );
    `

    insertTrackerQuery = `
    INSERT INTO validator_tracker (peer_id, enr, multiaddr, ip, port, last_seen, last_epoch, client_version, total_observations)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (peer_id) DO UPDATE SET total_observations = total_observations + 1;
    `

    updateTrackerQuery = `
    UPDATE validator_tracker
    SET enr = ?, multiaddr = ?, ip = ?, port = ?, last_seen = ?, last_epoch = ?, client_version = ?, total_observations = total_observations + 1
    WHERE peer_id = ?;
    `

    selectIpMetadataQuery = `SELECT * FROM ip_metadata WHERE ip = ?`

    insertIpMetadataQuery = `INSERT INTO ip_metadata (ip, hostname, city, region, country, latitude, longitude, postal_code, asn, asn_organization, asn_type)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(ip) DO UPDATE SET
                            hostname=excluded.hostname,
                            city=excluded.city,
                            region=excluded.region,
                            country=excluded.country,
                            latitude=excluded.latitude,
                            longitude=excluded.longitude,
                            postal_code=excluded.postal_code,
                            asn=excluded.asn,
                            asn_organization=excluded.asn_organization,
                            asn_type=excluded.asn_type;`

	insertValidatorCountsQuery = `INSERT INTO validator_counts (peer_id, validator_count, n_observations) VALUES (?, ?, 1) ON CONFLICT (peer_id, validator_count) DO UPDATE SET n_observations = validator_counts.n_observations + 1;`

)

func setupDatabase(db *sql.DB) error {
    _, err := db.Exec(createTrackerTableQuery)
    if err != nil {
        return err
    }

    _, err = db.Exec(createIpMetadataTableQuery)
    if err != nil {
        return err
    }

    _, err = db.Exec(createValidatorCountsTableQuery)
    if err != nil {
        return err
    }

    return nil
}

func loadIPMetadataFromCSV(db *sql.DB, path string) error {
    file, err := os.Open(path)
    if err != nil {
        return errors.Wrap(err, fmt.Sprintf("Error opening csv file: %s", path))
    }
    defer file.Close()

    r := csv.NewReader(file)
    ips, err := r.ReadAll()
    if err != nil {
        return errors.Wrap(err, "Error reading csv records")
    }

    var rowCountStr string
    err = db.QueryRow("SELECT COUNT(ip) FROM ip_metadata").Scan(&rowCountStr)
    if err != nil {
        return errors.Wrap(err, "Error querying ip_metadata database")
    }

    rowCount, _ := strconv.Atoi(rowCountStr)
    if rowCount == 0 {
        tx, err := db.Begin()
        if err != nil {
            return errors.Wrap(err, "Error beginning transaction")
        }
        defer tx.Commit()

        stmt, err := tx.Prepare(insertIpMetadataQuery)
        if err != nil {
            return errors.Wrap(err, "Error preparing insert statement")
        }

        for _, ip := range ips {
            parts := strings.Split(ip[5], ",")
            lat, _ := strconv.ParseFloat(parts[0], 64)
            long, _ := strconv.ParseFloat(parts[1], 64)

            var asnJson asnJSON
            if err := json.Unmarshal([]byte(ip[8]), &asnJson); err != nil {
                return errors.Wrap(err, fmt.Sprintf("Error unmarshalling ASN JSON: %s", ip[8]))
            }
            _, err = stmt.Exec(ip[0], ip[1], ip[2], ip[3], ip[4], lat, long, ip[7], asnJson.Asn, asnJson.AsnOrganization, asnJson.Type)
            if err != nil {
                return errors.Wrap(err, "Error inserting row")
            }
        }
    }

    return nil
}

func insertIPMetadata(tx *sql.Tx, ipEvent *types.IPMetadataEvent) error {
    _, err := tx.Exec(insertIpMetadataQuery,
        ipEvent.IP, ipEvent.Hostname, ipEvent.City, ipEvent.Region, ipEvent.Country,
        ipEvent.Latitude, ipEvent.Longitude, ipEvent.PostalCode, ipEvent.ASN, ipEvent.ASNOrganization, ipEvent.ASNType)
    return err
}

func (c *Consumer) runValidatorMetadataEventHandler(token string) {
    client := ipinfo.NewClient(nil, nil, token)
    batchSize := 0
    var tx *sql.Tx
    var err error

    for {
        select {
        case <-c.done:
            if tx != nil {
                tx.Rollback()
            }
            return
        default:
            func() {
                defer func() {
                    if r := recover(); r != nil {
                        c.log.Error().Interface("recover", r).Msg("Recovered from panic in validator metadata handler")
                        if tx != nil {
                            tx.Rollback()
                        }
                    }
                }()

                if tx == nil {
                    tx, err = c.db.Begin()
                    if err != nil {
                        c.log.Error().Err(err).Msg("Failed to begin transaction")
                        time.Sleep(time.Second)
                        return
                    }
                }

                event, ok := c.validatorMetadataChan.Receive()
                if !ok {
                    c.log.Warn().Msg("Validator metadata channel closed unexpectedly, recreating")
                    c.validatorMetadataChan = NewSafeChannel()
                    return
                }

                c.log.Trace().Any("event", event).Msg("Received validator event")

                maddr, err := ma.NewMultiaddr(event.Multiaddr)
                if err != nil {
                    c.log.Error().Err(err).Msg("Invalid multiaddr")
                    return
                }

                ip, err := maddr.ValueForProtocol(ma.P_IP4)
                if err != nil {
                    ip, err = maddr.ValueForProtocol(ma.P_IP6)
                    if err != nil {
                        c.log.Error().Err(err).Msg("Invalid IP in multiaddr")
                        return
                    }
                }

                portStr, err := maddr.ValueForProtocol(ma.P_TCP)
                if err != nil {
                    c.log.Error().Err(err).Msg("Invalid port in multiaddr")
                    return
                }

                port, err := strconv.Atoi(portStr)
                if err != nil {
                    c.log.Error().Err(err).Msg("Invalid port number")
                    return
                }

                longLived := indexesFromBitfield(event.MetaData.Attnets)
                shortLived := extractShortLivedSubnets(event.SubscribedSubnets, longLived)
                currValidatorCount := len(shortLived)

                var prevTotalObservations int
                err = c.db.QueryRow("SELECT total_observations FROM validator_tracker WHERE peer_id = ?", event.ID).Scan(&prevTotalObservations)

                if err == sql.ErrNoRows {
                    _, err = tx.Exec(insertTrackerQuery, event.ID, event.ENR, event.Multiaddr, ip, port, event.Timestamp, event.Epoch, event.ClientVersion, 1)
                    if err != nil {
                        c.log.Error().Err(err).Msg("Error inserting row")
                    }

                    _, err = tx.Exec(insertValidatorCountsQuery, event.ID, currValidatorCount)
                    if err != nil {
                        c.log.Error().Err(err).Msg("Error inserting validator count")
                    }

                    batchSize++

                    if err := c.db.QueryRow(selectIpMetadataQuery, ip).Scan(); err == sql.ErrNoRows {
                        c.log.Info().Str("ip", ip).Msg("Unknown IP, fetching IP info...")
                        go func(ip string) {
                            ipInfo, err := client.GetIPInfo(net.ParseIP(ip))
                            if err != nil {
                                c.log.Error().Err(err).Msg("Error fetching IP info")
                                return
                            }

                            asn := ""
                            asnOrg := ""
                            asnType := ""
                            if ipInfo.ASN != nil {
                                asn = ipInfo.ASN.ASN
                                asnOrg = ipInfo.ASN.Name
                                asnType = ipInfo.ASN.Type
                            }

                            parts := strings.Split(ipInfo.Location, ",")
                            lat, _ := strconv.ParseFloat(parts[0], 64)
                            long, _ := strconv.ParseFloat(parts[1], 64)

                            ipMeta := types.IPMetadataEvent{
                                IP:               ipInfo.IP.String(),
                                Hostname:         ipInfo.Hostname,
                                City:             ipInfo.City,
                                Region:           ipInfo.Region,
                                Country:          ipInfo.Country,
                                Latitude:         lat,
                                Longitude:        long,
                                PostalCode:       ipInfo.Postal,
                                ASN:              asn,
                                ASNOrganization:  asnOrg,
                                ASNType:          asnType,
                            }

                            if err := insertIPMetadata(tx, &ipMeta); err != nil {
                                c.log.Error().Err(err).Msg("Error inserting IP metadata")
                                return
                            }

                            batchSize++
                            c.ipMetadataChan <- &ipMeta
                        }(ip)
                    }
                    c.log.Trace().Str("peer_id", event.ID).Msg("Inserted new row")
                } else if err != nil {
                    c.log.Error().Err(err).Msg("Error querying validator_tracker database")
                } else {
                    _, err = tx.Exec(updateTrackerQuery, event.ENR, event.Multiaddr, ip, port, event.Timestamp, event.Epoch, event.ClientVersion, event.ID)
                    if err != nil {
                        c.log.Error().Err(err).Msg("Error updating row")
                    }

                    _, err = tx.Exec(insertValidatorCountsQuery, event.ID, currValidatorCount)
                    if err != nil {
                        c.log.Error().Err(err).Msg("Error inserting validator count")
                    }

                    batchSize++
                    c.log.Trace().Str("peer_id", event.ID).Msg("Updated row")
                }

                if batchSize >= 32 {
                    if err := tx.Commit(); err != nil {
                        c.log.Error().Err(err).Msg("Failed to commit transaction")
                        tx.Rollback()
                    }
                    tx = nil
                    batchSize = 0
                }
            }()
        }
    }
}
