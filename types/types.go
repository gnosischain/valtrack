package types

import "github.com/prysmaticlabs/go-bitfield"

type ValidatorEvent struct {
	ENR       string `parquet:"name=enr, type=BYTE_ARRAY, convertedtype=UTF8" json:"enr" ch:"enr"`
	ID        string `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8" json:"id" ch:"id"`
	Multiaddr string `parquet:"name=multiaddr, type=BYTE_ARRAY, convertedtype=UTF8" json:"multiaddr" ch:"multiaddr"`
	Epoch     int    `parquet:"name=epoch, type=INT32" json:"epoch" ch:"epoch"`

	// Metadata
	SeqNumber int64 `parquet:"name=seq_number, type=INT64" json:"seq_number" ch:"seq_number"`
	// NOTE: These are bitfields. They are stored as strings in the database due to the lack of support for bitfields in Clickhouse.
	Attnets  string `parquet:"name=attnets, type=BYTE_ARRAY, convertedtype=UTF8" json:"attnets" ch:"attnets"`
	Syncnets string `parquet:"name=syncnets, type=BYTE_ARRAY, convertedtype=UTF8" json:"syncnets" ch:"syncnets"`

	LongLivedSubnets  []int64 `parquet:"name=long_lived_subnets, type=LIST, valuetype=INT64" json:"long_lived_subnets" ch:"long_lived_subnets"`
	SubscribedSubnets []int64 `parquet:"name=subscribed_subnets, type=LIST, valuetype=INT64" json:"subscribed_subnets" ch:"subscribed_subnets"`
	ClientVersion     string  `parquet:"name=client_version, type=BYTE_ARRAY, convertedtype=UTF8" json:"client_version" ch:"client_version"`
	CrawlerID         string  `parquet:"name=crawler_id, type=BYTE_ARRAY, convertedtype=UTF8" json:"crawler_id" ch:"crawler_id"`
	CrawlerLoc        string  `parquet:"name=crawler_location, type=BYTE_ARRAY, convertedtype=UTF8" json:"crawler_location" ch:"crawler_location"`
	Timestamp         int64   `parquet:"name=timestamp, type=INT64" json:"timestamp" ch:"timestamp"`
}

type PeerDiscoveredEvent struct {
	ENR        string `parquet:"name=enr, type=BYTE_ARRAY, convertedtype=UTF8" json:"enr" ch:"enr"`
	ID         string `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8" json:"id" ch:"id"`
	IP         string `parquet:"name=ip, type=BYTE_ARRAY, convertedtype=UTF8" json:"ip" ch:"ip"`
	Port       int    `parquet:"name=port, type=INT32" json:"port" ch:"port"`
	CrawlerID  string `parquet:"name=crawler_id, type=BYTE_ARRAY, convertedtype=UTF8" json:"crawler_id" ch:"crawler_id"`
	CrawlerLoc string `parquet:"name=crawler_location, type=BYTE_ARRAY, convertedtype=UTF8" json:"crawler_location" ch:"crawler_location"`
	Timestamp  int64  `parquet:"name=timestamp, type=INT64" json:"timestamp" ch:"timestamp"`
}

type MetadataReceivedEvent struct {
	ENR               string          `parquet:"name=enr, type=BYTE_ARRAY, convertedtype=UTF8" json:"enr" ch:"enr"`
	ID                string          `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8" json:"id" ch:"id"`
	Multiaddr         string          `parquet:"name=multiaddr, type=BYTE_ARRAY, convertedtype=UTF8" json:"multiaddr" ch:"multiaddr"`
	Epoch             int             `parquet:"name=epoch, type=INT32" json:"epoch" ch:"epoch"`
	MetaData          *SimpleMetaData `parquet:"name=metadata, type=BYTE_ARRAY, convertedtype=UTF8" json:"metadata" ch:"metadata"`
	SubscribedSubnets []int64         `parquet:"name=subscribed_subnets, type=LIST, valuetype=INT64" json:"subscribed_subnets" ch:"subscribed_subnets"`
	ClientVersion     string          `parquet:"name=client_version, type=BYTE_ARRAY, convertedtype=UTF8" json:"client_version" ch:"client_version"`
	CrawlerID         string          `parquet:"name=crawler_id, type=BYTE_ARRAY, convertedtype=UTF8" json:"crawler_id" ch:"crawler_id"`
	CrawlerLoc        string          `parquet:"name=crawler_location, type=BYTE_ARRAY, convertedtype=UTF8" json:"crawler_location" ch:"crawler_location"`
	Timestamp         int64           `parquet:"name=timestamp, type=INT64" json:"timestamp" ch:"timestamp"`
}

type SimpleMetaData struct {
	SeqNumber int64                `parquet:"name=seq_number, type=INT64" json:"seq_number" ch:"seq_number"`
	Attnets   bitfield.Bitvector64 `parquet:"name=attnets, type=LIST, valuetype=BYTE_ARRAY" json:"attnets" ch:"attnets"`
	Syncnets  bitfield.Bitvector4  `parquet:"name=syncnets, type=LIST, valuetype=BYTE_ARRAY" json:"syncnets" ch:"syncnets"`
}

type IPMetadataEvent struct {
    IP                string  `parquet:"name=ip, type=BYTE_ARRAY, convertedtype=UTF8" json:"ip" ch:"ip"`
    Hostname          string  `parquet:"name=hostname, type=BYTE_ARRAY, convertedtype=UTF8" json:"hostname" ch:"hostname"`
    City              string  `parquet:"name=city, type=BYTE_ARRAY, convertedtype=UTF8" json:"city" ch:"city"`
    Region            string  `parquet:"name=region, type=BYTE_ARRAY, convertedtype=UTF8" json:"region" ch:"region"`
    Country           string  `parquet:"name=country, type=BYTE_ARRAY, convertedtype=UTF8" json:"country" ch:"country"`
    Latitude          float64 `parquet:"name=latitude, type=DOUBLE" json:"latitude" ch:"latitude"`
    Longitude         float64 `parquet:"name=longitude, type=DOUBLE" json:"longitude" ch:"longitude"`
    PostalCode        string  `parquet:"name=postal_code, type=BYTE_ARRAY, convertedtype=UTF8" json:"postal_code" ch:"postal_code"`
    ASN               string  `parquet:"name=asn, type=BYTE_ARRAY, convertedtype=UTF8" json:"asn" ch:"asn"`
    ASNOrganization   string  `parquet:"name=asn_organization, type=BYTE_ARRAY, convertedtype=UTF8" json:"asn_organization" ch:"asn_organization"`
    ASNType           string  `parquet:"name=asn_type, type=BYTE_ARRAY, convertedtype=UTF8" json:"asn_type" ch:"asn_type"`
}
