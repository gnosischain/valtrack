package config

import (
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p/encoder"
	"github.com/prysmaticlabs/prysm/v5/config/params"
	"github.com/prysmaticlabs/prysm/v5/network/forks"
	pb "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
)

// Bootnodes
var ethBootnodes []string = params.BeaconNetworkConfig().BootstrapNodes

// GetEthereumBootnodes returns the default Ethereum bootnodes in enode format.
func GetEthereumBootnodes() []*enode.Node {
	bootnodes := make([]*enode.Node, len(ethBootnodes))
	for i, enr := range ethBootnodes {
		node, err := enode.Parse(enode.ValidSchemes, enr)
		if err != nil {
			panic(err)
		}
		bootnodes[i] = node
	}
	return bootnodes
}

// Gnosis Bootnodes
var gnosisBootnodes []string = []string{
	"enr:-Ly4QIAhiTHk6JdVhCdiLwT83wAolUFo5J4nI5HrF7-zJO_QEw3cmEGxC1jvqNNUN64Vu-xxqDKSM528vKRNCehZAfEBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpCCS-QxAgAAZP__________gmlkgnY0gmlwhEFtZ5SJc2VjcDI1NmsxoQJwgL5C-30E8RJmW8gCb7sfwWvvfre7wGcCeV4X1G2wJYhzeW5jbmV0cwCDdGNwgiMog3VkcIIjKA",
	"enr:-Ly4QDhEjlkf8fwO5uWAadexy88GXZneTuUCIPHhv98v8ZfXMtC0S1S_8soiT0CMEgoeLe9Db01dtkFQUnA9YcnYC_8Bh2F0dG5ldHOIAAAAAAAAAACEZXRoMpCCS-QxAgAAZP__________gmlkgnY0gmlwhEFtZ5WJc2VjcDI1NmsxoQMRSho89q2GKx_l2FZhR1RmnSiQr6o_9hfXfQUuW6bjMohzeW5jbmV0cwCDdGNwgiMog3VkcIIjKA",
	"enr:-Ly4QLKgv5M2D4DYJgo6s4NG_K4zu4sk5HOLCfGCdtgoezsbfRbfGpQ4iSd31M88ec3DHA5FWVbkgIas9EaJeXia0nwBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpCCS-QxAgAAZP__________gmlkgnY0gmlwhI1eYRaJc2VjcDI1NmsxoQLpK_A47iNBkVjka9Mde1F-Kie-R0sq97MCNKCxt2HwOIhzeW5jbmV0cwCDdGNwgiMog3VkcIIjKA",
	"enr:-Ly4QF_0qvji6xqXrhQEhwJR1W9h5dXV7ZjVCN_NlosKxcgZW6emAfB_KXxEiPgKr_-CZG8CWvTiojEohG1ewF7P368Bh2F0dG5ldHOIAAAAAAAAAACEZXRoMpCCS-QxAgAAZP__________gmlkgnY0gmlwhI1eYUqJc2VjcDI1NmsxoQIpNRUT6llrXqEbjkAodsZOyWv8fxQkyQtSvH4sg2D7n4hzeW5jbmV0cwCDdGNwgiMog3VkcIIjKA",
	"enr:-Ly4QCD5D99p36WafgTSxB6kY7D2V1ca71C49J4VWI2c8UZCCPYBvNRWiv0-HxOcbpuUdwPVhyWQCYm1yq2ZH0ukCbQBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpCCS-QxAgAAZP__________gmlkgnY0gmlwhI1eYVSJc2VjcDI1NmsxoQJJMSV8iSZ8zvkgbi8cjIGEUVJeekLqT0LQha_co-siT4hzeW5jbmV0cwCDdGNwgiMog3VkcIIjKA",
	"enr:-KK4QKXJq1QOVWuJAGige4uaT8LRPQGCVRf3lH3pxjaVScMRUfFW1eiiaz8RwOAYvw33D4EX-uASGJ5QVqVCqwccxa-Bi4RldGgykCGm-DYDAABk__________-CaWSCdjSCaXCEM0QnzolzZWNwMjU2azGhAhNvrRkpuK4MWTf3WqiOXSOePL8Zc-wKVpZ9FQx_BDadg3RjcIIjKIN1ZHCCIyg",
	"enr:-LO4QO87Rn2ejN3SZdXkx7kv8m11EZ3KWWqoIN5oXwQ7iXR9CVGd1dmSyWxOL1PGsdIqeMf66OZj4QGEJckSi6okCdWBpIdhdHRuZXRziAAAAABgAAAAhGV0aDKQPr_UhAQAAGT__________4JpZIJ2NIJpcIQj0iX1iXNlY3AyNTZrMaEDd-_eqFlWWJrUfEp8RhKT9NxdYaZoLHvsp3bbejPyOoeDdGNwgiMog3VkcIIjKA",
	"enr:-LK4QIJUAxX9uNgW4ACkq8AixjnSTcs9sClbEtWRq9F8Uy9OEExsr4ecpBTYpxX66cMk6pUHejCSX3wZkK2pOCCHWHEBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpA-v9SEBAAAZP__________gmlkgnY0gmlwhCPSnDuJc2VjcDI1NmsxoQNuaAjFE-ANkH3pbeBdPiEIwjR5kxFuKaBWxHkqFuPz5IN0Y3CCIyiDdWRwgiMo",
}

// GetGnosisBootnodes returns the default Gnosis bootnodes in enode format.
func GetGnosisBootnodes() []*enode.Node {
	bootnodes := make([]*enode.Node, len(gnosisBootnodes))
	for i, enr := range gnosisBootnodes {
		node, err := enode.Parse(enode.ValidSchemes, enr)
		if err != nil {
			panic(err)
		}
		bootnodes[i] = node
	}
	return bootnodes
}

type DiscConfig struct {
	IP         string
	UDP        int
	TCP        int
	DBPath     string
	ForkDigest [4]byte
	LogPath    string
	Bootnodes  []*enode.Node
	NatsURL    string
}

var DefaultDiscConfig DiscConfig = DiscConfig{
	IP:         "0.0.0.0",
	UDP:        9000,
	TCP:        9000,
	DBPath:     "",
	ForkDigest: [4]byte{0x3e, 0xbf, 0xd4, 0x84},
	LogPath:    "discovery_events.log",
	Bootnodes:  GetGnosisBootnodes(),
}

func (d *DiscConfig) Eth2EnrEntry() (enr.Entry, error) {
	// currentSlot := slots.Since(genesisTime)
	// currentEpoch := slots.ToEpoch(currentSlot)

	// TODO: not hardcoded, use timestamp to calculate
	nextForkVersion, nextForkEpoch, err := forks.NextForkData(16204388)
	if err != nil {
		return nil, fmt.Errorf("calculate next fork data: %w", err)
	}

	enrForkID := &pb.ENRForkID{
		CurrentForkDigest: d.ForkDigest[:],
		NextForkVersion:   nextForkVersion[:],
		NextForkEpoch:     nextForkEpoch,
	}

	enc, err := enrForkID.MarshalSSZ()
	if err != nil {
		return nil, fmt.Errorf("marshal enr fork id: %w", err)
	}

	return enr.WithEntry("eth2", enc), nil
}

// NodeConfig holds additional configuration options for the node.
type NodeConfig struct {
	PrivateKey        *crypto.Secp256k1PrivateKey
	BeaconConfig      *params.BeaconChainConfig
	ForkDigest        [4]byte
	Encoder           encoder.NetworkEncoding
	DialTimeout       time.Duration
	ConcurrentDialers int
	IP                string
	Port              int
	NatsURL           string
	LogPath           string
}

var DefaultNodeConfig NodeConfig = NodeConfig{
	PrivateKey:        nil,
	BeaconConfig:      nil,
	ForkDigest:        [4]byte{0x3e, 0xbf, 0xd4, 0x84},
	Encoder:           encoder.SszNetworkEncoder{},
	DialTimeout:       10 * time.Second,
	ConcurrentDialers: 64,
	IP:                "0.0.0.0",
	Port:              9000,
	LogPath:           "metadata_events.log",
}
