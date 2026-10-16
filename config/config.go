// Package config loads the cluster configuration. Every node is supposed to 
// load an identical file.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/tommasoprandin/goactor/internal/ident"
)

var (
	ErrEmptyCluster        = errors.New("config: empty cluster")
	ErrInvalidNodeID       = errors.New("config: node id 0 is reserved")
	ErrDuplicateNodeID     = errors.New("config: duplicate node id")
	ErrInvalidAddr         = errors.New("config: invalid host:port")
	ErrMissingDataDir      = errors.New("config: missing data_dir")
	ErrNonPositiveDuration = errors.New("config: non-positive duration")
	ErrProbeTimeout        = errors.New("config: probe_timeout must be smaller than probe_interval")
	ErrSuspicionMult       = errors.New("config: suspicion_mult below 1")
	ErrIndirectChecks      = errors.New("config: negative indirect_checks")
	ErrNegativeWeight      = errors.New("config: negative placement weight")
	ErrZeroWeights         = errors.New("config: all placement weights are zero")
	ErrNonPositiveK        = errors.New("config: non-positive k_actors or k_depth")
	ErrUnknownNode         = errors.New("config: unknown node")
)

type Config struct {
	Cluster   []NodeConfig
	DataDir   string
	Gossip    GossipConfig
	Placement PlacementConfig
}

type NodeConfig struct {
	ID     ident.NodeID `yaml:"id"`
	Gossip string       `yaml:"gossip"`
	GRPC   string       `yaml:"grpc"`
}

type GossipConfig struct {
	ProbeInterval  time.Duration
	ProbeTimeout   time.Duration
	IndirectChecks int
	SuspicionMult  int
	MetaRefresh    time.Duration
}

type PlacementConfig struct {
	WActors float64 `yaml:"w_actors"`
	WDepth  float64 `yaml:"w_depth"`
	WCPU    float64 `yaml:"w_cpu"`
	KActors float64 `yaml:"k_actors"`
	KDepth  float64 `yaml:"k_depth"`
}

type file struct {
	Cluster   []NodeConfig    `yaml:"cluster"`
	DataDir   string          `yaml:"data_dir"`
	Gossip    gossipFile      `yaml:"gossip"`
	Placement PlacementConfig `yaml:"placement"`
}

type gossipFile struct {
	ProbeInterval  duration `yaml:"probe_interval"`
	ProbeTimeout   duration `yaml:"probe_timeout"`
	IndirectChecks int      `yaml:"indirect_checks"`
	SuspicionMult  int      `yaml:"suspicion_mult"`
	MetaRefresh    duration `yaml:"meta_refresh"`
}

// N.B. yaml.v3 decodes time.Duration only from integer nanoseconds.
type duration time.Duration

func (d *duration) UnmarshalYAML(n *yaml.Node) error {
	v, err := time.ParseDuration(n.Value)
	if err != nil {
		return fmt.Errorf("line %d: %w", n.Line, err)
	}
	*d = duration(v)
	return nil
}

var (
	defaultGossip = gossipFile{
		ProbeInterval:  duration(time.Second),
		ProbeTimeout:   duration(500 * time.Millisecond),
		IndirectChecks: 3,
		SuspicionMult:  4,
		MetaRefresh:    duration(time.Second),
	}
	defaultPlacement = PlacementConfig{WActors: 0.2, WDepth: 0.5, WCPU: 0.3, KActors: 100, KDepth: 8}
)

// Load reads and validates the configuration at path. Absent gossip and placement keys take their
// defaults; unknown keys are rejected.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parse(b)
}

func parse(b []byte) (*Config, error) {
	f := file{Gossip: defaultGossip, Placement: defaultPlacement}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("config: %w", err)
	}
	c := &Config{
		Cluster: f.Cluster,
		DataDir: f.DataDir,
		Gossip: GossipConfig{
			ProbeInterval:  time.Duration(f.Gossip.ProbeInterval),
			ProbeTimeout:   time.Duration(f.Gossip.ProbeTimeout),
			IndirectChecks: f.Gossip.IndirectChecks,
			SuspicionMult:  f.Gossip.SuspicionMult,
			MetaRefresh:    time.Duration(f.Gossip.MetaRefresh),
		},
		Placement: f.Placement,
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Config) validate() error {
	if len(c.Cluster) == 0 {
		return ErrEmptyCluster
	}
	seen := make(map[ident.NodeID]bool, len(c.Cluster))
	for _, n := range c.Cluster {
		if n.ID == 0 {
			return ErrInvalidNodeID
		}
		if seen[n.ID] {
			return fmt.Errorf("%w: %d", ErrDuplicateNodeID, n.ID)
		}
		seen[n.ID] = true
		for _, a := range []string{n.Gossip, n.GRPC} {
			if !validAddr(a) {
				return fmt.Errorf("%w: node %d: %q", ErrInvalidAddr, n.ID, a)
			}
		}
	}
	if c.DataDir == "" {
		return ErrMissingDataDir
	}

	g := c.Gossip
	switch {
	case g.ProbeInterval <= 0:
		return fmt.Errorf("%w: probe_interval", ErrNonPositiveDuration)
	case g.ProbeTimeout <= 0:
		return fmt.Errorf("%w: probe_timeout", ErrNonPositiveDuration)
	case g.MetaRefresh <= 0:
		return fmt.Errorf("%w: meta_refresh", ErrNonPositiveDuration)
	case g.ProbeTimeout >= g.ProbeInterval:
		return ErrProbeTimeout
	case g.SuspicionMult < 1:
		return ErrSuspicionMult
	case g.IndirectChecks < 0:
		return ErrIndirectChecks
	}

	p := c.Placement
	switch {
	case p.WActors < 0 || p.WDepth < 0 || p.WCPU < 0:
		return ErrNegativeWeight
	case p.WActors == 0 && p.WDepth == 0 && p.WCPU == 0:
		return ErrZeroWeights
	case p.KActors <= 0 || p.KDepth <= 0:
		return ErrNonPositiveK
	}
	return nil
}

func validAddr(a string) bool {
	host, port, err := net.SplitHostPort(a)
	if err != nil || host == "" {
		return false
	}
	p, err := strconv.ParseUint(port, 10, 16)
	return err == nil && p != 0
}

func (c *Config) Node(id ident.NodeID) (NodeConfig, error) {
	for _, n := range c.Cluster {
		if n.ID == id {
			return n, nil
		}
	}
	return NodeConfig{}, fmt.Errorf("%w: %d", ErrUnknownNode, id)
}

func (c *Config) N() int {
	return len(c.Cluster)
}
