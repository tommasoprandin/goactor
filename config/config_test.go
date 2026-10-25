package config

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

const example = `
cluster:
  - { id: 1, gossip: "10.0.0.1:7946", grpc: "10.0.0.1:9000" }
  - { id: 2, gossip: "10.0.0.2:7946", grpc: "10.0.0.2:9000" }
  - { id: 3, gossip: "10.0.0.3:7946", grpc: "10.0.0.3:9000" }
data_dir: /var/lib/goactor

gossip:
  probe_interval:  1s
  probe_timeout:   500ms
  indirect_checks: 3
  suspicion_mult:  4
`

const base = "cluster: [{id: 1, gossip: 'h:1', grpc: 'h:2'}]\ndata_dir: /d\n"

var wantGossip = GossipConfig{
	ProbeInterval:  time.Second,
	ProbeTimeout:   500 * time.Millisecond,
	IndirectChecks: 3,
	SuspicionMult:  4,
}

func TestLoadExample(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cluster.yaml")
	if err := os.WriteFile(path, []byte(example), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	want := &Config{
		Cluster: []NodeConfig{
			{ID: 1, Gossip: "10.0.0.1:7946", GRPC: "10.0.0.1:9000"},
			{ID: 2, Gossip: "10.0.0.2:7946", GRPC: "10.0.0.2:9000"},
			{ID: 3, Gossip: "10.0.0.3:7946", GRPC: "10.0.0.3:9000"},
		},
		DataDir: "/var/lib/goactor",
		Gossip:  wantGossip,
	}
	if !reflect.DeepEqual(c, want) {
		t.Fatalf("got %+v, want %+v", c, want)
	}
	if c.N() != 3 {
		t.Errorf("N = %d, want 3", c.N())
	}
	if n, err := c.Node(2); err != nil || n != want.Cluster[1] {
		t.Errorf("Node(2) = %+v, %v", n, err)
	}
	if _, err := c.Node(4); !errors.Is(err, ErrUnknownNode) {
		t.Errorf("Node(4): got %v", err)
	}
}

func TestRejectsInvalid(t *testing.T) {
	for _, tc := range []struct {
		yaml string
		want error
	}{
		{"data_dir: /d\n", ErrEmptyCluster},
		{"cluster: [{id: 0, gossip: 'h:1', grpc: 'h:2'}]\ndata_dir: /d\n", ErrInvalidNodeID},
		{"cluster: [{id: 1, gossip: 'h:1', grpc: 'h:2'}, {id: 1, gossip: 'h:3', grpc: 'h:4'}]\ndata_dir: /d\n", ErrDuplicateNodeID},
		{"cluster: [{id: 1, gossip: ':1', grpc: 'h:2'}]\ndata_dir: /d\n", ErrInvalidAddr},
		{"cluster: [{id: 1, gossip: 'h:1', grpc: 'h'}]\ndata_dir: /d\n", ErrInvalidAddr},
		{"cluster: [{id: 1, gossip: 'h:1', grpc: 'h:65536'}]\ndata_dir: /d\n", ErrInvalidAddr},
		{"cluster: [{id: 1, gossip: 'h:1', grpc: 'h:2'}]\n", ErrMissingDataDir},
		{base + "gossip: {probe_interval: 0s}\n", ErrNonPositiveDuration},
		{base + "gossip: {probe_timeout: -1s}\n", ErrNonPositiveDuration},
		{base + "gossip: {probe_timeout: 1s}\n", ErrProbeTimeout},
		{base + "gossip: {suspicion_mult: 0}\n", ErrSuspicionMult},
		{base + "gossip: {indirect_checks: -1}\n", ErrIndirectChecks},
	} {
		if _, err := parse([]byte(tc.yaml)); !errors.Is(err, tc.want) {
			t.Errorf("%q: got %v, want %v", tc.yaml, err, tc.want)
		}
	}
}

func TestRejectsUnknownKey(t *testing.T) {
	for _, key := range []string{
		"gossip: {probe_intervall: 2s}\n",
		"gossip: {meta_refresh: 1s}\n",
		"placement: {w_cpu: 0.3}\n",
	} {
		if _, err := parse([]byte(base + key)); err == nil {
			t.Errorf("%q: no error", key)
		}
	}
}

func TestDefaults(t *testing.T) {
	for _, src := range []string{base, base + "gossip:\n"} {
		c, err := parse([]byte(src))
		if err != nil {
			t.Fatal(err)
		}
		if c.Gossip != wantGossip {
			t.Errorf("%q: got %+v", src, c.Gossip)
		}
	}
}

func TestPartialBlocks(t *testing.T) {
	c, err := parse([]byte(base + "gossip: {probe_interval: 2s, suspicion_mult: 6}\n"))
	if err != nil {
		t.Fatal(err)
	}
	g := wantGossip
	g.ProbeInterval, g.SuspicionMult = 2*time.Second, 6
	if c.Gossip != g {
		t.Errorf("got %+v", c.Gossip)
	}
}

func TestDuration(t *testing.T) {
	for in, want := range map[string]time.Duration{
		"1s":    time.Second,
		"500ms": 500 * time.Millisecond,
		"1m30s": 90 * time.Second,
	} {
		c, err := parse([]byte(base + "gossip: {probe_interval: 1h, probe_timeout: " + in + "}\n"))
		if err != nil || c.Gossip.ProbeTimeout != want {
			t.Errorf("%s: got %v, %v", in, c, err)
		}
	}
	if _, err := parse([]byte(base + "gossip: {probe_timeout: 1}\n")); err == nil {
		t.Error("unitless 1: no error")
	}
}
