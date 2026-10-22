package goactor

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tommasoprandin/goactor/config"
	"github.com/tommasoprandin/goactor/transport"
)

// A multi-node test runs one process per node. The test process is node 1; startCluster
// re-executes the test binary for every other node, and these variables switch TestMain into
// node mode.
const (
	envNode     = "GOACTOR_TEST_NODE"
	envScenario = "GOACTOR_TEST_SCENARIO"
	envConfig   = "GOACTOR_TEST_CONFIG"
)

const harnessTimeout = 10 * time.Second

// scenarios spawn a node's actors and return their ids, which the node prints for the parent.
var scenarios = map[string]func(*System) ([]ActorID, error){}

func runNode() int {
	if err := serveNode(); err != nil {
		fmt.Fprintf(os.Stderr, "node %s: %v\n", os.Getenv(envNode), err)
		return 1
	}
	return 0
}

func serveNode() error {
	id, err := strconv.ParseUint(os.Getenv(envNode), 10, 16)
	if err != nil {
		return err
	}
	scenario, ok := scenarios[os.Getenv(envScenario)]
	if !ok {
		return fmt.Errorf("unknown scenario %q", os.Getenv(envScenario))
	}
	cfg, err := config.Load(os.Getenv(envConfig))
	if err != nil {
		return err
	}
	s, err := Start(cfg, NodeID(id))
	if err != nil {
		return err
	}
	defer s.Shutdown()
	if !eventually(func() bool { return meshed(s) }) {
		return fmt.Errorf("no full mesh after %v", harnessTimeout)
	}
	ids, err := scenario(s)
	if err != nil {
		return err
	}
	for _, a := range ids {
		fmt.Println(a)
	}
	fmt.Println()
	_, err = io.Copy(io.Discard, os.Stdin)
	return err
}

func eventually(cond func() bool) bool {
	deadline := time.Now().Add(harnessTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
	return true
}

// meshed reports whether s has a stream to every other node.
func meshed(s *System) bool {
	tr := s.out.(*transport.Transport)
	for _, n := range s.cfg.Cluster {
		if n.ID != s.node && tr.Epoch(n.ID) == 0 {
			return false
		}
	}
	return true
}

type testCluster struct {
	t        *testing.T
	path     string
	scenario string
	sys      *System
	nodes    map[NodeID]*testNode
}

type testNode struct {
	id     NodeID
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	actors []ActorID
}

// startCluster runs node 1 in this process and nodes 2..n as child processes running scenario.
// It returns once every node has a stream to every other.
func startCluster(t *testing.T, n int, scenario string) *testCluster {
	t.Helper()
	dir := t.TempDir()
	var b strings.Builder
	b.WriteString("cluster:\n")
	for id := 1; id <= n; id++ {
		fmt.Fprintf(&b, "  - { id: %d, gossip: %q, grpc: %q }\n", id, freeAddr(t), freeAddr(t))
	}
	fmt.Fprintf(&b, "data_dir: %q\n", dir)
	path := filepath.Join(dir, "cluster.yaml")
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Start(cfg, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Shutdown)

	c := &testCluster{t: t, path: path, scenario: scenario, sys: s, nodes: make(map[NodeID]*testNode)}
	t.Cleanup(c.stop)
	for id := NodeID(2); id <= NodeID(n); id++ {
		c.nodes[id] = c.launch(id)
	}
	for _, nd := range c.nodes {
		nd.ready(t)
	}
	if !eventually(func() bool { return meshed(s) }) {
		t.Fatal("node 1: no full mesh")
	}
	return c
}

func freeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func (c *testCluster) launch(id NodeID) *testNode {
	c.t.Helper()
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(),
		envNode+"="+strconv.Itoa(int(id)),
		envScenario+"="+c.scenario,
		envConfig+"="+c.path,
	)
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		c.t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		c.t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		c.t.Fatal(err)
	}
	return &testNode{id: id, cmd: cmd, stdin: stdin, stdout: stdout}
}

// restart kills node id and runs it again on the same data directory, so it takes a fresh epoch.
func (c *testCluster) restart(id NodeID) {
	c.t.Helper()
	tr := c.sys.out.(*transport.Transport)
	before := tr.Epoch(id)
	old := c.nodes[id]
	old.cmd.Process.Kill()
	old.cmd.Wait()
	c.nodes[id] = c.launch(id)
	c.nodes[id].ready(c.t)
	if !eventually(func() bool { return tr.Epoch(id) > before }) {
		c.t.Fatalf("node 1: no stream to restarted node %d", id)
	}
}

func (c *testCluster) stop() {
	for _, nd := range c.nodes {
		nd.stdin.Close()
	}
	for _, nd := range c.nodes {
		if err := nd.wait(); err != nil {
			c.t.Errorf("node %d: %v", nd.id, err)
		}
	}
}

// ready reads the actor ids nd prints, up to the blank line that ends them.
func (nd *testNode) ready(t *testing.T) {
	t.Helper()
	lines := make(chan []string, 1)
	go func() {
		var ls []string
		sc := bufio.NewScanner(nd.stdout)
		for sc.Scan() {
			if sc.Text() == "" {
				lines <- ls
				return
			}
			ls = append(ls, sc.Text())
		}
		close(lines)
	}()
	select {
	case ls, ok := <-lines:
		if !ok {
			t.Fatalf("node %d exited before it was ready", nd.id)
		}
		for _, l := range ls {
			a, err := ParseActorID(l)
			if err != nil {
				t.Fatal(err)
			}
			nd.actors = append(nd.actors, a)
		}
	case <-time.After(harnessTimeout):
		nd.cmd.Process.Kill()
		t.Fatalf("node %d not ready after %v", nd.id, harnessTimeout)
	}
}

func (nd *testNode) wait() error {
	done := make(chan error, 1)
	go func() { done <- nd.cmd.Wait() }()
	select {
	case err := <-done:
		return err
	case <-time.After(harnessTimeout):
		nd.cmd.Process.Kill()
		<-done
		return fmt.Errorf("killed after %v without exiting", harnessTimeout)
	}
}
