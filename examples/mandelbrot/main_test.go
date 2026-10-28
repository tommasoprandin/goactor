package main

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	envChild      = "MANDELBROT_TEST_CHILD"
	renderTimeout = time.Minute
)

func TestMain(m *testing.M) {
	if os.Getenv(envChild) != "" {
		main()
		return
	}
	os.Exit(m.Run())
}

func TestThreeNodesMatchSingleNode(t *testing.T) {
	reference, _ := renderCluster(t, 1)
	image, logs := renderCluster(t, 2, 3, 1)

	if !bytes.Equal(reference, image) {
		t.Error("3-node image differs from the single-node reference")
	}
	for id, l := range logs {
		want := 0
		if id == 3 {
			want = 1
		}
		if got := strings.Count(l, "coordinator started"); got != want {
			t.Errorf("node %d started %d coordinators, want %d", id, got, want)
		}
	}
}

// renderCluster runs one process per id, in the given order, and returns the image and each
// node's log.
func renderCluster(t *testing.T, order ...int) ([]byte, map[int]string) {
	t.Helper()
	dir := t.TempDir()
	var cfg strings.Builder
	cfg.WriteString("cluster:\n")
	for id := 1; id <= len(order); id++ {
		fmt.Fprintf(&cfg, "  - { id: %d, gossip: %q, grpc: %q }\n", id, freeAddr(t), freeAddr(t))
	}
	fmt.Fprintf(&cfg, "data_dir: %q\n", dir)
	cfgPath := filepath.Join(dir, "cluster.yaml")
	if err := os.WriteFile(cfgPath, []byte(cfg.String()), 0o644); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(dir, "out.png")
	nodes := make(map[int]*exec.Cmd)
	logs := make(map[int]*bytes.Buffer)
	for _, id := range order {
		cmd := exec.Command(os.Args[0], "-config", cfgPath, "-id", strconv.Itoa(id), "-out", out,
			"-size", "256", "-tiles", "16", "-max-iter", "200", "-snapshot-interval", "0")
		cmd.Env = append(os.Environ(), envChild+"=1")
		logs[id] = new(bytes.Buffer)
		cmd.Stderr = logs[id]
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		nodes[id] = cmd
		t.Cleanup(func() { cmd.Process.Kill() })
		time.Sleep(200 * time.Millisecond)
	}

	image := waitForFile(t, out)
	result := make(map[int]string)
	for id, cmd := range nodes {
		if err := cmd.Process.Signal(os.Interrupt); err != nil {
			t.Fatal(err)
		}
		if err := cmd.Wait(); err != nil {
			t.Errorf("node %d: %v\n%s", id, err, logs[id])
		}
		result[id] = logs[id].String()
	}
	return image, result
}

func waitForFile(t *testing.T, path string) []byte {
	t.Helper()
	deadline := time.Now().Add(renderTimeout)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(path)
		if err == nil {
			return b
		}
		if !errors.Is(err, fs.ErrNotExist) {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no image after %v", renderTimeout)
	return nil
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}
