package goactor

import (
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
)

func TestSeqAllocatorUnique(t *testing.T) {
	var s SeqAllocator
	seqs := make([]uint64, 1000)
	var wg sync.WaitGroup
	for i := range seqs {
		wg.Go(func() { seqs[i] = s.Next() })
	}
	wg.Wait()
	slices.Sort(seqs)
	for i, q := range seqs {
		if q != uint64(i+1) {
			t.Fatalf("seqs[%d] = %d, want %d", i, q, i+1)
		}
	}
}

func TestLoadEpochIncrements(t *testing.T) {
	dir := t.TempDir()
	for want := uint32(1); want <= 3; want++ {
		if got, err := LoadEpoch(dir, 1); err != nil || got != want {
			t.Fatalf("got %d, %v, want %d", got, err, want)
		}
	}
	if got, err := LoadEpoch(dir, 2); err != nil || got != 1 {
		t.Fatalf("other node: got %d, %v, want 1", got, err)
	}
}

func TestLoadEpochIgnoresLeftoverTemp(t *testing.T) {
	dir := t.TempDir()
	if _, err := LoadEpoch(dir, 1); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "node-1-epoch.tmp-123"), []byte("9"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := LoadEpoch(dir, 1); err != nil || got != 2 {
		t.Fatalf("got %d, %v, want 2", got, err)
	}
}

func TestLoadEpochRejectsCorrupt(t *testing.T) {
	for _, content := range []string{"", "x\n", "0\n", "4294967295\n", "4294967296\n"} {
		dir := t.TempDir()
		path := filepath.Join(dir, "node-1-epoch")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadEpoch(dir, 1); err == nil {
			t.Errorf("%q: no error", content)
		}
		if b, _ := os.ReadFile(path); string(b) != content {
			t.Errorf("%q: file rewritten to %q", content, b)
		}
	}
}
