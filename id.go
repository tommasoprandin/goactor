package goactor

import (
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/tommasoprandin/goactor/internal/ident"
)

type (
	NodeID  = ident.NodeID
	ActorID = ident.ActorID
)

func ParseActorID(s string) (ActorID, error) {
	return ident.ParseActorID(s)
}

// SeqAllocator hands out sequence numbers from 1; 0 is never allocated. The zero value is ready
// for use and safe for concurrent use.
type SeqAllocator struct {
	n atomic.Uint64
}

func (s *SeqAllocator) Next() uint64 {
	return s.n.Add(1)
}

// LoadEpoch increments the epoch persisted in <dataDir>/node-<node>-epoch and returns the new value,
// 1 if the file does not exist. A corrupt file is an error, never a reset: reusing an epoch would
// let stale references reach new actors.
func LoadEpoch(dataDir string, node NodeID) (uint32, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return 0, err
	}
	path := filepath.Join(dataDir, fmt.Sprintf("node-%d-epoch", node))

	var epoch uint32
	b, err := os.ReadFile(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
	case err != nil:
		return 0, err
	default:
		n, err := strconv.ParseUint(strings.TrimSuffix(string(b), "\n"), 10, 32)
		if err != nil || n == 0 {
			return 0, fmt.Errorf("goactor: corrupt epoch file %s: %q", path, b)
		}
		if n == math.MaxUint32 {
			return 0, fmt.Errorf("goactor: epochs exhausted in %s", path)
		}
		epoch = uint32(n)
	}
	epoch++

	if err := replaceFile(path, []byte(strconv.FormatUint(uint64(epoch), 10)+"\n")); err != nil {
		return 0, err
	}
	return epoch, nil
}

func replaceFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(f.Name(), path)
	}
	if err != nil {
		os.Remove(f.Name())
		return err
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}
