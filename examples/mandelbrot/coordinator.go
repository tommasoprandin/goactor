package main

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"image"
	"image/png"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tommasoprandin/goactor"
)

const tickEvery = 250 * time.Millisecond

// Params is the same on every node, since every node passes it to SpawnNamed.
type Params struct {
	Size, Tiles, Workers, MaxIter int
	Nodes                         int
	Out                           string
	SnapshotInterval              time.Duration
	TileTimeout                   time.Duration
	JoinTimeout                   time.Duration
}

func (p Params) validate() error {
	side := tileSide(p.Tiles)
	switch {
	case p.Size < 1 || p.Workers < 1 || p.MaxIter < 1:
		return errors.New("size, workers and max-iter must be positive")
	case side < 1 || side*side != p.Tiles || side > p.Size:
		return fmt.Errorf("tiles must be a perfect square of at most size², got %d", p.Tiles)
	}
	return nil
}

type Hello struct {
	Node goactor.NodeID
}

type TileDone struct {
	Index  int
	Pixels []byte
	Worker goactor.Ref[WorkerMsg]
}

// CoordMsg sets exactly one field.
type CoordMsg struct {
	Hello *Hello
	Done  *TileDone
	Begin bool
	Tick  bool
}

var CoordinatorBehavior = goactor.Register("mandelbrot.Coordinator", func(p Params) goactor.Actor[CoordMsg] {
	return &Coordinator{p: p}
})

type assignment struct {
	worker goactor.Ref[WorkerMsg]
	tile   int
	since  time.Time
}

type Coordinator struct {
	p     Params
	self  goactor.Ref[CoordMsg]
	img   *image.Paletted
	tiles []image.Rectangle

	nodes     map[goactor.NodeID]bool
	idle      []goactor.Ref[WorkerMsg]
	busy      map[goactor.ActorID]assignment
	spawned   int
	shortfall int

	pending    []int
	done       []bool
	completed  int
	indexSum   int
	duplicates int

	started  time.Time
	finished bool

	lastSnapshot time.Time
	snapshotting atomic.Bool
	snapshots    sync.WaitGroup
}

func (c *Coordinator) Start(ctx *goactor.ActorContext[CoordMsg]) {
	c.self = ctx.SelfRef()
	c.img = image.NewPaletted(image.Rect(0, 0, c.p.Size, c.p.Size), palette())
	c.tiles = grid(c.p.Size, tileSide(c.p.Tiles))
	c.done = make([]bool, len(c.tiles))
	for i := range c.tiles {
		c.pending = append(c.pending, i)
	}
	c.nodes = make(map[goactor.NodeID]bool)
	c.busy = make(map[goactor.ActorID]assignment)

	slog.Info("coordinator started", "actor", ctx.Self(), "awaiting_nodes", c.p.Nodes)
	ctx.After(c.p.JoinTimeout, CoordMsg{Begin: true})
	ctx.After(tickEvery, CoordMsg{Tick: true})
}

func (c *Coordinator) Stop(*goactor.ActorContext[CoordMsg], error) {
	c.snapshots.Wait()
}

func (c *Coordinator) Receive(ctx *goactor.ActorContext[CoordMsg], m CoordMsg) {
	switch {
	case m.Hello != nil:
		c.hello(ctx, m.Hello.Node)
	case m.Done != nil:
		c.tileDone(*m.Done)
	case m.Begin:
		c.begin(ctx)
	case m.Tick:
		c.tick(ctx)
	}
}

func (c *Coordinator) hello(ctx *goactor.ActorContext[CoordMsg], node goactor.NodeID) {
	c.nodes[node] = true
	slog.Info("node ready", "node", node, "ready", len(c.nodes), "of", c.p.Nodes)
	if len(c.nodes) >= c.p.Nodes {
		c.begin(ctx)
	}
}

func (c *Coordinator) begin(ctx *goactor.ActorContext[CoordMsg]) {
	if !c.started.IsZero() {
		return
	}
	c.started = time.Now()
	c.shortfall = c.p.Workers
	slog.Info("render started", "size", c.p.Size, "tiles", len(c.tiles), "workers", c.p.Workers,
		"max_iter", c.p.MaxIter, "nodes_ready", len(c.nodes))
	c.refill(ctx)
	c.dispatch()
}

func (c *Coordinator) tick(ctx *goactor.ActorContext[CoordMsg]) {
	if c.finished {
		return
	}
	if !c.started.IsZero() {
		now := time.Now()
		for _, a := range c.busy {
			if err := a.worker.Send(WorkerMsg{Ping: true}); err != nil {
				c.lost(a.worker, err)
			} else if now.Sub(a.since) > c.p.TileTimeout {
				c.lost(a.worker, goactor.ErrTimeout)
			}
		}
		c.refill(ctx)
		c.dispatch()
		c.snapshot(now)
	}
	ctx.After(tickEvery, CoordMsg{Tick: true})
}

func (c *Coordinator) refill(ctx *goactor.ActorContext[CoordMsg]) {
	for ; c.shortfall > 0; c.shortfall-- {
		w, err := goactor.Spawn(ctx, WorkerBehavior, struct{}{})
		if err != nil {
			slog.Warn("worker spawn failed", "err", err)
			return
		}
		event := "worker spawned"
		if c.spawned >= c.p.Workers {
			event = "worker replaced"
		}
		c.spawned++
		c.idle = append(c.idle, w)
		slog.Info(event, "worker", w.ID(), "node", w.ID().Node)
	}
}

func (c *Coordinator) dispatch() {
	for len(c.idle) > 0 && len(c.pending) > 0 {
		i := c.pending[0]
		if c.done[i] {
			c.pending = c.pending[1:]
			continue
		}
		w := c.idle[0]
		c.idle = c.idle[1:]
		t := Tile{Index: i, Rect: c.tiles[i], Size: c.p.Size, MaxIter: c.p.MaxIter, ReplyTo: c.self}
		if err := w.Send(WorkerMsg{Tile: &t}); err != nil {
			c.lost(w, err)
			continue
		}
		c.pending = c.pending[1:]
		c.busy[w.ID()] = assignment{w, i, time.Now()}
		slog.Info("tile dispatched", "tile", i, "worker", w.ID())
	}
}

func (c *Coordinator) lost(w goactor.Ref[WorkerMsg], err error) {
	c.shortfall++
	tile := -1
	if a, ok := c.busy[w.ID()]; ok {
		delete(c.busy, w.ID())
		tile = a.tile
		if !c.done[tile] {
			c.pending = append(c.pending, tile)
		}
	}
	slog.Warn("worker lost", "worker", w.ID(), "node", w.ID().Node, "requeued_tile", tile, "err", err)
}

func (c *Coordinator) tileDone(d TileDone) {
	if c.finished {
		return
	}
	id := d.Worker.ID()
	if _, ok := c.busy[id]; ok {
		delete(c.busy, id)
		c.idle = append(c.idle, d.Worker)
	}
	if c.done[d.Index] {
		c.duplicates++
		slog.Info("duplicate tile dropped", "tile", d.Index, "worker", id)
	} else {
		c.paste(c.tiles[d.Index], d.Pixels)
		c.done[d.Index] = true
		c.completed++
		c.indexSum += d.Index
		slog.Info("tile completed", "tile", d.Index, "worker", id, "completed", c.completed, "of", len(c.tiles))
	}
	if c.completed == len(c.tiles) {
		c.finish()
		return
	}
	c.dispatch()
}

func (c *Coordinator) paste(r image.Rectangle, pix []byte) {
	w := r.Dx()
	for y := range r.Dy() {
		copy(c.img.Pix[c.img.PixOffset(r.Min.X, r.Min.Y+y):], pix[y*w:(y+1)*w])
	}
}

func (c *Coordinator) finish() {
	elapsed := time.Since(c.started)
	c.finished = true
	c.snapshots.Wait()

	n := len(c.tiles)
	expected := n * (n - 1) / 2
	if c.indexSum != expected || bytes.IndexByte(c.img.Pix, background) >= 0 {
		slog.Error("render incomplete", "index_sum", c.indexSum, "expected", expected)
		return
	}
	b, err := encodePNG(c.img, png.DefaultCompression)
	if err == nil {
		err = replaceFile(c.p.Out, b)
	}
	if err == nil && c.p.SnapshotInterval > 0 {
		err = replaceFile(snapshotPath(c.p.Out), b)
	}
	if err != nil {
		slog.Error("writing image", "err", err)
		return
	}
	slog.Info("render complete", "tiles", n, "index_sum", c.indexSum, "expected", expected,
		"duplicates", c.duplicates, "elapsed", elapsed, "sha256", fmt.Sprintf("%x", sha256.Sum256(b)))
}

// snapshot encodes off the actor's goroutine: doing it in Receive would stall dispatch and leave
// every worker idle.
func (c *Coordinator) snapshot(now time.Time) {
	if c.p.SnapshotInterval <= 0 || now.Sub(c.lastSnapshot) < c.p.SnapshotInterval || !c.snapshotting.CompareAndSwap(false, true) {
		return
	}
	c.lastSnapshot = now
	img := *c.img
	img.Pix = bytes.Clone(c.img.Pix)
	c.snapshots.Go(func() {
		defer c.snapshotting.Store(false)
		b, err := encodePNG(&img, png.BestSpeed)
		if err == nil {
			err = replaceFile(snapshotPath(c.p.Out), b)
		}
		if err != nil {
			slog.Warn("writing snapshot", "err", err)
		}
	})
}

func encodePNG(img image.Image, level png.CompressionLevel) ([]byte, error) {
	var b bytes.Buffer
	enc := png.Encoder{CompressionLevel: level}
	err := enc.Encode(&b, img)
	return b.Bytes(), err
}

func snapshotPath(out string) string {
	return strings.TrimSuffix(out, ".png") + ".snapshot.png"
}

// replaceFile swaps path atomically, so a viewer never reads a partial image.
func replaceFile(path string, b []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, err = f.Write(b)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Chmod(f.Name(), 0o644)
	}
	if err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}
