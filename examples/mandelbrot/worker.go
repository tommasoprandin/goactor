package main

import (
	"image"
	"log/slog"

	"github.com/tommasoprandin/goactor"
)

// Tile carries everything needed to compute it, so workers hold no state.
type Tile struct {
	Index   int
	Rect    image.Rectangle
	Size    int
	MaxIter int
	ReplyTo goactor.Ref[CoordMsg]
}

// WorkerMsg sets exactly one field. A Ping does nothing: it exists for its sender's error.
type WorkerMsg struct {
	Tile *Tile
	Ping bool
}

var WorkerBehavior = goactor.Register("mandelbrot.Worker", func(struct{}) goactor.Actor[WorkerMsg] {
	return &Worker{}
})

type Worker struct{}

func (w *Worker) Start(*goactor.ActorContext[WorkerMsg]) {}

func (w *Worker) Stop(*goactor.ActorContext[WorkerMsg], error) {}

func (w *Worker) Receive(ctx *goactor.ActorContext[WorkerMsg], m WorkerMsg) {
	if m.Tile == nil {
		return
	}
	done := TileDone{Index: m.Tile.Index, Pixels: renderTile(*m.Tile), Worker: ctx.SelfRef()}
	if err := m.Tile.ReplyTo.Send(CoordMsg{Done: &done}); err != nil {
		slog.Warn("reply lost", "tile", done.Index, "err", err)
	}
}
