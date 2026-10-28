// Command mandelbrot renders the Mandelbrot set across a goactor cluster. Every node runs the same
// command, differing only in -id. See README.md.
package main

import (
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tommasoprandin/goactor"
	"github.com/tommasoprandin/goactor/config"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfgPath := flag.String("config", "cluster.yaml", "cluster configuration file")
	nodeID := flag.Uint("id", 1, "id of this node in the configuration")
	var p Params
	flag.IntVar(&p.Workers, "workers", 6, "workers across the cluster")
	flag.IntVar(&p.Tiles, "tiles", 256, "tiles, a perfect square")
	flag.IntVar(&p.Size, "size", 4096, "image side in pixels")
	flag.IntVar(&p.MaxIter, "max-iter", 10000, "iterations before a point counts as inside the set")
	flag.StringVar(&p.Out, "out", "mandelbrot.png", "output PNG on the coordinator's node")
	flag.DurationVar(&p.SnapshotInterval, "snapshot-interval", time.Second, "time between progress snapshots, 0 for none")
	flag.DurationVar(&p.TileTimeout, "tile-timeout", time.Minute, "time before a silent worker counts as lost")
	flag.DurationVar(&p.JoinTimeout, "join-timeout", 30*time.Second, "longest wait for every node before rendering")
	flag.Parse()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	if err := p.validate(); err != nil {
		return err
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	p.Nodes = cfg.N()
	id := goactor.NodeID(*nodeID)
	sys, err := goactor.Start(cfg, id)
	if err != nil {
		return err
	}
	defer sys.Shutdown()

	coordinator, created, err := goactor.SpawnNamed(sys.Root(), "coordinator", CoordinatorBehavior, p)
	if err != nil {
		return err
	}
	slog.Info("node up", "node", id, "coordinator", coordinator.ID(), "created_here", created)
	if err := coordinator.Send(CoordMsg{Hello: &Hello{Node: id}}); err != nil {
		return err
	}
	<-sig
	return nil
}
