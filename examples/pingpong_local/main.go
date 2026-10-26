// Command pingpong_local runs two actors on a single node that pass a ball back and forth.
//
//	cd examples/pingpong_local
//	go run . -config cluster.yaml -id 1 -n 10
package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/tommasoprandin/goactor"
	"github.com/tommasoprandin/goactor/config"
)

type Ball struct {
	N    int
	From goactor.Ref[Ball]
}

type PlayerArgs struct {
	Name string
	Done chan struct{}
}

type Player struct {
	PlayerArgs
}

func (p *Player) Start(ctx *goactor.ActorContext[Ball]) {
	fmt.Printf("%s started as %v\n", p.Name, ctx.Self())
}

func (p *Player) Receive(ctx *goactor.ActorContext[Ball], b Ball) {
	fmt.Printf("%s <- ball %d from %v\n", p.Name, b.N, b.From.ID())
	if b.N == 0 {
		close(p.Done)
		return
	}
	if err := b.From.Send(Ball{N: b.N - 1, From: ctx.SelfRef()}); err != nil {
		fmt.Printf("%s: send: %v\n", p.Name, err)
	}
}

func (p *Player) Stop(ctx *goactor.ActorContext[Ball], err error) {
	fmt.Printf("%s stopped (err=%v)\n", p.Name, err)
}

var PlayerBehavior = goactor.Register("pingpong.Player", func(a PlayerArgs) goactor.Actor[Ball] {
	return &Player{a}
})

func main() {
	cfgPath := flag.String("config", "cluster.yaml", "cluster configuration file")
	nodeID := flag.Uint("id", 1, "id of this node in the configuration")
	rounds := flag.Int("n", 10, "number of passes")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	self := goactor.NodeID(*nodeID)
	sys, err := goactor.Start(cfg, self)
	if err != nil {
		log.Fatal(err)
	}
	defer sys.Shutdown()

	done := make(chan struct{})
	ping, err := goactor.Spawn(sys.Root(), PlayerBehavior, PlayerArgs{Name: "ping", Done: done}, goactor.WithNode(self))
	if err != nil {
		log.Fatal(err)
	}
	pong, err := goactor.Spawn(sys.Root(), PlayerBehavior, PlayerArgs{Name: "pong", Done: done}, goactor.WithNode(self))
	if err != nil {
		log.Fatal(err)
	}
	if err := ping.Send(Ball{N: *rounds, From: pong}); err != nil {
		log.Fatal(err)
	}
	<-done
}
