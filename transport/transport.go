// Package transport connects the nodes of a cluster over gRPC. It carries envelopes on one stream
// per ordered node pair and hosts the framework's unary services.
package transport

//go:generate sh -c "protoc --plugin=protoc-gen-go=$(go tool -n protoc-gen-go) --plugin=protoc-gen-go-grpc=$(go tool -n protoc-gen-go-grpc) --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative transport.proto"

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"sync"

	"google.golang.org/grpc"

	"github.com/tommasoprandin/goactor/codec"
	"github.com/tommasoprandin/goactor/config"
	"github.com/tommasoprandin/goactor/internal/ident"
)

type NodeID = ident.NodeID

var (
	ErrNoRoute     = errors.New("goactor: no route")
	ErrUnknownNode = config.ErrUnknownNode
)

// Handler receives inbound envelopes. It runs concurrently across streams and in envelope order
// within one; it must not block.
type Handler func(e *codec.Envelope)

type Transport struct {
	cfg      *config.Config
	self     NodeID
	dialOpts []grpc.DialOption
	peers    map[NodeID]*peer
	srv      *grpc.Server
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	once     sync.Once
	err      error
}

// New returns the transport of node self. It holds no resources until Start.
func New(cfg *config.Config, self NodeID) *Transport {
	return &Transport{cfg: cfg, self: self, peers: make(map[NodeID]*peer)}
}

// Start listens on self's grpc address, registers services on the server before it starts serving,
// and dials every other configured node in the background. A failed Start leaves nothing to Close.
func (t *Transport) Start(h Handler, services ...func(grpc.ServiceRegistrar)) error {
	me, err := t.cfg.Node(t.self)
	if err != nil {
		return err
	}
	auth := authenticator{t.cfg}
	t.srv = grpc.NewServer(
		grpc.ChainUnaryInterceptor(auth.unary),
		grpc.ChainStreamInterceptor(auth.stream),
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.WaitForHandlers(true),
	)
	RegisterTransportServer(t.srv, &server{self: t.self, h: h})
	for _, register := range services {
		register(t.srv)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.cancel = cancel
	for _, n := range t.cfg.Cluster {
		if n.ID == t.self {
			continue
		}
		p, err := dial(t.self, n, t.dialOpts)
		if err != nil {
			t.Close()
			return err
		}
		t.peers[n.ID] = p
	}
	lis, err := net.Listen("tcp", me.GRPC)
	if err != nil {
		t.Close()
		return err
	}
	t.wg.Add(1 + len(t.peers))
	go func() {
		defer t.wg.Done()
		t.srv.Serve(lis)
	}()
	for _, p := range t.peers {
		go func() {
			defer t.wg.Done()
			p.run(ctx)
		}()
	}
	return nil
}

// Send queues e for to and returns at once. A nil error means e was accepted, not delivered.
func (t *Transport) Send(to NodeID, e *codec.Envelope) error {
	p, err := t.peer(to)
	if err != nil {
		return err
	}
	b, err := codec.Encode(e)
	if err != nil {
		return err
	}
	return p.enqueue(b)
}

// Conn returns the connection to to, for building generated clients. Unary calls on it fail with
// ErrNoRoute while the connection is not ready.
func (t *Transport) Conn(to NodeID) (grpc.ClientConnInterface, error) {
	p, err := t.peer(to)
	if err != nil {
		return nil, err
	}
	return p.conn, nil
}

// Epoch counts the streams established to to so far. FIFO holds only within one epoch.
func (t *Transport) Epoch(to NodeID) uint64 {
	p, ok := t.peers[to]
	if !ok {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.epoch
}

// Close terminates every stream and waits for the transport's goroutines to exit.
func (t *Transport) Close() error {
	t.once.Do(func() {
		if t.srv == nil {
			return
		}
		t.cancel()
		t.srv.Stop()
		t.wg.Wait()
		for _, p := range t.peers {
			t.err = errors.Join(t.err, p.conn.Close())
		}
	})
	return t.err
}

func (t *Transport) peer(to NodeID) (*peer, error) {
	if p, ok := t.peers[to]; ok {
		return p, nil
	}
	if to == t.self {
		return nil, ErrNoRoute
	}
	return nil, fmt.Errorf("%w: %d", ErrUnknownNode, to)
}
