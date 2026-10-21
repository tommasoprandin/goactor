package transport

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/tommasoprandin/goactor/config"
)

const (
	nodeKey       = "goactor-node"
	maxFrameBytes = 1 << 20
	minBackoff    = 100 * time.Millisecond
	maxBackoff    = 5 * time.Second
)

type peer struct {
	id     NodeID
	conn   *grpc.ClientConn
	client TransportClient

	mu    sync.Mutex
	sess  *session
	epoch uint64
}

// session is the outbound queue of one stream. It dies with the stream, so nothing queued is ever
// replayed onto the next one.
type session struct {
	buf    [][]byte
	notify chan struct{}
}

func dial(self NodeID, n config.NodeConfig, extra []grpc.DialOption) (*peer, error) {
	c := claim(idString(self))
	opts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.Config{
			BaseDelay: minBackoff, Multiplier: 1.6, Jitter: 0.2, MaxDelay: maxBackoff,
		}}),
		grpc.WithDisableRetry(),
		grpc.WithChainUnaryInterceptor(c.unary),
		grpc.WithChainStreamInterceptor(c.stream),
	}, extra...)
	conn, err := grpc.NewClient("passthrough:///"+n.GRPC, opts...)
	if err != nil {
		return nil, err
	}
	conn.Connect()
	return &peer{id: n.ID, conn: conn, client: NewTransportClient(conn)}, nil
}

func (p *peer) run(ctx context.Context) {
	delay := minBackoff
	for {
		if p.connect(ctx) {
			delay = minBackoff
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		delay = min(2*delay, maxBackoff)
	}
}

// connect runs one stream until it breaks and reports whether it was established.
func (p *peer) connect(ctx context.Context) bool {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	st, err := p.client.Stream(ctx, grpc.WaitForReady(true))
	if err != nil {
		return false
	}
	if err := p.handshake(st); err != nil {
		slog.Warn("goactor: stream rejected", "peer", p.id, "err", err)
		return false
	}

	s := &session{notify: make(chan struct{}, 1)}
	p.mu.Lock()
	p.sess = s
	p.epoch++
	p.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		st.Recv()
		cancel()
	}()
	p.write(ctx, st, s)

	p.mu.Lock()
	p.sess = nil
	p.mu.Unlock()
	cancel()
	<-done
	return true
}

// handshake waits for the peer to accept the stream and checks it is the node we dialled.
func (p *peer) handshake(st Transport_StreamClient) error {
	md, _ := st.Header()
	if md == nil {
		_, err := st.Recv()
		return err
	}
	if got := md.Get(nodeKey); len(got) != 1 || got[0] != idString(p.id) {
		return fmt.Errorf("goactor: node %d answered as %v", p.id, got)
	}
	return nil
}

func (p *peer) write(ctx context.Context, st Transport_StreamClient, s *session) {
	for {
		select {
		case <-s.notify:
		case <-ctx.Done():
			return
		}
		p.mu.Lock()
		batch := s.buf
		s.buf = nil
		p.mu.Unlock()
		for len(batch) > 0 {
			n := frameLen(batch)
			if st.Send(&Frame{Envelopes: batch[:n]}) != nil {
				return
			}
			batch = batch[n:]
		}
	}
}

// frameLen returns how many leading envelopes fit in one frame, at least one.
func frameLen(batch [][]byte) int {
	n, size := 1, len(batch[0])
	for n < len(batch) && size+len(batch[n]) <= maxFrameBytes {
		size += len(batch[n])
		n++
	}
	return n
}

func (p *peer) enqueue(b []byte) error {
	p.mu.Lock()
	s := p.sess
	if s == nil {
		p.mu.Unlock()
		return ErrNoRoute
	}
	s.buf = append(s.buf, b)
	p.mu.Unlock()
	select {
	case s.notify <- struct{}{}:
	default:
	}
	return nil
}

// claim is the node id a client presents on every call.
type claim string

func (c claim) unary(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	// A fail-fast call still blocks while the connection is being established.
	if cc.GetState() != connectivity.Ready {
		return ErrNoRoute
	}
	return invoke(metadata.AppendToOutgoingContext(ctx, nodeKey, string(c)), method, req, reply, cc, opts...)
}

func (c claim) stream(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, open grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return open(metadata.AppendToOutgoingContext(ctx, nodeKey, string(c)), desc, cc, method, opts...)
}

func idString(id NodeID) string {
	return strconv.FormatUint(uint64(id), 10)
}
