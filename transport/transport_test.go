package transport

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/tommasoprandin/goactor/codec"
	"github.com/tommasoprandin/goactor/config"
	"github.com/tommasoprandin/goactor/internal/ident"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type recorder struct {
	gate chan struct{}
	mu   sync.Mutex
	got  []*codec.Envelope
}

func drop(*codec.Envelope) {}

func (r *recorder) deliver(e *codec.Envelope) {
	if r.gate != nil {
		<-r.gate
	}
	r.mu.Lock()
	r.got = append(r.got, e)
	r.mu.Unlock()
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.got)
}

func (r *recorder) envelopes() []*codec.Envelope {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.got)
}

func cluster(t *testing.T, n int) *config.Config {
	cfg := &config.Config{}
	for i := 1; i <= n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		cfg.Cluster = append(cfg.Cluster, config.NodeConfig{ID: NodeID(i), GRPC: l.Addr().String()})
		l.Close()
	}
	return cfg
}

func start(t *testing.T, cfg *config.Config, id NodeID, h Handler, services ...func(grpc.ServiceRegistrar)) *Transport {
	t.Helper()
	tr := New(cfg, id)
	if err := tr.Start(h, services...); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tr.Close() })
	return tr
}

func registerHealth(r grpc.ServiceRegistrar) {
	healthpb.RegisterHealthServer(r, health.NewServer())
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func envelope(i int) *codec.Envelope {
	payload := binary.BigEndian.AppendUint64(bytes.Repeat([]byte{byte(i)}, i%100), uint64(i))
	return &codec.Envelope{To: ident.ActorID{Node: 2, Epoch: 1, Seq: uint64(i)}, TypeName: "t", Payload: payload}
}

func TestExchange(t *testing.T) {
	const n = 100_000
	cfg := cluster(t, 2)
	var ra, rb recorder
	a := start(t, cfg, 1, ra.deliver)
	b := start(t, cfg, 2, rb.deliver)
	waitFor(t, "streams", func() bool { return a.Epoch(2) == 1 && b.Epoch(1) == 1 })

	var wg sync.WaitGroup
	for from, to := range map[*Transport]NodeID{a: 2, b: 1} {
		wg.Go(func() {
			for i := range n {
				if err := from.Send(to, envelope(i)); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	for _, r := range []*recorder{&ra, &rb} {
		waitFor(t, "delivery", func() bool { return r.count() == n })
		for i, e := range r.envelopes() {
			if !reflect.DeepEqual(e, envelope(i)) {
				t.Fatalf("envelope %d: got %+v", i, e)
			}
		}
	}
}

type countingStream struct {
	grpc.ClientStream
	n *atomic.Int64
}

func (s countingStream) SendMsg(m any) error {
	s.n.Add(1)
	return s.ClientStream.SendMsg(m)
}

func TestBatching(t *testing.T) {
	const n = 1000
	cfg := cluster(t, 2)
	var frames atomic.Int64
	count := grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, open grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		s, err := open(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}
		return countingStream{s, &frames}, nil
	})
	a := New(cfg, 1)
	a.dialOpts = []grpc.DialOption{count}
	if err := a.Start(drop); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close() })
	var rb recorder
	start(t, cfg, 2, rb.deliver)
	waitFor(t, "stream", func() bool { return a.Epoch(2) == 1 })

	for i := range n {
		if err := a.Send(2, envelope(i)); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "delivery", func() bool { return rb.count() == n })
	if f := frames.Load(); f >= n/2 {
		t.Fatalf("%d frames for %d envelopes", f, n)
	}
}

func TestErrors(t *testing.T) {
	cfg := cluster(t, 2)
	a := start(t, cfg, 1, drop)

	if err := a.Send(9, envelope(0)); !errors.Is(err, ErrUnknownNode) {
		t.Fatalf("Send to unconfigured node: %v", err)
	}
	if _, err := a.Conn(9); !errors.Is(err, ErrUnknownNode) {
		t.Fatalf("Conn to unconfigured node: %v", err)
	}
	if err := a.Send(2, envelope(0)); !errors.Is(err, ErrNoRoute) {
		t.Fatalf("Send to down peer: %v", err)
	}
	conn, err := a.Conn(2)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); !errors.Is(err, ErrNoRoute) {
		t.Fatalf("unary call to down peer: %v", err)
	}
}

func TestService(t *testing.T) {
	cfg := cluster(t, 2)
	a := start(t, cfg, 1, drop)
	start(t, cfg, 2, drop, registerHealth)
	waitFor(t, "connection", func() bool { return a.Epoch(2) == 1 })

	conn, err := a.Conn(2)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{})
	if err != nil || resp.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("Check: %v, %v", resp, err)
	}
}

func TestRejectUnknownNode(t *testing.T) {
	cfg := cluster(t, 1)
	start(t, cfg, 1, drop, registerHealth)
	conn, err := grpc.NewClient("passthrough:///"+cfg.Cluster[0].GRPC, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := metadata.AppendToOutgoingContext(t.Context(), nodeKey, "9")

	st, err := NewTransportClient(conn).Stream(ctx)
	if err == nil {
		_, err = st.Recv()
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("stream: %v", err)
	}
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unary: %v", err)
	}
}

// The old receiver stalls, so A still holds queued envelopes when the stream drops. None of them
// may reach the restarted receiver.
func TestReconnect(t *testing.T) {
	const n = 32 << 10 // 1 KiB each, past the 16 MiB HTTP/2 window cap
	cfg := cluster(t, 2)
	a := start(t, cfg, 1, drop)
	old := &recorder{gate: make(chan struct{})}
	b := start(t, cfg, 2, old.deliver)
	waitFor(t, "stream", func() bool { return a.Epoch(2) == 1 })

	big := &codec.Envelope{Payload: make([]byte, 1<<10)}
	for range n {
		if err := a.Send(2, big); err != nil {
			t.Fatal(err)
		}
	}
	go func() {
		for !errors.Is(a.Send(2, big), ErrNoRoute) {
			time.Sleep(time.Millisecond)
		}
		close(old.gate)
	}()
	b.Close()
	if got := old.count(); got >= n {
		t.Fatalf("all %d envelopes delivered before the drop", got)
	}

	fresh := &recorder{}
	start(t, cfg, 2, fresh.deliver)
	waitFor(t, "reconnect", func() bool { return a.Epoch(2) == 2 })
	marker := envelope(7)
	if err := a.Send(2, marker); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "marker", func() bool { return fresh.count() > 0 })
	if got := fresh.envelopes(); len(got) != 1 || !reflect.DeepEqual(got[0], marker) {
		t.Fatalf("restarted receiver got %d envelopes, first %+v", len(got), got[0])
	}
}
