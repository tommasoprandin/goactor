package transport

import (
	"context"
	"log/slog"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/tommasoprandin/goactor/codec"
	"github.com/tommasoprandin/goactor/config"
)

type server struct {
	UnimplementedTransportServer
	self NodeID
	h    Handler
}

// Stream sends its header at once: that is what tells the dialling peer the stream is accepted.
func (s *server) Stream(st Transport_StreamServer) error {
	if err := st.SendHeader(metadata.Pairs(nodeKey, idString(s.self))); err != nil {
		return err
	}
	for {
		f, err := st.Recv()
		if err != nil {
			return err
		}
		for _, b := range f.Envelopes {
			e, err := codec.Decode(b)
			if err != nil {
				slog.Warn("goactor: dropping envelope", "err", err)
				continue
			}
			s.h.Deliver(e)
		}
	}
}

type authenticator struct{ cfg *config.Config }

func (a authenticator) check(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	v := md.Get(nodeKey)
	if len(v) == 1 {
		if id, err := strconv.ParseUint(v[0], 10, 16); err == nil {
			if _, err := a.cfg.Node(NodeID(id)); err == nil {
				return nil
			}
		}
	}
	return status.Errorf(codes.Unauthenticated, "goactor: unknown node %v", v)
}

func (a authenticator) unary(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	if err := a.check(ctx); err != nil {
		return nil, err
	}
	return h(ctx, req)
}

func (a authenticator) stream(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
	if err := a.check(ss.Context()); err != nil {
		return err
	}
	return h(srv, ss)
}
