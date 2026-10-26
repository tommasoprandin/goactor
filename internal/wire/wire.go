// Package wire holds the protobuf definitions of the framework RPCs.
package wire

//go:generate sh -c "protoc --plugin=protoc-gen-go=$(go tool -n protoc-gen-go) --plugin=protoc-gen-go-grpc=$(go tool -n protoc-gen-go-grpc) --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative wire.proto"

import "github.com/tommasoprandin/goactor/internal/ident"

func FromIdent(a ident.ActorID) *ActorID {
	return &ActorID{Node: uint32(a.Node), Epoch: a.Epoch, Seq: a.Seq}
}

// Ident returns the zero id for a nil message.
func (a *ActorID) Ident() ident.ActorID {
	return ident.ActorID{Node: ident.NodeID(a.GetNode()), Epoch: a.GetEpoch(), Seq: a.GetSeq()}
}
