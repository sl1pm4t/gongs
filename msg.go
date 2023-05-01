package gongs

import "context"

type MsgHandlerFunc[T any] func(*T) error

type MsgEvent[T any] interface {
	GetId(ctx context.Context) string
	DecodeEventData(b []byte) error
	EncodeEventData(ctx context.Context) []byte
	*T
}
