package gongq

type MsgHandlerFunc[T any] func(*T) error

type MsgEvent[T any] interface {
	GetId() string
	DecodeEventData([]byte) error
	EncodeEventData() []byte
	*T
}
