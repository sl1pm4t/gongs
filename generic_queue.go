package gongq

import (
	"github.com/nats-io/nats.go"
)

type GenericQueue[T any, I MsgEvent[T]] struct {
	js      nats.JetStreamContext
	stream  string
	subject string
}

func NewGenericQueue[T any, I MsgEvent[T]](
	js nats.JetStreamContext,
	sub string,
	stream string,
) *GenericQueue[T, I] {

	return &GenericQueue[T, I]{
		js:      js,
		subject: sub,
		stream:  stream,
	}
}

func (h *GenericQueue[T, I]) Publish(evt I) (*nats.PubAck, error) {
	b := evt.EncodeEventData()

	wId := nats.MsgId(evt.GetId())
	return h.js.Publish(h.subject, b, wId)
}

func (h *GenericQueue[T, I]) decodeMsg(msg *nats.RawStreamMsg) (*T, error) {
	se := I(new(T))
	err := se.DecodeEventData(msg.Data)
	if err != nil {
		return nil, err
	}
	return (*T)(se), nil
}

func (h *GenericQueue[T, I]) GetLastMsg(name string) (*T, error) {
	msg, err := h.js.GetLastMsg(h.stream, h.subject)
	if err != nil {
		return nil, err
	}

	return h.decodeMsg(msg)
}

func (h *GenericQueue[T, I]) QueueSubscribe(queue string, fn MsgHandlerFunc[T]) (*nats.Subscription, error) {
	sub, err := h.js.QueueSubscribe(h.subject, queue,
		func(msg *nats.Msg) {
			se := I(new(T))
			// invoke msg type to decode
			err := se.DecodeEventData(msg.Data)
			if err != nil {
				// dump msg
				msg.Ack()
			}
			evt := (*T)(se)
			err = fn(evt)
			if err != nil {
				msg.Nak()
			}
			msg.Ack()
		},
	)
	if err != nil {
		return nil, err
	}

	return sub, nil
}
