package gongs

import (
	"github.com/nats-io/nats.go"
)

type GenericStream[T any, I MsgEvent[T]] struct {
	js      nats.JetStreamContext
	stream  string
	subject string
}

func NewGenericStream[T any, I MsgEvent[T]](
	js nats.JetStreamContext,
	sub string,
	stream string,
) *GenericStream[T, I] {

	return &GenericStream[T, I]{
		js:      js,
		subject: sub,
		stream:  stream,
	}
}

// Publish will publish a message to nats using a message id returned by MsgEvent.GetId
// The message id is used for deduplication https://docs.nats.io/using-nats/developer/develop_jetstream/model_deep_dive#message-deduplication
func (s *GenericStream[T, I]) Publish(evt I) (*nats.PubAck, error) {
	b := evt.EncodeEventData()

	wId := nats.MsgId(evt.GetId())
	return s.js.Publish(s.subject, b, wId)
}

func (s *GenericStream[T, I]) decodeRawStreamMsg(msg *nats.RawStreamMsg) (*T, error) {
	se := I(new(T))
	err := se.DecodeEventData(msg.Data)
	if err != nil {
		return nil, err
	}
	return (*T)(se), nil
}

func (s *GenericStream[T, I]) decodeMsg(msg *nats.Msg) (*T, error) {
	se := I(new(T))
	err := se.DecodeEventData(msg.Data)
	if err != nil {
		return nil, err
	}
	return (*T)(se), nil
}

func (s *GenericStream[T, I]) GetLastMsg(name string) (*T, error) {
	msg, err := s.js.GetLastMsg(s.stream, s.subject)
	if err != nil {
		return nil, err
	}

	return s.decodeRawStreamMsg(msg)
}

func (s *GenericStream[T, I]) QueueSubscribe(queue string, fn MsgHandlerFunc[T]) (*nats.Subscription, error) {
	return s.QueueSubscribeWithOpts(queue, fn)
}

func (s *GenericStream[T, I]) QueueSubscribeWithOpts(queue string, fn MsgHandlerFunc[T], opts ...nats.SubOpt) (*nats.Subscription, error) {
	sub, err := s.js.QueueSubscribe(s.subject, queue,
		func(msg *nats.Msg) {
			se, err := s.decodeMsg(msg)

			if err != nil {
				// dump msg
				msg.Ack()
				return
			}

			evt := (*T)(se)
			err = fn(evt)
			if err != nil {
				msg.Nak()
			}
			msg.Ack()
		},
		opts...,
	)
	if err != nil {
		return nil, err
	}

	return sub, nil
}
