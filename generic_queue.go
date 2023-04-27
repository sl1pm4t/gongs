package gongs

import (
	"context"

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
func (s *GenericStream[T, I]) Publish(ctx context.Context, evt I) (*nats.PubAck, error) {
	b := evt.EncodeEventData(ctx)

	wId := nats.MsgId(evt.GetId(ctx))
	return s.js.Publish(s.subject, b, wId)
}

func (s *GenericStream[T, I]) decodeRawStreamMsg(ctx context.Context, msg *nats.RawStreamMsg) (*T, error) {
	se := I(new(T))
	err := se.DecodeEventData(ctx, msg.Data)
	if err != nil {
		return nil, err
	}
	return (*T)(se), nil
}

func (s *GenericStream[T, I]) decodeMsg(ctx context.Context, msg *nats.Msg) (*T, error) {
	se := I(new(T))
	err := se.DecodeEventData(ctx, msg.Data)
	if err != nil {
		return nil, err
	}
	return (*T)(se), nil
}

func (s *GenericStream[T, I]) GetLastMsg(ctx context.Context, name string) (*T, error) {
	msg, err := s.js.GetLastMsg(s.stream, s.subject)
	if err != nil {
		return nil, err
	}

	return s.decodeRawStreamMsg(ctx, msg)
}

func (s *GenericStream[T, I]) QueueSubscribe(ctx context.Context, queue string, fn MsgHandlerFunc[T]) (*nats.Subscription, error) {
	sub, err := s.js.QueueSubscribe(s.subject, queue,
		func(msg *nats.Msg) {
			se, err := s.decodeMsg(ctx, msg)

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
