package gongs_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/nats-io/nats.go"
	"github.com/sl1pm4t/gongs"
	"github.com/sl1pm4t/gongs/test"
)

func init() {
	s := test.RunBasicJetStreamServer()
	os.Setenv("NATS_URL", s.ClientURL())
}

type ExampleMsgEventData struct {
	Id          string
	Type        string
	Description string
}
type ExampleMsg struct {
	eventData *ExampleMsgEventData
}

func (e *ExampleMsg) GetId(ctx context.Context) string {
	return e.eventData.Id
}

func (e *ExampleMsg) DecodeEventData(b []byte) error {
	d := &ExampleMsgEventData{}
	json.Unmarshal(b, d)
	e.eventData = d
	return nil
}

func (e *ExampleMsg) EncodeEventData(ctx context.Context) []byte {
	b, _ := json.Marshal(e.eventData)
	return b
}

func Example() {
	// Get NATS connection
	nc, _ := nats.Connect(os.Getenv("NATS_URL"))

	// create Jetstream for Stream
	cfg := &nats.StreamConfig{
		Name:      "EXAMPLE",
		Subjects:  []string{"example.>"},
		Storage:   nats.MemoryStorage,
		Retention: nats.WorkQueuePolicy,
	}
	js, _ := nc.JetStream()
	js.AddStream(cfg)
	ctx := context.Background()

	// create Generic Stream
	q := gongs.NewGenericStream[ExampleMsg](js, "example.events", cfg.Name)

	// Publish an event
	q.Publish(ctx, &ExampleMsg{
		eventData: &ExampleMsgEventData{
			Id:          "abc123",
			Type:        "start",
			Description: "An important task has started",
		},
	})

	// Read event from NATS
	event, _ := q.GetLastMsg("example")

	fmt.Printf("Id: %s [%s] - %s",
		event.eventData.Id,
		event.eventData.Type,
		event.eventData.Description,
	)

	// Output:
	// Id: abc123 [start] - An important task has started
}
