# GONGS - Go Nats Generic Streams

A thin wrapper around Nats Jetstream client that uses Go Generics to provide strongly typed NATS streams.

## Example Usage

Define the Type that will be published and retrieved from the NATS stream:

```go
type ExampleMsgEventData struct {
	Id          string
	Type        string
	Description string
}

type ExampleMsg struct {
	eventData *ExampleMsgEventData
}

// Mandatory - Implement the `gongs.MsgEvent` interface
func (e *ExampleMsg) GetId() string {
	return e.eventData.Id
}

func (e *ExampleMsg) DecodeEventData(b []byte) error {
	d := &ExampleMsgEventData{}
	json.Unmarshal(b, d)
	e.eventData = d
	return nil
}

func (e *ExampleMsg) EncodeEventData() []byte {
	b, _ := json.Marshal(e.eventData)
	return b
}
```

Create Generic Stream for the above type:

```go
	// create Jetstream for Stream
	cfg := &nats.StreamConfig{
		Name:      "EXAMPLE",
		Subjects:  []string{"example.>"},
		Storage:   nats.MemoryStorage,
		Retention: nats.WorkQueuePolicy,
	}
	js, _ := nc.JetStream()
	js.AddStream(cfg)

	// create Generic Stream
	q := gongs.NewGenericStream[ExampleMsg](js, "example.events", cfg.Name)
```

Publish event

```go
	// Publish an event
	q.Publish(&ExampleMsg{
		eventData: &ExampleMsgEventData{
			Id:          "abc123",
			Type:        "start",
			Description: "An important task has started",
		},
	})
```

Read last event off queue

```go
	// Read event from NATS
	event, _ := q.GetLastMsg("example")

	fmt.Printf("Id: %s [%s] - %s",
		event.eventData.Id,
		event.eventData.Type,
		event.eventData.Description,
	)
```