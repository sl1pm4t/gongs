package gongs_test

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/sl1pm4t/gongs"
	"github.com/sl1pm4t/gongs/test"
)

type TestStreamMsg struct {
	eventData *TestStreamEventData
}

type TestStreamEventData struct {
	Id  int
	Foo string
}

func (e *TestStreamMsg) GetId() string {
	return fmt.Sprintf("%d", e.eventData.Id)
}

func (e *TestStreamMsg) DecodeEventData(b []byte) error {
	d := &TestStreamEventData{}
	err := json.Unmarshal(b, d)
	if err != nil {
		return err
	}
	e.eventData = d
	return nil
}

func (e *TestStreamMsg) EncodeEventData() []byte {
	b, _ := json.Marshal(e.eventData)
	return b
}

func Test_GenericStream_QueueSubscribe(t *testing.T) {
	s := test.RunBasicJetStreamServer()
	defer test.ShutdownJSServerAndRemoveStorage(t, s)

	nc, js := test.JsClient(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:      t.Name(),
		Retention: nats.WorkQueuePolicy,
		Subjects:  []string{"generic.>"},
		Storage:   nats.MemoryStorage,
	}

	_, err := js.AddStream(cfg)
	if err != nil {
		t.Fatalf("could not create stream: %v", err)
	}

	workStream := gongs.NewGenericStream[TestStreamMsg](
		js,
		"generic.test",
		cfg.Name,
	)

	wg := sync.WaitGroup{}
	sub, err := workStream.QueueSubscribe("test-workers", func(evt *TestStreamMsg) error {
		defer wg.Done()
		t.Logf("Test Event: %d - %s\n", evt.eventData.Id, evt.eventData.Foo)
		if evt.eventData.Id == 0 {
			t.Fatalf("test event Id should be positive integer")
		}
		if evt.eventData.Foo != fmt.Sprintf("foo-%d", evt.eventData.Id) {
			t.Fatalf("test event data not decoded correctly")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unable to subscribe: %v \n", err)
	}
	defer sub.Unsubscribe()

	for i := 1; i < 4; i++ {
		wg.Add(1)
		_, err := workStream.Publish(&TestStreamMsg{eventData: &TestStreamEventData{
			Id:  i,
			Foo: fmt.Sprintf("foo-%d", i),
		}})
		if err != nil {
			t.Errorf("could not publish test msg: %v", err)
		}
	}

	wg.Wait()

}
