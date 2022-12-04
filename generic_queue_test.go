package gongq

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

type TestQueueMsg struct {
	eventData *TestQueueEventData
}

type TestQueueEventData struct {
	Id  int
	Foo string
}

func (e *TestQueueMsg) GetId() string {
	return fmt.Sprintf("%d", e.eventData.Id)
}

func (e *TestQueueMsg) DecodeEventData(b []byte) error {
	d := &TestQueueEventData{}
	err := json.Unmarshal(b, d)
	if err != nil {
		return err
	}
	e.eventData = d
	return nil
}

func (e *TestQueueMsg) EncodeEventData() []byte {
	b, _ := json.Marshal(e.eventData)
	return b
}

func Test_GenericQueue_QueueSubscribe(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
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

	workStream := NewGenericQueue[TestQueueMsg](
		js,
		"generic.test",
		cfg.Name,
	)

	wg := sync.WaitGroup{}

	sub, err := workStream.QueueSubscribe("test-workers", func(evt *TestQueueMsg) error {
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
		_, err := workStream.Publish(&TestQueueMsg{eventData: &TestQueueEventData{
			Id:  i,
			Foo: fmt.Sprintf("foo-%d", i),
		}})
		if err != nil {
			t.Errorf("could not publish test msg: %v", err)
		}
	}

	wg.Wait()

}

////////////////////////////////////////////////////////////////////////////////
// Test Helpers - temp NATS server support
////////////////////////////////////////////////////////////////////////////////

// RunDefaultServer will run a server on the default port.
func RunDefaultServer() *server.Server {
	return RunServerOnPort(nats.DefaultPort)
}

// RunServerOnPort will run a server on the given port.
func RunServerOnPort(port int) *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = port
	opts.Cluster.Name = "testing"
	return RunServerWithOptions(opts)
}

// RunServerWithOptions will run a server with the given options.
func RunServerWithOptions(opts server.Options) *server.Server {
	return natsserver.RunServer(&opts)
}

// RunServerWithConfig will run a server with the given configuration file.
func RunServerWithConfig(configFile string) (*server.Server, *server.Options) {
	return natsserver.RunServerWithConfig(configFile)
}

func RunBasicJetStreamServer() *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	return RunServerWithOptions(opts)
}

func createConfFile(t *testing.T, content []byte) string {
	t.Helper()
	conf, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatalf("Error creating conf file: %v", err)
	}
	fName := conf.Name()
	conf.Close()
	if err := ioutil.WriteFile(fName, content, 0666); err != nil {
		os.Remove(fName)
		t.Fatalf("Error writing conf file: %v", err)
	}
	return fName
}

func client(t *testing.T, s *server.Server, opts ...nats.Option) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL(), opts...)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	return nc
}

func jsClient(t *testing.T, s *server.Server, opts ...nats.Option) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()
	nc := client(t, s, opts...)
	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	return nc, js
}

func shutdownJSServerAndRemoveStorage(t *testing.T, s *server.Server) {
	t.Helper()
	var sd string
	if config := s.JetStreamConfig(); config != nil {
		sd = config.StoreDir
	}
	s.Shutdown()
	if sd != "" {
		if err := os.RemoveAll(sd); err != nil {
			t.Fatalf("Unable to remove storage %q: %v", sd, err)
		}
	}
	s.WaitForShutdown()
}
