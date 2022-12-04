package test

import (
	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

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

func JsClient(t *testing.T, s *server.Server, opts ...nats.Option) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()
	nc := client(t, s, opts...)
	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	return nc, js
}

func ShutdownJSServerAndRemoveStorage(t *testing.T, s *server.Server) {
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
