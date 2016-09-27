package lsp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/cmu440/lspnet"
	"math/rand"
	"testing"
	"time"
)

// Message lengths
const (
	SHORT = iota
	NORMAL
	LONG
)

/* Try to read message at server side. */
func (ts *testSystem) serverTryRead(size int, expectedData []byte) {
	var q struct{}
	for {
		ts.t.Logf("server starts to read...")
		_, data, err := ts.server.Read()

		if err != nil {
			ts.t.Fatalf("Server received error during read.")
			return
		}

		switch size {
		case SHORT:
			ts.t.Fatalf("Server received short message: %s", data)
			return
		case LONG:
			ts.exitChan <- q
			if len(data) != len(expectedData) {
				ts.t.Fatalf("Expecting data %s, server received longer message: %s",
					expectedData, data)
			}
		case NORMAL:
			ts.exitChan <- q
			if !bytes.Equal(data, expectedData) {
				ts.t.Fatalf("Expecting %s, server received message: %s",
					expectedData, data)
			}
			return
		}
	}
}

/* Try to read message at client side */
func (ts *testSystem) clientTryRead(size int, expectedData []byte) {
	var q struct{}
	for {
		ts.t.Logf("client starts to read...")
		data, err := ts.clients[0].Read()
		if err != nil {
			ts.t.Fatalf("Client received error during read.")
			return
		}

		switch size {
		case SHORT:
			ts.t.Fatalf("Server received short message!")
			return
		case LONG:
			ts.exitChan <- q
			if len(data) != len(expectedData) {
				ts.t.Fatalf("Expecting shorter data %s, client received longer message: %s",
					expectedData, data)
			}
			return
		case NORMAL:
			ts.exitChan <- q
			if !bytes.Equal(data, expectedData) {
				ts.t.Fatalf("Expecting %s, client received message: %s",
					expectedData, data)
			}
			return
		}
	}
}

func randData() []byte {
	// Random int r: 1000 <= r < 1,000,000
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	writeBytes, _ := json.Marshal(r.Intn(1000) * 1000)
	return writeBytes
}

func (ts *testSystem) serverSend(data []byte) {
	err := ts.server.Write(ts.clients[0].ConnID(), data)
	if err != nil {
		ts.t.Fatalf("Error returned by server.Write(): %s", err)
	}
}

func (ts *testSystem) clientSend(data []byte) {
	err := ts.clients[0].Write(data)
	if err != nil {
		ts.t.Fatalf("Error returned by client.Write(): %s", err)
	}
}

func (ts *testSystem) testServerWithVariableLengthMsg(timeout int) {
	fmt.Printf("=== %s (1 clients, 1 msgs/client, %d%% drop rate, %d window size)\n",
		ts.desc, ts.dropPercent, ts.params.WindowSize)
	data := randData()

	// First, verify that server can read normal length message
	ts.t.Logf("Testing server read with normal length data")
	go ts.serverTryRead(NORMAL, data)
	go ts.clientSend(data)

	timeoutChan := time.After(time.Duration(timeout) * time.Millisecond)
	select {
	case <-timeoutChan:
		ts.t.Fatalf("Server didn't receive any message in %dms", timeout)
	case <-ts.exitChan:
	}

	// Now verify that server truncates long messages
	ts.t.Logf("Testing server read with a long message")
	lspnet.SetMsgLengtheningPercent(100)
	go ts.serverTryRead(LONG, data)
	go ts.clientSend(data)

	timeoutChan = time.After(time.Duration(timeout) * time.Millisecond)
	select {
	case <-timeoutChan:
		ts.t.Fatalf("Server didn't receive any message in %dms", timeout)
	case <-ts.exitChan:
	}
	lspnet.SetMsgLengtheningPercent(0)

	// Last, verify that server doesn't read short messages
	ts.t.Logf("Testing the server with a short messages")
	lspnet.SetMsgShorteningPercent(100)
	defer lspnet.SetMsgShorteningPercent(0)

	go ts.serverTryRead(SHORT, data)
	go ts.clientSend(data)

	// If server does receive any message before timeout, your implementation is correct
	time.Sleep(time.Duration(timeout) * time.Millisecond)
}

func (ts *testSystem) testClientWithVariableLengthMsg(timeout int) {
	fmt.Printf("=== %s (1 clients, 1 msgs/client, %d%% drop rate, %d window size)\n",
		ts.desc, ts.dropPercent, ts.params.WindowSize)
	data := randData()

	// First, verify that client can read normal length message
	ts.t.Logf("Testing client read with normal length data")
	go ts.clientTryRead(NORMAL, data)
	go ts.serverSend(data)

	timeoutChan := time.After(time.Duration(timeout) * time.Millisecond)
	select {
	case <-timeoutChan:
		ts.t.Fatalf("client didn't receive any message in %dms", timeout)
	case <-ts.exitChan:
	}

	// Now verify that client truncates long messages
	ts.t.Logf("Testing client read with a long message")
	lspnet.SetMsgLengtheningPercent(100)
	go ts.clientTryRead(LONG, data)
	go ts.serverSend(data)

	timeoutChan = time.After(time.Duration(timeout) * time.Millisecond)
	select {
	case <-timeoutChan:
		ts.t.Fatalf("Client didn't receive any message in %dms", timeout)
	case <-ts.exitChan:
	}
	lspnet.SetMsgLengtheningPercent(0)

	// Last, verify that client doesn't read short messages
	ts.t.Logf("Testing the client with a short messages")
	lspnet.SetMsgShorteningPercent(100)
	defer lspnet.SetMsgShorteningPercent(0)

	go ts.clientTryRead(SHORT, data)
	go ts.serverSend(data)

	// If client does receive any message before timeout, your implementation is correct
	time.Sleep(time.Duration(timeout) * time.Millisecond)
}

func TestVariableLengthMsgServer(t *testing.T) {
	newTestSystem(t, 1, makeParams(5, 2000, 1)).
		setDescription("TestVariableLengthMsgServer: server should handle variable length messages").
		testServerWithVariableLengthMsg(2000)
}

func TestVariableLengthMsgClient(t *testing.T) {
	newTestSystem(t, 1, makeParams(5, 2000, 1)).
		setDescription("TestVariableLengthMsgClient: client should handle variable length messages").
		testClientWithVariableLengthMsg(2000)
}
