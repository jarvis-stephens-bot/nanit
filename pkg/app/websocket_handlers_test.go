package app

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/gregory-m/nanit/pkg/baby"
	"github.com/gregory-m/nanit/pkg/client"
)

// fakeCameraServer is a minimal stand-in for Nanit's camera WebSocket
// endpoint. It records every PUT_STREAMING request it receives and lets the
// test script a response (or no response, to simulate a timeout) per
// request.
type fakeResponse struct {
	statusCode    int32
	statusMessage string
	respond       bool
}

type fakeCameraServer struct {
	server    *httptest.Server
	received  chan *client.Request
	responder func(req *client.Request) fakeResponse
}

func newFakeCameraServer(responder func(req *client.Request) fakeResponse) *fakeCameraServer {
	f := &fakeCameraServer{
		received:  make(chan *client.Request, 16),
		responder: responder,
	}

	upgrader := websocket.Upgrader{}

	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		for {
			msgType, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if msgType != websocket.BinaryMessage {
				continue
			}

			m := &client.Message{}
			if err := proto.Unmarshal(data, m); err != nil {
				continue
			}

			if m.Request == nil {
				continue
			}

			f.received <- m.Request

			fr := f.responder(m.Request)
			if !fr.respond {
				continue
			}

			resp := &client.Response{
				RequestId:   m.Request.Id,
				RequestType: m.Request.Type,
				StatusCode:  &fr.statusCode,
			}
			if fr.statusMessage != "" {
				resp.StatusMessage = &fr.statusMessage
			}

			respMsg := &client.Message{
				Type:     client.Message_Type(client.Message_RESPONSE).Enum(),
				Response: resp,
			}

			bytes, err := proto.Marshal(respMsg)
			if err != nil {
				continue
			}

			if err := conn.WriteMessage(websocket.BinaryMessage, bytes); err != nil {
				return
			}
		}
	}))

	return f
}

func (f *fakeCameraServer) wsURL() string {
	return "ws" + strings.TrimPrefix(f.server.URL, "http")
}

func (f *fakeCameraServer) close() {
	f.server.Close()
	close(f.received)
}

func (f *fakeCameraServer) dial(t *testing.T) *client.WebsocketConnection {
	t.Helper()

	conn, err := client.DialWebsocketConnection(f.wsURL(), 5*time.Second)
	require.NoError(t, err)

	return conn
}

func requireNextRequest(t *testing.T, f *fakeCameraServer, timeout time.Duration) *client.Request {
	t.Helper()
	select {
	case req := <-f.received:
		return req
	case <-time.After(timeout):
		t.Fatal("timed out waiting for camera server to receive a request")
		return nil
	}
}

func TestSendStopStreaming_SendsStoppedStatus(t *testing.T) {
	server := newFakeCameraServer(func(req *client.Request) fakeResponse {
		return fakeResponse{statusCode: 200, respond: true}
	})
	defer server.close()

	conn := server.dial(t)

	sendStopStreaming("baby-1", "rtmp://example/local/baby-1", conn)

	req := requireNextRequest(t, server, 2*time.Second)
	require.NotNil(t, req.Streaming)
	assert.Equal(t, client.Streaming_STOPPED, req.Streaming.GetStatus())
	assert.Equal(t, "rtmp://example/local/baby-1", req.Streaming.GetRtmpUrl())
}

func TestSendStopStreaming_DoesNotBlockOnMissingResponse(t *testing.T) {
	// Camera never responds - sendStopStreaming must return immediately
	// regardless, since live observation found the camera never acks a
	// STOPPED for an already-dead session (see
	// _System/research/nanit-lockout-drops-and-stop-timeout-2026-10-21.md).
	server := newFakeCameraServer(func(req *client.Request) fakeResponse {
		return fakeResponse{respond: false}
	})
	defer server.close()

	conn := server.dial(t)

	done := make(chan struct{})
	go func() {
		sendStopStreaming("baby-1", "rtmp://example/local/baby-1", conn)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("sendStopStreaming blocked waiting for a response it was not supposed to await")
	}

	requireNextRequest(t, server, 2*time.Second)
}

func TestRequestLocalStreaming_SendsStopBeforeRetryingOnForbidden(t *testing.T) {
	const forbiddenMsg = "Forbidden: Number of Mobile App connections above limit, declining connection"

	server := newFakeCameraServer(func(req *client.Request) fakeResponse {
		if req.Streaming.GetStatus() == client.Streaming_STOPPED {
			// Explicit stop always succeeds.
			return fakeResponse{statusCode: 200, respond: true}
		}
		// Every STARTED request is refused as over the connection limit.
		return fakeResponse{statusCode: 403, statusMessage: forbiddenMsg, respond: true}
	})
	defer server.close()

	conn := server.dial(t)
	stateManager := baby.NewStateManager()

	// requestLocalStreaming's Forbidden branch sleeps 300s between retries,
	// far too long for a unit test. We only need to observe the first
	// iteration: STARTED refused -> explicit STOPPED sent -> (sleep begins).
	// Run it in a goroutine and inspect the first two requests the fake
	// server receives, which is enough to prove the STOPPED send happens
	// before the retry/sleep rather than being skipped.
	go requestLocalStreaming("baby-1", "rtmp://example/local/baby-1", client.Streaming_STARTED, conn, stateManager)

	first := requireNextRequest(t, server, 2*time.Second)
	require.NotNil(t, first.Streaming)
	assert.Equal(t, client.Streaming_STARTED, first.Streaming.GetStatus(), "first request should be the normal start request")

	second := requireNextRequest(t, server, 2*time.Second)
	require.NotNil(t, second.Streaming)
	assert.Equal(t, client.Streaming_STOPPED, second.Streaming.GetStatus(), "after a Forbidden refusal, an explicit STOPPED must be sent before the retry sleep")
}
