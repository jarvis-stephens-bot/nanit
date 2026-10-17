package client

import (
	"time"

	"github.com/sacOO7/gowebsocket"
	"google.golang.org/protobuf/proto"
)

// DialWebsocketConnection connects to wsURL and returns a ready
// *WebsocketConnection, wired the same way WebsocketConnectionManager.run
// wires a real camera connection (OnBinaryMessage -> handleMessage). It
// exists so tests (in this package and others) can exercise SendRequest's
// real request/response correlation against a fake WebSocket server instead
// of a live camera.
func DialWebsocketConnection(wsURL string, connectTimeout time.Duration) (*WebsocketConnection, error) {
	connectedC := make(chan error, 1)

	socket := gowebsocket.New(wsURL)

	var conn *WebsocketConnection

	socket.OnConnected = func(gowebsocket.Socket) {
		connectedC <- nil
	}
	socket.OnConnectError = func(err error, _ gowebsocket.Socket) {
		connectedC <- err
	}
	socket.OnBinaryMessage = func(data []byte, _ gowebsocket.Socket) {
		m := &Message{}
		if err := proto.Unmarshal(data, m); err != nil {
			return
		}
		if conn != nil {
			conn.handleMessage(m)
		}
	}

	socket.Connect()

	select {
	case err := <-connectedC:
		if err != nil {
			return nil, err
		}
	case <-time.After(connectTimeout):
		return nil, errTimeout("timed out connecting to " + wsURL)
	}

	conn = NewWebsocketConnection(&socket)
	return conn, nil
}

type errTimeout string

func (e errTimeout) Error() string { return string(e) }
