# WSC

WSC (WebSocket Channel) is a library that can be used to manage
github.com/gorilla/websocket using channels.

It provides 2 main functions:

- `func Connect(ctx context.Context, url string, config Config) (Websocket, *http.Response, error)`
- `func Accept(conn WSConnection, config Config) (Websocket, error)`

The interface `WSConnection` is used to interract with the websocket.

The interface for `wsc.Websocket` is:

```go
type Websocket interface {

	// Reads returns a channel where the incoming frames are published.
	// If nothing pumps the Read() while it is full, new frames will be
	// discarded, unless Blocking mode is on.
	//
	// You can configure the size of the read chan in Config.
	// The default is 64 frames.
	Read() chan Frame

	// Write writes the given Frame into the websocket as text.
	// If the other side of the websocket cannot get all messages
	// while the internal write channel is full, new messages will
	// be discarded, unless Blocking mode is on.
	//
	// You can configure the size of the write chan in Config.
	// The default is 64 messages.
	Write(Frame)

	// Done returns a channel that will return when the connection
	// is closed.
	//
	// The content will be nil for clean disconnection or
	// the error that caused the disconnection. If nothing pumps the
	// Done() channel, the error will be discarded.
	Done() chan error

	// Close closes the websocket.
	//
	// Closing the websocket a second time has no effect.
	// A closed Websocket cannot be reused.
	Close(code int)

	// Error returns a channel that will return errors like
	// read or write discards and other errors that are not
	// terminating the connection.
	//
	// If nothing pumps the Error() channel, the error will be discarded.
	Error() chan error
}
```
