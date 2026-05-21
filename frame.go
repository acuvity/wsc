package wsc

import "github.com/gorilla/websocket"

// Frame represents a single WebSocket frame exchanged between client and server.
// It encapsulates the message payload along with any associated metadata required
// for transmission and processing within the wsc package.
type Frame struct {
	D []byte
	T int
}

// Empty reports whether the frame contains no payload data.
func (f *Frame) Empty() bool {
	return len(f.D) == 0
}

// Binary reports whether the frame's payload is binary data.
// It returns true for binary frames and false for text or control frames.
func (f *Frame) Binary() bool {
	return f.T == websocket.BinaryMessage
}

// Text reports whether the frame's payload is a UTF-8 encoded text message.
// It returns true for text frames and false for binary or control frames.
func (f *Frame) Text() bool {
	return f.T == websocket.TextMessage
}

// Control reports whether the frame is a control frame (close, ping, or pong)
// as defined by RFC 6455. Control frames are identified by an opcode with the
// high bit set and are used to communicate state about the WebSocket connection.
func (f *Frame) Control() bool {
	return f.T == websocket.CloseMessage || f.T == websocket.PingMessage || f.T == websocket.PongMessage
}

// TextFrame creates and returns a new Frame of type TextMessage
// containing the provided data payload.
func TextFrame(data []byte) Frame {
	return Frame{
		D: data,
		T: websocket.TextMessage,
	}
}

// BinaryFrame creates and returns a new Frame containing the given binary
// payload, with its type set to BinaryFrame.
func BinaryFrame(data []byte) Frame {
	return Frame{
		D: data,
		T: websocket.BinaryMessage,
	}
}

// EmptyTextFrame creates and returns a new Frame initialized as an empty text frame.
func EmptyTextFrame() Frame {
	return TextFrame([]byte{})
}

// EmptyBinaryFrame returns a new Frame initialized as a binary frame with an empty payload.
func EmptyBinaryFrame() Frame {
	return BinaryFrame([]byte{})
}
