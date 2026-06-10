// Copyright 2019 Aporeto Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wsc

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	. "github.com/smartystreets/goconvey/convey"
)

type fakeWSConnection struct {
	readDeadlineError  error
	writeDeadlineError error
	readMessageError   error
	writeMessageError  error
	writeControlError  error
	closeError         error

	pongHandler func(string) error
}

func (c *fakeWSConnection) SetReadDeadline(time.Time) error           { return c.readDeadlineError }
func (c *fakeWSConnection) SetWriteDeadline(time.Time) error          { return c.writeDeadlineError }
func (c *fakeWSConnection) SetCloseHandler(func(int, string) error)   {}
func (c *fakeWSConnection) SetPongHandler(h func(string) error)       { c.pongHandler = h }
func (c *fakeWSConnection) ReadMessage() (int, []byte, error)         { return 0, nil, c.readMessageError }
func (c *fakeWSConnection) WriteMessage(int, []byte) error            { return c.writeMessageError }
func (c *fakeWSConnection) WriteControl(int, []byte, time.Time) error { return c.writeControlError }
func (c *fakeWSConnection) Close() error                              { return c.closeError }

func waitClose(s Websocket, code int) { // nolint: unparam

	s.Close(code)

	var cerr error

	select {

	case cerr = <-s.Done():

	case <-time.After(10 * time.Second):
		cerr = fmt.Errorf("did not close in time")
	}

	So(cerr, ShouldBeNil)

	// give a bit of time for everything to settle down.
	time.Sleep(300 * time.Millisecond)

	So(s.(*ws).readPumpClosed, ShouldBeTrue)
	So(s.(*ws).writePumpClosed, ShouldBeTrue)
}

func echoServer(ctx context.Context) *httptest.Server {

	var upgrader = websocket.Upgrader{
		CheckOrigin: func(_ *http.Request) bool { return true },
	}

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		s, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			panic(err)
		}

		h, err := Accept(s, Config{})
		if err != nil {
			panic(err)
		}

		for {
			select {
			case d := <-h.Read():

				if bytes.EqualFold(d.D, []byte("die")) {
					return
				}

				if bytes.EqualFold(d.D, []byte("brutal-close")) {
					_ = s.Close()
					return
				}

				if bytes.EqualFold(d.D, []byte("gentle-close")) {
					h.Close(websocket.CloseGoingAway)
					return
				}

				if bytes.EqualFold(d.D, []byte("delay")) {
					time.Sleep(time.Second)
				}

				h.Write(d)

			case <-ctx.Done():
				return
			}
		}
	}))
}

func TestWSC_ReadWrite(t *testing.T) {

	Convey("Given I have a webserver that works", t, func() {

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		ts := echoServer(ctx)
		defer ts.Close()

		s, resp, err := Connect(
			ctx,
			strings.Replace(ts.URL, "http://", "ws://", 1),
			Config{},
		)
		defer func() { _ = resp.Body.Close() }()

		So(err, ShouldBeNil)
		So(resp, ShouldNotBeNil)
		So(resp.Status, ShouldEqual, "101 Switching Protocols")

		s.Write(TextFrame([]byte("hello")))
		msg := <-s.Read()

		So(string(msg.D), ShouldEqual, "hello")
		So(msg.T, ShouldEqual, websocket.TextMessage)

		waitClose(s, 0)
	})
}

func TestWSC_ReadWriteBlocking(t *testing.T) {

	Convey("Given I have a webserver that works", t, func() {

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		ts := echoServer(ctx)
		defer ts.Close()

		s, resp, err := Connect(
			ctx,
			strings.Replace(ts.URL, "http://", "ws://", 1),
			Config{
				Blocking:      true,
				ReadChanSize:  1,
				WriteChanSize: 1,
			},
		)
		defer func() { _ = resp.Body.Close() }()

		So(err, ShouldBeNil)
		So(resp, ShouldNotBeNil)
		So(resp.Status, ShouldEqual, "101 Switching Protocols")

		go func() {
			s.Write(TextFrame([]byte("hello1")))
			s.Write(TextFrame([]byte("hello2")))
			s.Write(TextFrame([]byte("hello3")))
		}()

		// since this is blocking and the size of chan is 1,
		// if it was non blocking, we would discard some messages.
		msg1 := <-s.Read()
		msg2 := <-s.Read()
		msg3 := <-s.Read()

		So(string(msg1.D), ShouldEqual, "hello1")
		So(msg1.T, ShouldEqual, websocket.TextMessage)

		So(string(msg2.D), ShouldEqual, "hello2")
		So(msg2.T, ShouldEqual, websocket.TextMessage)

		So(string(msg3.D), ShouldEqual, "hello3")
		So(msg3.T, ShouldEqual, websocket.TextMessage)

		waitClose(s, 0)
	})
}

func TestWSC_ReadFull(t *testing.T) {

	Convey("Given I have a webserver that works", t, func() {

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		ts := echoServer(ctx)
		defer ts.Close()

		Convey("When I connect to the webserver", func() {

			s, resp, _ := Connect(
				ctx,
				strings.Replace(ts.URL, "http://", "ws://", 1),
				Config{
					WriteChanSize: 1,
				},
			)
			defer func() { _ = resp.Body.Close() }()

			s.Write(TextFrame([]byte("hello")))
			s.Write(TextFrame([]byte("hello")))
			s.Write(TextFrame([]byte("hello")))
			s.Write(TextFrame([]byte("hello")))
			s.Write(TextFrame([]byte("hello")))

			var err error
			select {
			case err = <-s.Error():
			case <-time.After(2 * time.Second):
				panic("did not receive error in time")
			}

			So(err, ShouldNotBeNil)
			So(err, ShouldEqual, ErrWriteMessageDiscarded)

			waitClose(s, 0)
		})
	})
}

func TestWSC_ConnectToServerWithHTTPError(t *testing.T) {

	Convey("Given I have a webserver that returns an http error", t, func() {

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "nope man", http.StatusForbidden)
		}))
		defer ts.Close()

		Convey("When I connect to the webserver", func() {

			ws, resp, err := Connect(ctx, strings.Replace(ts.URL, "http://", "ws://", 1), Config{})
			defer func() { _ = resp.Body.Close() }()

			So(ws, ShouldBeNil)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "websocket: bad handshake")
			So(resp, ShouldNotBeNil)
			So(resp.Status, ShouldEqual, "403 Forbidden")
		})
	})
}

func TestWSC_CannotConnect(t *testing.T) {

	Convey("Given I have a no webserver", t, func() {

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		Convey("When I connect to the non existing server", func() {

			ws, resp, err := Connect(ctx, "ws://127.0.0.1:7745", Config{})
			defer func() {
				if resp != nil {
					_ = resp.Body.Close()
				}
			}()

			So(ws, ShouldBeNil)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEndWith, "connection refused")
			So(resp, ShouldBeNil)
		})
	})
}

func TestWSC_GentleServerDisconnection(t *testing.T) {

	Convey("Given I have a webserver", t, func() {

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		ts := echoServer(ctx)
		defer ts.Close()

		Convey("When I connect to the webserver", func() {

			ws, resp, _ := Connect(ctx, strings.Replace(ts.URL, "http://", "ws://", 1), Config{})
			defer func() { _ = resp.Body.Close() }()

			// tell the echo server to close ws gently
			ws.Write(TextFrame([]byte("gentle-close")))

			Convey("When I wait for a message", func() {

				var err error
				select {
				case err = <-ws.Done():
				case <-ws.Read():
					panic("test: should not have received message")
				case <-ctx.Done():
					panic("test: no response in time")
				}

				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "unable to read message: websocket: close 1001 (going away)")

				waitClose(ws, 0)
			})
		})
	})
}

func TestWSC_BrutalServerDisconnection(t *testing.T) {

	Convey("Given I have a webserver", t, func() {

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		ts := echoServer(ctx)
		defer ts.Close()

		Convey("When I connect to the webserver", func() {

			ws, resp, _ := Connect(ctx, strings.Replace(ts.URL, "http://", "ws://", 1), Config{})
			defer func() { _ = resp.Body.Close() }()

			// tell the echo server to close ws brutally
			ws.Write(TextFrame([]byte("brutal-close")))

			Convey("When I wait for a message", func() {

				var err error
				select {
				case err = <-ws.Done():
				case <-ws.Read():
					panic("test: should not have received message")
				case <-ctx.Done():
					panic("test: no response in time")
				}

				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "unable to read message: websocket: close 1006 (abnormal closure): unexpected EOF")

				waitClose(ws, 0)
			})
		})
	})
}

func TestWSC_GentleClientDisconnection(t *testing.T) {

	Convey("Given I have a webserver", t, func() {

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		var upgrader = websocket.Upgrader{
			CheckOrigin: func(_ *http.Request) bool { return true },
		}

		rcvmsg := make(chan Frame)
		rcvdone := make(chan error)

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			ws, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				panic(err)
			}

			h, err := Accept(ws, Config{})
			if err != nil {
				panic(err)
			}

			select {
			case err = <-h.Done():
				rcvdone <- err
			case msg := <-h.Read():
				rcvmsg <- msg
			case <-ctx.Done():
				panic("test: no response in time")
			}

		}))
		defer ts.Close()

		Convey("When I connect to the webserver", func() {

			ws, resp, _ := Connect(ctx, strings.Replace(ts.URL, "http://", "ws://", 1), Config{})
			defer func() { _ = resp.Body.Close() }()

			ws.Close(websocket.CloseInvalidFramePayloadData)

			var err error
			var msg Frame
			select {
			case err = <-rcvdone:
			case msg = <-rcvmsg:
			case <-time.After(1 * time.Second):
				panic("test: no response in time")
			}

			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "unable to read message: websocket: close 1007 (invalid payload data)")
			So(msg, ShouldBeZeroValue)

			waitClose(ws, 0)
		})
	})
}

func TestWSC_BrutalClientDisconnection(t *testing.T) {

	Convey("Given I have a webserver", t, func() {

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		var upgrader = websocket.Upgrader{
			CheckOrigin: func(_ *http.Request) bool { return true },
		}

		rcvmsg := make(chan Frame)
		rcvdone := make(chan error)

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			ws, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				panic(err)
			}

			h, err := Accept(ws, Config{})
			if err != nil {
				panic(err)
			}

			select {
			case err = <-h.Done():
				rcvdone <- err
			case msg := <-h.Read():
				rcvmsg <- msg
			case <-ctx.Done():
				panic("test: no response in time")
			}
		}))
		defer ts.Close()

		Convey("When I connect to the webserver", func() {

			w, resp, _ := Connect(ctx, strings.Replace(ts.URL, "http://", "ws://", 1), Config{})
			defer func() { _ = resp.Body.Close() }()

			w.(*ws).conn.Close() // nolint: errcheck

			var err error
			var msg Frame
			select {
			case err = <-rcvdone:
			case msg = <-rcvmsg:
			case <-ctx.Done():
				panic("test: no response in time")
			}

			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "unable to read message: websocket: close 1006 (abnormal closure): unexpected EOF")
			So(msg, ShouldBeZeroValue)

			So(w.(*ws).readPumpClosed, ShouldBeTrue)
			So(w.(*ws).writePumpClosed, ShouldBeTrue)
		})
	})
}

func TestWSC_ServerMissingPong(t *testing.T) {

	Convey("Given I have a webserver", t, func() {

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		ts := echoServer(ctx)
		defer ts.Close()

		Convey("When I connect to the webserver", func() {

			s, resp, _ := Connect(
				ctx, strings.Replace(ts.URL, "http://", "ws://", 1), Config{
					PongWait:   1 * time.Nanosecond, // we wait for nothing
					PingPeriod: 50 * time.Millisecond,
				},
			)
			defer func() { _ = resp.Body.Close() }()

			Convey("When I wait for a message", func() {

				<-time.After(300 * time.Millisecond)

				var err error
				var msg Frame
				select {
				case err = <-s.Done():
				case msg = <-s.Read():
				case <-ctx.Done():
					panic("test: no response in time")
				}

				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEndWith, "i/o timeout")
				So(msg, ShouldBeZeroValue)

				So(s.(*ws).readPumpClosed, ShouldBeTrue)
				So(s.(*ws).writePumpClosed, ShouldBeTrue)
			})
		})
	})
}

func TestWSC_ClientMissingPong(t *testing.T) {

	Convey("Given I have a webserver", t, func() {

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		var upgrader = websocket.Upgrader{
			CheckOrigin: func(_ *http.Request) bool { return true },
		}

		rcvmsg := make(chan Frame)
		rcvdone := make(chan error)

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			ws, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				panic(err)
			}

			h, err := Accept(ws, Config{
				PongWait:   1 * time.Millisecond,
				PingPeriod: 50 * time.Millisecond,
			})
			if err != nil {
				panic(err)
			}

			select {
			case err = <-h.Done():
				rcvdone <- err
			case msg := <-h.Read():
				rcvmsg <- msg
			case <-ctx.Done():
				panic("test: no response in time")
			}

		}))
		defer ts.Close()

		Convey("When I connect to the webserver", func() {

			s, resp, _ := Connect(ctx, strings.Replace(ts.URL, "http://", "ws://", 1), Config{})
			defer func() { _ = resp.Body.Close() }()

			Convey("When I wait for a message", func() {

				<-time.After(300 * time.Millisecond)

				var err error
				var msg Frame
				select {
				case err = <-rcvdone:
				case msg = <-rcvmsg:
				case <-ctx.Done():
					panic("test: no response in time")
				}

				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEndWith, "i/o timeout")
				So(msg, ShouldBeZeroValue)

				So(s.(*ws).readPumpClosed, ShouldBeTrue)
				So(s.(*ws).writePumpClosed, ShouldBeTrue)
			})
		})
	})
}

func TestWWS_AcceptWithFailedReadDeadline(t *testing.T) {

	Convey("Given I have a wsconn", t, func() {

		conn := &fakeWSConnection{
			readDeadlineError: fmt.Errorf("failed"),
		}

		Convey("When I call Accept", func() {

			ws, err := Accept(conn, Config{})

			So(err, ShouldEqual, conn.readDeadlineError)
			So(ws, ShouldBeNil)
		})
	})
}

func TestWSC_writePumpWithWriteErrorForPing(t *testing.T) {

	Convey("Given i have wsconn and ws with a running write pump", t, func() {

		conn := &fakeWSConnection{
			writeMessageError: fmt.Errorf("failed"),
		}

		subctx, cancel := context.WithCancel(t.Context())

		s := &ws{
			conn:     conn,
			doneChan: make(chan error, 1),
			ctx:      subctx,
			cancel:   cancel,
			config: Config{
				PingPeriod: 1 * time.Millisecond,
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		errCh := make(chan error)
		go func() {
			select {
			case e := <-s.Done():
				errCh <- e
			case <-ctx.Done():
				panic("did not receive the expected error in time")
			}
		}()

		go s.writePump(ctx)

		Convey("When I read the errors", func() {

			Convey("Then the error should be correct", func() {
				So((<-errCh).Error(), ShouldEqual, "unable to write ping message: "+conn.writeMessageError.Error())
			})
		})
	})
}

func TestWSC_writePumpWithWriteErrorForWrite(t *testing.T) {

	Convey("Given i have wsconn and ws with a running write pump", t, func() {

		conn := &fakeWSConnection{
			writeMessageError: fmt.Errorf("failed"),
		}

		subctx, cancel := context.WithCancel(t.Context())

		s := &ws{
			conn:      conn,
			doneChan:  make(chan error, 1),
			writeChan: make(chan Frame, 2),
			ctx:       subctx,
			cancel:    cancel,
			config: Config{
				PingPeriod: 10 * time.Millisecond,
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		errCh := make(chan error)
		go func() {
			select {
			case e := <-s.Done():
				errCh <- e
			case <-ctx.Done():
				panic("did not receive the expected error in time")
			}
		}()

		go s.writePump(ctx)

		s.writeChan <- Frame{}
		Convey("When I read the errors", func() {

			Convey("Then the error should be correct", func() {
				So((<-errCh).Error(), ShouldEqual, "unable to write message: "+conn.writeMessageError.Error())
			})
		})
	})
}

func TestWSC_PongHandlerWithError(t *testing.T) {

	Convey("Given I have a wsconn", t, func() {

		conn := &fakeWSConnection{}

		Convey("When I call Accept", func() {

			_, _ = Accept(conn, Config{})

			err := conn.pongHandler("hello")

			So(err, ShouldBeNil)
		})
	})
}
