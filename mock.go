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
	"context"
	"fmt"
)

// A MockWebsocket is a utility to write unit
// tests on websockets.
type MockWebsocket interface {
	NextRead(data Frame)
	LastWrite() chan Frame
	NextDone(err error)

	Websocket
}

type mockWebsocket struct {
	readChan  chan Frame
	writeChan chan Frame
	doneChan  chan error
	errChan   chan error
	cancel    context.CancelFunc
}

// NewMockWebsocket returns a mocked Websocket that can be used
// in unit tests.
func NewMockWebsocket(ctx context.Context) MockWebsocket {

	_, cancel := context.WithCancel(ctx)

	return &mockWebsocket{
		readChan:  make(chan Frame, 64),
		writeChan: make(chan Frame, 64),
		doneChan:  make(chan error, 64),
		errChan:   make(chan error, 1),
		cancel:    cancel,
	}
}

func (s *mockWebsocket) Write(f Frame)         { s.writeChan <- f }
func (s *mockWebsocket) Read() chan Frame      { return s.readChan }
func (s *mockWebsocket) Done() chan error      { return s.doneChan }
func (s *mockWebsocket) Close(code int)        { s.doneChan <- fmt.Errorf("%d", code) }
func (s *mockWebsocket) NextRead(data Frame)   { s.readChan <- data }
func (s *mockWebsocket) LastWrite() chan Frame { return s.writeChan }
func (s *mockWebsocket) NextDone(err error)    { s.doneChan <- err }
func (s *mockWebsocket) Error() chan error     { return s.errChan }
