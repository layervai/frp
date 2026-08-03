// Copyright 2026 The frp Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	pkgerr "github.com/fatedier/frp/pkg/errors"
	"github.com/fatedier/frp/pkg/msg"
	"github.com/fatedier/frp/server/proxy"
	"github.com/fatedier/frp/server/registry"
)

// newTeardownControl builds a started Control whose control connection is one
// end of an in-memory pipe, so closing the returned client end drives the
// dispatcher to completion and runs worker teardown.
func newTeardownControl(t *testing.T) (*Control, net.Conn) {
	t.Helper()
	serverEnd, clientEnd := net.Pipe()
	t.Cleanup(func() {
		serverEnd.Close()
		clientEnd.Close()
	})

	sessionCtx := &SessionContext{
		Conn:           msg.NewConn(serverEnd, msg.NewV1ReadWriter(serverEnd)),
		LoginMsg:       &msg.Login{RunID: "teardown-race-run-id"},
		ServerCfg:      &v1.ServerConfig{},
		ClientRegistry: registry.NewClientRegistry(),
	}
	ctl, err := NewControl(context.Background(), sessionCtx)
	require.NoError(t, err)
	ctl.Start()
	return ctl, clientEnd
}

func newTestWorkConn() *proxy.WorkConn {
	near, far := net.Pipe()
	far.Close()
	return proxy.NewWorkConn(msg.NewConn(near, msg.NewV1ReadWriter(near)))
}

// TestRegisterWorkConnDuringTeardownDoesNotRace races work-connection
// registration against worker teardown closing workConnCh. Registration must
// fail with ErrCtlClosed once teardown wins instead of sending on the closed
// channel (a data race, previously masked by a recover of the resulting
// panic). Run with -race to give the schedule below teeth.
func TestRegisterWorkConnDuringTeardownDoesNotRace(t *testing.T) {
	for range 20 {
		ctl, clientEnd := newTeardownControl(t)

		const senders = 4
		var wg sync.WaitGroup
		startCh := make(chan struct{})
		for range senders {
			wg.Go(func() {
				<-startCh
				for {
					workConn := newTestWorkConn()
					err := ctl.RegisterWorkConn(workConn)
					if err != nil {
						workConn.Close()
					}
					if errors.Is(err, pkgerr.ErrCtlClosed) {
						return
					}
				}
			})
		}
		close(startCh)
		clientEnd.Close()

		done := make(chan struct{})
		go func() {
			ctl.WaitClosed()
			wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("registration goroutines did not observe control teardown")
		}
	}
}

// TestRegisterWorkConnAfterTeardownReturnsClosed pins the sequential
// contract: once the control worker has torn down, registration reports
// ErrCtlClosed and ownership of the connection stays with the caller.
func TestRegisterWorkConnAfterTeardownReturnsClosed(t *testing.T) {
	ctl, clientEnd := newTeardownControl(t)
	clientEnd.Close()
	ctl.WaitClosed()

	workConn := newTestWorkConn()
	defer workConn.Close()
	err := ctl.RegisterWorkConn(workConn)
	require.ErrorIs(t, err, pkgerr.ErrCtlClosed)
}
