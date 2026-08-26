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
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fatedier/frp/pkg/msg"
	"github.com/fatedier/frp/server/proxy"
	"github.com/fatedier/frp/server/registry"
)

func newTestWorkConn() *proxy.WorkConn {
	near, far := net.Pipe()
	far.Close()
	return proxy.NewWorkConn(msg.NewConn(near, msg.NewV1ReadWriter(near)))
}

// TestRegisterWorkConnDuringTeardownDoesNotRace races work-connection
// registration against the control worker closing workConnCh.
//
// Control.worker closes ctl.workConnCh during teardown. A sender that reaches
// the channel after that close panics with "send on closed channel", which is
// a data race the -race detector reports as chansend vs close. Upstream
// fatedier/frp#5424 fenced this with the control lifecycle state machine:
// worker moves state out of controlStateRunning under lifecycleMu before the
// close, and ControlManager.RegisterWorkConn only sends while holding
// lifecycleMu with state == controlStateRunning, so a sender can never observe
// Running once the close is reachable.
//
// This pins that ordering. Run with -race to give the schedule teeth.
func TestRegisterWorkConnDuringTeardownDoesNotRace(t *testing.T) {
	for range 20 {
		manager := NewControlManager(registry.NewClientRegistry())
		metrics := newCountingServerMetrics()
		ctl, conn := newLifecycleTestControl(t, "teardown-race", "teardown-race", metrics)
		mustAddAndActivate(t, manager, ctl)
		require.True(t, ctl.Start())
		waitForSignal(t, conn.readStarted, "control reader to start")

		closed := make(chan struct{})
		go func() {
			ctl.WaitClosed()
			close(closed)
		}()

		const senders = 4
		var wg sync.WaitGroup
		startCh := make(chan struct{})
		for range senders {
			wg.Go(func() {
				<-startCh
				for {
					select {
					case <-closed:
						return
					default:
					}
					workConn := newTestWorkConn()
					if err := manager.RegisterWorkConn(ctl, workConn); err != nil {
						workConn.Close()
					}
				}
			})
		}
		close(startCh)
		require.NoError(t, ctl.Close())
		wg.Wait()
		waitForControlDone(t, ctl)
	}
}

// TestRegisterWorkConnAfterTeardownIsRejected pins the sequential contract:
// once the worker has torn the control down, registration fails and ownership
// of the connection stays with the caller.
func TestRegisterWorkConnAfterTeardownIsRejected(t *testing.T) {
	manager := NewControlManager(registry.NewClientRegistry())
	metrics := newCountingServerMetrics()
	ctl, conn := newLifecycleTestControl(t, "after-teardown", "after-teardown", metrics)
	mustAddAndActivate(t, manager, ctl)
	require.True(t, ctl.Start())
	waitForSignal(t, conn.readStarted, "control reader to start")

	require.NoError(t, ctl.Close())
	waitForControlDone(t, ctl)

	workConn := newTestWorkConn()
	defer workConn.Close()
	require.Error(t, manager.RegisterWorkConn(ctl, workConn))
}
