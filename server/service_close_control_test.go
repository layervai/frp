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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/fatedier/frp/pkg/auth"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/msg"
	plugin "github.com/fatedier/frp/pkg/plugin/server"
	"github.com/fatedier/frp/server/controller"
	"github.com/fatedier/frp/server/proxy"
	"github.com/fatedier/frp/server/registry"
)

type closeControlAddr string

func (a closeControlAddr) Network() string { return string(a) }
func (a closeControlAddr) String() string  { return string(a) }

// closeControlConn is a control transport whose Read blocks until Close, so a
// started Control stays in controlStateRunning until something closes it.
//
// It deliberately has no blocking-Close mode. On the v0.71.0 lifecycle a Close
// that blocks inside the transport holds ctl.lifecycleMu, and installing a
// same-run replacement needs that same mutex via Replaced(), so the two
// serialize: the "replacement lands mid-close" interleaving that the v0.68.1
// version of this file constructed now deadlocks by construction.
type closeControlConn struct {
	closeCalls atomic.Int32
	readGate   chan struct{}
	closeOnce  sync.Once
}

func newCloseControlConn() *closeControlConn {
	return &closeControlConn{readGate: make(chan struct{})}
}

func (c *closeControlConn) Read([]byte) (int, error) {
	<-c.readGate
	return 0, net.ErrClosed
}
func (c *closeControlConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *closeControlConn) LocalAddr() net.Addr              { return closeControlAddr("local") }
func (c *closeControlConn) RemoteAddr() net.Addr             { return closeControlAddr("remote") }
func (c *closeControlConn) SetDeadline(time.Time) error      { return nil }
func (c *closeControlConn) SetReadDeadline(time.Time) error  { return nil }
func (c *closeControlConn) SetWriteDeadline(time.Time) error { return nil }

func (c *closeControlConn) Close() error {
	call := c.closeCalls.Add(1)
	c.closeOnce.Do(func() { close(c.readGate) })
	if call > 1 {
		return net.ErrClosed
	}
	return nil
}

// startCloseControl brings a control all the way to controlStateRunning, which
// is what ControlManager.GetByID (and therefore Service.CloseControl) requires
// on the v0.71.0 lifecycle.
func startCloseControl(t *testing.T, manager *ControlManager, runID string, conn net.Conn) *Control {
	t.Helper()
	ctl, err := NewControl(context.Background(), &SessionContext{
		RC:            &controller.ResourceController{},
		PxyManager:    proxy.NewManager(),
		PluginManager: plugin.NewManager(),
		AuthVerifier:  auth.AlwaysPassVerifier,
		Conn:          msg.NewConn(conn, msg.NewV1ReadWriter(conn)),
		LoginMsg:      &msg.Login{RunID: runID, ClientID: runID},
		ServerCfg:     &v1.ServerConfig{},
	})
	require.NoError(t, err)
	ctl.serverMetrics = newCountingServerMetrics()
	require.NoError(t, manager.Add(ctl))
	active, err := manager.Activate(ctl)
	require.NoError(t, err)
	require.True(t, active)
	require.True(t, ctl.Start())
	return ctl
}

func TestServiceCloseControlAbsentOrFinishedReturnsFalse(t *testing.T) {
	manager := NewControlManager(registry.NewClientRegistry())
	service := &Service{ctlManager: manager}
	require.False(t, service.CloseControl("missing"), "CloseControl(missing)")

	conn := newCloseControlConn()
	ctl := startCloseControl(t, manager, "finished", conn)
	require.NoError(t, ctl.Close())
	waitForControlDone(t, ctl)
	before := conn.closeCalls.Load()

	require.False(t, service.CloseControl("finished"), "CloseControl(finished)")
	require.Equal(t, before, conn.closeCalls.Load(),
		"CloseControl must not touch the transport of a finished control")
}

// CloseControl acts on the exact current generation for a run ID. Installing a
// same-run replacement retires the previous control; a subsequent CloseControl
// closes the replacement and does not re-close the retired generation.
//
// On v0.71.0 the generation fence is structural: GetByID resolves the current
// entry under the run gate and requires controlStateRunning, so a retired
// control can never be handed to a caller.
func TestServiceCloseControlClosesExactCurrentGenerationAcrossReplacement(t *testing.T) {
	manager := NewControlManager(registry.NewClientRegistry())
	service := &Service{ctlManager: manager}

	oldConn := newCloseControlConn()
	oldCtl := startCloseControl(t, manager, "same-run", oldConn)

	newConn := newCloseControlConn()
	newCtl := startCloseControl(t, manager, "same-run", newConn)
	require.NotSame(t, oldCtl, newCtl)

	// The replacement retires the previous generation and becomes current.
	waitForControlDone(t, oldCtl)
	retiredCloses := oldConn.closeCalls.Load()
	require.GreaterOrEqual(t, retiredCloses, int32(1), "retired transport closed by replacement")

	got, ok := manager.GetByID("same-run")
	require.True(t, ok)
	require.Same(t, newCtl, got, "replacement is the current manager entry")

	require.True(t, service.CloseControl("same-run"), "CloseControl(current generation)")
	waitForControlDone(t, newCtl)
	require.GreaterOrEqual(t, newConn.closeCalls.Load(), int32(1), "current transport closed")
	require.Equal(t, retiredCloses, oldConn.closeCalls.Load(),
		"CloseControl must not re-close the retired generation")

	require.False(t, service.CloseControl("same-run"),
		"CloseControl on a finished current generation reports false")
}

// A CloseControl queued before a same-run replacement closes whichever control
// is current when it actually runs: one runID is one outer authorization
// cycle, so a same-run reconnect stays part of the stale cycle.
func TestServiceCloseControlQueuedBeforeSameRunReplacementClosesCurrentControl(t *testing.T) {
	manager := NewControlManager(registry.NewClientRegistry())
	service := &Service{ctlManager: manager}
	oldConn := newCloseControlConn()
	startCloseControl(t, manager, "same-run", oldConn)

	queued := make(chan struct{})
	execute := make(chan struct{})
	result := make(chan bool, 1)
	go func() {
		close(queued)
		<-execute
		result <- service.CloseControl("same-run")
	}()
	<-queued

	newConn := newCloseControlConn()
	startCloseControl(t, manager, "same-run", newConn)
	close(execute)

	select {
	case got := <-result:
		require.True(t, got, "CloseControl(queued stale cycle)")
	case <-time.After(5 * time.Second):
		t.Fatal("queued CloseControl did not return")
	}
	// Installing the replacement closes the old control; the queued call closes
	// the new one. Both transports end up closed exactly once by that path.
	require.GreaterOrEqual(t, oldConn.closeCalls.Load(), int32(1), "replaced transport closed")
	require.GreaterOrEqual(t, newConn.closeCalls.Load(), int32(1), "current transport closed")
}

func TestServiceCloseControlWorkerEmitsExactCloseProxy(t *testing.T) {
	const (
		runID     = "0123456789abcdef"
		proxyName = "proxy-exact"
		attemptID = "0123456789abcdef0123456789abcdef"
	)
	delivered := make(chan plugin.CloseProxyContent, 1)
	pluginManager := plugin.NewManager()
	pluginManager.Register(&controlResultPlugin{handle: func(op string, content any) (*plugin.Response, any, error) {
		if op != plugin.OpCloseProxy {
			return &plugin.Response{Unchange: true}, nil, nil
		}
		delivered <- content.(plugin.CloseProxyContent)
		return &plugin.Response{Unchange: true}, nil, nil
	}})
	t.Cleanup(pluginManager.Close)

	conn := newCloseControlConn()
	manager := NewControlManager(registry.NewClientRegistry())
	ctl, err := NewControl(context.Background(), &SessionContext{
		RC:            &controller.ResourceController{},
		PxyManager:    proxy.NewManager(),
		PluginManager: pluginManager,
		AuthVerifier:  auth.AlwaysPassVerifier,
		Conn:          msg.NewConn(conn, msg.NewV1ReadWriter(conn)),
		LoginMsg:      &msg.Login{RunID: runID, ClientID: runID},
		ServerCfg:     &v1.ServerConfig{},
	})
	require.NoError(t, err)
	ctl.serverMetrics = newCountingServerMetrics()
	ctl.proxies[proxyName] = newRegisteredProxy(
		newCloseResultTestProxy(proxyName),
		plugin.UserInfo{RunID: runID},
		attemptID,
	)
	require.NoError(t, manager.Add(ctl))
	active, err := manager.Activate(ctl)
	require.NoError(t, err)
	require.True(t, active)
	require.True(t, ctl.Start())

	service := &Service{ctlManager: manager}
	require.True(t, service.CloseControl(runID), "CloseControl(active worker)")
	waitForControlDone(t, ctl)

	select {
	case got := <-delivered:
		require.Equal(t, runID, got.User.RunID)
		require.Equal(t, proxyName, got.ProxyName)
		require.Equal(t, attemptID, got.AttemptID)
	case <-time.After(5 * time.Second):
		t.Fatal("control worker did not emit CloseProxy callback")
	}
}

func TestServiceCloseControlAlreadyClosedConnectionStillSignalsActiveControl(t *testing.T) {
	manager := NewControlManager(registry.NewClientRegistry())
	service := &Service{ctlManager: manager}
	conn := newCloseControlConn()
	ctl := startCloseControl(t, manager, "closed", conn)

	// Close the transport underneath a still-running control. CloseControl
	// reports on control liveness, not on a second transport Close result.
	require.NoError(t, conn.Close())

	got := service.CloseControl("closed")
	if got {
		require.True(t, errors.Is(conn.Close(), net.ErrClosed), "transport retains closed state")
		return
	}
	// If the worker already observed the transport failure and published
	// doneCh, CloseControl correctly reports false for a finished control.
	waitForControlDone(t, ctl)
}
