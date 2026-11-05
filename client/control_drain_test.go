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

//go:build !frps

package client

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	clientproxy "github.com/fatedier/frp/client/proxy"
	"github.com/fatedier/frp/pkg/auth"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/msg"
	clientplugin "github.com/fatedier/frp/pkg/plugin/client"
	"github.com/fatedier/frp/pkg/proto/wire"
)

type handoffBarrierConn struct {
	net.Conn
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *handoffBarrierConn) LocalAddr() net.Addr {
	c.once.Do(func() { close(c.entered); <-c.release })
	return c.Conn.LocalAddr()
}

func TestControlDrainSerializesAcceptedWorkHandoff(t *testing.T) {
	work, peer := net.Pipe()
	defer work.Close()
	defer peer.Close()
	barrier := &handoffBarrierConn{Conn: work, entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(barrier.release) }) }
	defer release()
	_ = peer.SetDeadline(time.Now().Add(5 * time.Second))
	control, controlPeer := net.Pipe()
	defer control.Close()
	defer controlPeer.Close()
	authRuntime, err := auth.BuildClientAuth(&v1.AuthClientConfig{Method: v1.AuthMethodToken, Token: "test"})
	if err != nil {
		t.Fatal(err)
	}
	ctl, err := NewControl(context.Background(), &SessionContext{
		Common: &v1.ClientCommonConfig{}, RunID: "handoff", Conn: msg.NewConn(control, msg.NewV1ReadWriter(control)), Auth: authRuntime,
		Connector: newMessageConnector(&testConnector{conn: barrier}, wire.ProtocolV1),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ctl.Close()
	accepted := make(chan net.Conn, 1)
	ctl.SetInWorkConnCallback(func(_ *v1.ProxyBaseConfig, conn net.Conn, _ *msg.StartWorkConn) bool { accepted <- conn; return false })
	cfg := &v1.TCPProxyConfig{ProxyBaseConfig: v1.ProxyBaseConfig{Name: "kept", Type: "tcp"}}
	ctl.pm.UpdateAll([]v1.ProxyConfigurer{cfg})
	deadline := time.Now().Add(time.Second)
	for {
		status, ok := ctl.pm.GetProxyStatus("kept")
		if ok && status.Phase == clientproxy.ProxyPhaseWaitStart {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("proxy did not start registration")
		}
		time.Sleep(time.Millisecond)
	}
	if err := ctl.pm.StartProxy("kept", "", ""); err != nil {
		t.Fatal(err)
	}
	serverErr := make(chan error, 1)
	go func() {
		rw := msg.NewV1ReadWriter(peer)
		var request msg.NewWorkConn
		if err := rw.ReadMsgInto(&request); err != nil {
			serverErr <- err
			return
		}
		serverErr <- rw.WriteMsg(&msg.StartWorkConn{ProxyName: "kept"})
	}()
	go ctl.handleReqWorkConn(&msg.ReqWorkConn{})
	select {
	case <-barrier.entered:
	case <-time.After(time.Second):
		t.Fatal("work did not reach synchronous handoff")
	}
	if ctl.updateMu.TryLock() {
		ctl.updateMu.Unlock()
		t.Fatal("accepted work handoff was not serialized with withdrawal")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	drained := make(chan error, 1)
	go func() { drained <- ctl.Drain(ctx) }()
	release()
	var conn net.Conn
	select {
	case conn = <-accepted:
	case <-ctx.Done():
		t.Fatal("accepted work lost during withdrawal")
	}
	defer conn.Close()
	select {
	case err := <-drained:
		t.Fatalf("drain ended with live assigned work: %v", err)
	default:
	}
	sent := make(chan error, 1)
	go func() { _, err := conn.Write([]byte("x")); sent <- err }()
	var payload [1]byte
	if _, err := io.ReadFull(peer, payload[:]); err != nil || payload[0] != 'x' {
		t.Fatalf("accepted work interrupted: %q %v", payload, err)
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	select {
	case err := <-drained:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("drain did not finish after work closed")
	}
}

func TestControlConfigRemovalClosesWorkAndReaddClearsRevocation(t *testing.T) {
	control, peer := net.Pipe()
	defer peer.Close()
	authRuntime, err := auth.BuildClientAuth(&v1.AuthClientConfig{Method: v1.AuthMethodToken, Token: "test"})
	if err != nil {
		t.Fatal(err)
	}
	ctl, err := NewControl(t.Context(), &SessionContext{
		Common: &v1.ClientCommonConfig{}, RunID: "reload", Conn: msg.NewConn(control, msg.NewV1ReadWriter(control)), Auth: authRuntime,
		Connector: &gracefulCloseTestConnector{conn: peer},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ctl.Close()
	cfg := &v1.TCPProxyConfig{ProxyBaseConfig: v1.ProxyBaseConfig{Name: "readded", Type: "tcp"}}
	if err := ctl.UpdateAllConfigurer([]v1.ProxyConfigurer{cfg}, nil); err != nil {
		t.Fatal(err)
	}
	work, workPeer := net.Pipe()
	defer workPeer.Close()
	tracked := msg.NewConn(work, msg.NewV1ReadWriter(work))
	if !ctl.workConnections.track(tracked, "readded") {
		t.Fatal("initial work rejected")
	}
	changed := *cfg
	changed.LocalPort = 10081
	if err := ctl.UpdateAllConfigurer([]v1.ProxyConfigurer{&changed}, nil); err != nil {
		t.Fatal(err)
	}
	sent := make(chan error, 1)
	go func() { _, err := tracked.Write([]byte("x")); sent <- err }()
	_ = workPeer.SetReadDeadline(time.Now().Add(time.Second))
	var payload [1]byte
	if _, err := io.ReadFull(workPeer, payload[:]); err != nil || payload[0] != 'x' {
		t.Fatalf("plain-proxy reload interrupted existing work: %v", err)
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	ctl.CloseProxyConnections("readded")
	sibling := &v1.TCPProxyConfig{ProxyBaseConfig: v1.ProxyBaseConfig{Name: "sibling", Type: "tcp"}}
	if err := ctl.UpdateAllConfigurer([]v1.ProxyConfigurer{&changed, sibling}, nil); err != nil {
		t.Fatal(err)
	}
	late, latePeer := net.Pipe()
	defer latePeer.Close()
	if ctl.workConnections.track(msg.NewConn(late, msg.NewV1ReadWriter(late)), "readded") {
		t.Fatal("unrelated proxy reload cleared revocation")
	}
	_ = latePeer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := latePeer.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("revoked work stayed open after unrelated reload: %v", err)
	}
	if err := ctl.UpdateAllConfigurer(nil, nil); err != nil {
		t.Fatal(err)
	}
	_ = workPeer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := workPeer.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("removed work remained open: %v", err)
	}
	if err := ctl.UpdateAllConfigurer([]v1.ProxyConfigurer{cfg}, nil); err != nil {
		t.Fatal(err)
	}
	fresh, freshPeer := net.Pipe()
	defer freshPeer.Close()
	accepted := msg.NewConn(fresh, msg.NewV1ReadWriter(fresh))
	defer accepted.Close()
	if !ctl.workConnections.track(accepted, "readded") {
		t.Fatal("readded proxy kept stale revocation")
	}
	ctl.CloseProxyConnections("unknown")
	ctl.workConnections.mu.Lock()
	unknownCount := len(ctl.workConnections.revoked)
	ctl.workConnections.mu.Unlock()
	if unknownCount != 0 {
		t.Fatal("unknown proxy grew revocation state")
	}
	if err := ctl.UpdateAllConfigurer(nil, nil); err != nil {
		t.Fatal(err)
	}
	_ = freshPeer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := freshPeer.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("config removal did not release work before plugin teardown: %v", err)
	}
}

type waitingDrainPlugin struct{ entered, done chan struct{} }

func (*waitingDrainPlugin) Name() string { return "test-drain-wait" }
func (p *waitingDrainPlugin) Handle(_ context.Context, info *clientplugin.ConnectionInfo) {
	close(p.entered)
	_, _ = io.Copy(io.Discard, info.Conn)
	_ = info.Conn.Close()
	close(p.done)
}

func (p *waitingDrainPlugin) Close() error {
	select {
	case <-p.entered:
		<-p.done
	default:
	}
	return nil
}

type waitingDrainPluginOptions struct{ plugin *waitingDrainPlugin }

func (*waitingDrainPluginOptions) Complete() {}
func (o *waitingDrainPluginOptions) Clone() v1.ClientPluginOptions {
	return &waitingDrainPluginOptions{plugin: o.plugin}
}

func init() {
	clientplugin.Register("test-drain-wait", func(_ clientplugin.PluginContext, options v1.ClientPluginOptions) (clientplugin.Plugin, error) {
		return options.(*waitingDrainPluginOptions).plugin, nil
	})
}

func TestControlGracefulCloseDeadlineInterruptsWaitingPlugin(t *testing.T) {
	control, peer := net.Pipe()
	defer peer.Close()
	authRuntime, err := auth.BuildClientAuth(&v1.AuthClientConfig{Method: v1.AuthMethodToken, Token: "test"})
	if err != nil {
		t.Fatal(err)
	}
	ctl, err := NewControl(t.Context(), &SessionContext{
		Common: &v1.ClientCommonConfig{}, RunID: "plugin-grace", Conn: msg.NewConn(control, msg.NewV1ReadWriter(control)), Auth: authRuntime,
		Connector: &gracefulCloseTestConnector{conn: peer},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ctl.Close()
	plugin := &waitingDrainPlugin{entered: make(chan struct{}), done: make(chan struct{})}
	cfg := &v1.TCPProxyConfig{ProxyBaseConfig: v1.ProxyBaseConfig{
		Name: "waiting", Type: "tcp",
		ProxyBackend: v1.ProxyBackend{Plugin: v1.TypedClientPluginOptions{Type: "test-drain-wait", ClientPluginOptions: &waitingDrainPluginOptions{plugin: plugin}}},
	}}
	ctl.pm.UpdateAll([]v1.ProxyConfigurer{cfg})
	deadline := time.Now().Add(time.Second)
	for {
		status, ok := ctl.pm.GetProxyStatus("waiting")
		if ok && status.Phase == clientproxy.ProxyPhaseWaitStart {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("plugin proxy did not register")
		}
		time.Sleep(time.Millisecond)
	}
	if err := ctl.pm.StartProxy("waiting", "", ""); err != nil {
		t.Fatal(err)
	}
	work, workPeer := net.Pipe()
	defer workPeer.Close()
	conn := msg.NewConn(work, msg.NewV1ReadWriter(work))
	if !ctl.workConnections.track(conn, "waiting") {
		t.Fatal("initial work rejected")
	}
	ctl.pm.HandleWorkConn("waiting", conn, &msg.StartWorkConn{ProxyName: "waiting"})
	select {
	case <-plugin.entered:
	case <-time.After(time.Second):
		t.Fatal("plugin did not take work")
	}
	closed := make(chan error, 1)
	go func() { closed <- ctl.GracefulClose(20 * time.Millisecond) }()
	_ = workPeer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := workPeer.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("plugin teardown prevented grace deadline: %v", err)
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("plugin close stayed blocked after work EOF")
	}
}
