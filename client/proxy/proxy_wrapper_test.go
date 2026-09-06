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

package proxy

import (
	"net"
	"runtime"
	"sync/atomic"
	"testing"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/msg"
	"github.com/fatedier/frp/pkg/util/xlog"
)

type closingProxy struct{ calls atomic.Int64 }

func (*closingProxy) Run() error { return nil }
func (p *closingProxy) InWorkConn(conn net.Conn, _ *msg.StartWorkConn) {
	p.calls.Add(1)
	_ = conn.Close()
}

func (*closingProxy) SetInWorkConnCallback(func(*v1.ProxyBaseConfig, net.Conn, *msg.StartWorkConn) bool) {
}
func (*closingProxy) Close() {}

func TestWrapperInWorkConnSynchronizesPhase(t *testing.T) {
	proxy := &closingProxy{}
	wrapper := &Wrapper{
		WorkingStatus: WorkingStatus{Phase: ProxyPhaseRunning},
		pxy:           proxy,
		xl:            xlog.New(),
	}
	checkDispatch := func(phase string, want int64) {
		t.Helper()
		wrapper.Phase = phase
		conn, peer := net.Pipe()
		wrapper.InWorkConn(conn, &msg.StartWorkConn{})
		var buffer [1]byte
		_, _ = peer.Read(buffer[:])
		_ = peer.Close()
		if got := proxy.calls.Load(); got != want {
			t.Fatalf("phase %q dispatched %d work connections, want %d", phase, got, want)
		}
	}
	checkDispatch(ProxyPhaseRunning, 1)
	checkDispatch(ProxyPhaseClosed, 1)

	done := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		phase := ProxyPhaseClosed
		for {
			select {
			case <-done:
				return
			default:
			}
			wrapper.mu.Lock()
			wrapper.Phase = phase
			wrapper.mu.Unlock()
			if phase == ProxyPhaseClosed {
				phase = ProxyPhaseRunning
			} else {
				phase = ProxyPhaseClosed
			}
			runtime.Gosched()
		}
	}()
	defer func() {
		close(done)
		<-writerDone
	}()
	for range 1000 {
		conn, peer := net.Pipe()
		wrapper.InWorkConn(conn, &msg.StartWorkConn{})
		var buffer [1]byte
		_, _ = peer.Read(buffer[:])
		_ = peer.Close()
	}
}
