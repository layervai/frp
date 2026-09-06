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
	"sync"
	"testing"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/msg"
	"github.com/fatedier/frp/pkg/util/xlog"
)

type closingProxy struct{}

func (closingProxy) Run() error { return nil }
func (closingProxy) InWorkConn(conn net.Conn, _ *msg.StartWorkConn) {
	_ = conn.Close()
}

func (closingProxy) SetInWorkConnCallback(func(*v1.ProxyBaseConfig, net.Conn, *msg.StartWorkConn) bool) {
}
func (closingProxy) Close() {}

func TestWrapperInWorkConnSynchronizesPhase(t *testing.T) {
	wrapper := &Wrapper{
		WorkingStatus: WorkingStatus{Phase: ProxyPhaseRunning},
		pxy:           closingProxy{},
		xl:            xlog.New(),
	}
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		for range 1000 {
			wrapper.mu.Lock()
			wrapper.Phase = ProxyPhaseClosed
			wrapper.mu.Unlock()
		}
	}()
	go func() {
		defer workers.Done()
		for range 1000 {
			conn, peer := net.Pipe()
			wrapper.InWorkConn(conn, &msg.StartWorkConn{})
			_ = peer.Close()
		}
	}()
	workers.Wait()
}
