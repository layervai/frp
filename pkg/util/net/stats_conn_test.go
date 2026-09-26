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

package net

import (
	stdnet "net"
	"sync"
	"sync/atomic"
	"testing"
)

// A concurrency-safe immediate connection isolates the wrapper's counters
// from transport timing. net.Conn permits concurrent Read, Write and Close.
type statsTestConn struct{ stdnet.Conn }

func (*statsTestConn) Read(p []byte) (int, error)  { return len(p), nil }
func (*statsTestConn) Write(p []byte) (int, error) { return len(p), nil }
func (*statsTestConn) Close() error                { return nil }

func TestStatsConnConcurrentReadWriteClose(t *testing.T) {
	var callbacks atomic.Int32
	conn := WrapStatsConn(&statsTestConn{}, func(read, written int64) {
		callbacks.Add(1)
		if read < 0 || written < 0 || read > 2000 || written > 2000 {
			t.Errorf("invalid byte-count snapshot: %d, %d", read, written)
		}
	})
	start := make(chan struct{})
	var workers sync.WaitGroup
	for range 2 {
		workers.Go(func() {
			<-start
			for range 1000 {
				_, _ = conn.Read(make([]byte, 1))
				_, _ = conn.Write([]byte{1})
			}
		})
	}
	workers.Go(func() {
		<-start
		for range 1000 {
			_ = conn.Close()
		}
	})
	close(start)
	workers.Wait()
	if callbacks.Load() != 1 {
		t.Fatalf("close callback ran %d times", callbacks.Load())
	}
}

func TestStatsConnCompletedBytesReportedOnce(t *testing.T) {
	calls := 0
	conn := WrapStatsConn(&statsTestConn{}, func(read, written int64) {
		calls++
		if read != 3 || written != 5 {
			t.Errorf("byte counts = %d, %d", read, written)
		}
	})
	_, _ = conn.Read(make([]byte, 3))
	_, _ = conn.Write(make([]byte, 5))
	_ = conn.Close()
	_ = conn.Close()
	if calls != 1 {
		t.Fatalf("close callback ran %d times", calls)
	}
}
