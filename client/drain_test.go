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

package client

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/fatedier/frp/pkg/msg"
	"github.com/fatedier/frp/pkg/proto/wire"
	netutil "github.com/fatedier/frp/pkg/util/net"
)

func TestWorkConnDrainWaitsForClose(t *testing.T) {
	var tracker workConnTracker
	conn, peer := net.Pipe()
	defer peer.Close()
	tracked := msg.NewConn(conn, msg.NewReadWriter(conn, wire.ProtocolV1))
	if !tracker.track(tracked, "test") {
		t.Fatal("initial work rejected")
	}
	tracker.beginDrain()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := tracker.wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("live work drain = %v, want deadline", err)
	}
	var callers sync.WaitGroup
	for range 8 {
		callers.Go(func() { _ = tracked.Close() })
	}
	callers.Wait()
	if err := tracker.wait(context.Background()); err != nil {
		t.Fatalf("idle drain = %v", err)
	}
}

func TestWorkConnDrainRejectsLateAssignment(t *testing.T) {
	var tracker workConnTracker
	tracker.beginDrain()
	conn, peer := net.Pipe()
	defer peer.Close()
	if tracker.track(msg.NewConn(conn, msg.NewReadWriter(conn, wire.ProtocolV1)), "test") {
		t.Fatal("draining control accepted new work")
	}
	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := peer.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("rejected work not closed: %v", err)
	}
}

func TestWorkConnDrainReportsForcedClose(t *testing.T) {
	var tracker workConnTracker
	conn, peer := net.Pipe()
	defer peer.Close()
	_ = tracker.track(msg.NewConn(conn, msg.NewReadWriter(conn, wire.ProtocolV1)), "test")
	tracker.beginDrain()
	tracker.closeAll()
	if err := tracker.wait(context.Background()); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("forced drain = %v, want closed", err)
	}
	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := peer.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("forced work not closed: %v", err)
	}
}

func TestWorkConnDrainPreservesNegotiatedCodec(t *testing.T) {
	for _, protocol := range []string{wire.ProtocolV1, wire.ProtocolV2} {
		t.Run(protocol, func(t *testing.T) {
			var tracker workConnTracker
			conn, peer := net.Pipe()
			defer conn.Close()
			defer peer.Close()
			tracked := msg.NewConn(conn, msg.NewReadWriter(conn, protocol))
			if !tracker.track(tracked, "test") {
				t.Fatal("initial work rejected")
			}
			server := msg.NewConn(peer, msg.NewReadWriter(peer, protocol))
			sent := make(chan error, 1)
			go func() { sent <- tracked.WriteMsg(&msg.Ping{Timestamp: 123}) }()
			received, err := server.ReadMsg()
			if err != nil {
				t.Fatal(err)
			}
			ping, ok := received.(*msg.Ping)
			if !ok || ping.Timestamp != 123 {
				t.Fatalf("negotiated codec changed: %T", received)
			}
			if err := <-sent; err != nil {
				t.Fatal(err)
			}
			tracker.beginDrain()
			_ = tracked.Close()
			if err := tracker.wait(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWorkConnDrainRevokesOneProxyWithoutClosingSibling(t *testing.T) {
	var tracker workConnTracker
	connA, peerA := net.Pipe()
	defer peerA.Close()
	connB, peerB := net.Pipe()
	defer peerB.Close()
	a := msg.NewConn(connA, msg.NewReadWriter(connA, wire.ProtocolV1))
	b := msg.NewConn(connB, msg.NewReadWriter(connB, wire.ProtocolV1))
	if !tracker.track(a, "a") || !tracker.track(b, "b") {
		t.Fatal("initial work rejected")
	}
	tracker.beginDrain()
	tracker.closeProxy("a")
	_ = peerA.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := peerA.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("revoked proxy not closed: %v", err)
	}
	sent := make(chan error, 1)
	go func() { _, err := b.Write([]byte("x")); sent <- err }()
	_ = peerB.SetReadDeadline(time.Now().Add(time.Second))
	var payload [1]byte
	if _, err := io.ReadFull(peerB, payload[:]); err != nil || payload[0] != 'x' {
		t.Fatalf("sibling work interrupted: %q %v", payload, err)
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	_ = b.Close()
	if err := tracker.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWorkConnDrainWakesAllWaiters(t *testing.T) {
	var tracker workConnTracker
	conn, peer := net.Pipe()
	defer peer.Close()
	tracked := msg.NewConn(conn, msg.NewReadWriter(conn, wire.ProtocolV1))
	if !tracker.track(tracked, "test") {
		t.Fatal("initial work rejected")
	}
	tracker.beginDrain()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	results := make(chan error, 8)
	for range 8 {
		go func() { results <- tracker.wait(ctx) }()
	}
	_ = tracked.Close()
	for range 8 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}

func TestWorkConnDrainPreservesConnectionContext(t *testing.T) {
	var tracker workConnTracker
	conn, peer := net.Pipe()
	defer peer.Close()
	base := netutil.NewContextConn(context.Background(), conn)
	tracked := msg.NewConn(base, msg.NewReadWriter(base, wire.ProtocolV1))
	if !tracker.track(tracked, "test") {
		t.Fatal("initial work rejected")
	}
	defer tracked.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	tracked.WithContext(ctx)
	if tracked.Context() != ctx || base.Context() != ctx {
		t.Fatal("work connection context was lost")
	}
}

func TestWorkConnRevocationRejectsLateNameBeforeDrain(t *testing.T) {
	var tracker workConnTracker
	tracker.closeProxy("revoked")
	rejected, rejectedPeer := net.Pipe()
	defer rejectedPeer.Close()
	if tracker.track(msg.NewConn(rejected, msg.NewReadWriter(rejected, wire.ProtocolV1)), "revoked") {
		t.Fatal("revoked proxy accepted late work")
	}
	_ = rejectedPeer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := rejectedPeer.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("rejected work remained open: %v", err)
	}
	sibling, siblingPeer := net.Pipe()
	defer siblingPeer.Close()
	accepted := msg.NewConn(sibling, msg.NewReadWriter(sibling, wire.ProtocolV1))
	if !tracker.track(accepted, "sibling") {
		t.Fatal("revocation blocked sibling assignment")
	}
	_ = accepted.Close()
}

func TestWorkConnRevocationClearsOnConfigUpdate(t *testing.T) {
	var tracker workConnTracker
	tracker.closeProxy("readded")
	tracker.clearRevocations()
	conn, peer := net.Pipe()
	defer peer.Close()
	tracked := msg.NewConn(conn, msg.NewReadWriter(conn, wire.ProtocolV1))
	if !tracker.track(tracked, "readded") {
		t.Fatal("configuration update kept old revocation")
	}
	_ = tracked.Close()
}

func TestControlDrainDeadlineClosesLiveWorkAndRejectsUpdate(t *testing.T) {
	ctl := newGracefulCloseTestService().ctl
	defer ctl.Close()
	conn, peer := net.Pipe()
	defer peer.Close()
	tracked := msg.NewConn(conn, msg.NewReadWriter(conn, wire.ProtocolV1))
	if !ctl.workConnections.track(tracked, "live") {
		t.Fatal("initial work rejected")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := ctl.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drain deadline = %v", err)
	}
	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := peer.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("expired work stayed open: %v", err)
	}
	if err := ctl.UpdateAllConfigurer(nil, nil); err == nil {
		t.Fatal("draining control accepted configuration update")
	}
}
