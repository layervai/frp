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
	"bytes"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/fatedier/golib/log"
	fmux "github.com/fatedier/yamux"

	frplog "github.com/fatedier/frp/pkg/util/log"
	"github.com/fatedier/frp/pkg/util/xlog"
)

func TestTransportErrorConnReportsPeerEOFOnce(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	var reports int
	c := &transportErrorConn{Conn: a, report: func(operation string, err error) {
		reports++
		if operation != "read" || !errors.Is(err, io.EOF) {
			t.Errorf("unexpected cause: %s %v", operation, err)
		}
	}}
	b.Close()
	for range 2 {
		if n, err := c.Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.EOF) {
			t.Fatalf("read = %d, %v", n, err)
		}
	}
	if reports != 1 {
		t.Fatalf("reports = %d, want 1", reports)
	}
}

func TestTransportErrorConnDoesNotReportLocalClose(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	reported := make(chan struct{}, 1)
	c := &transportErrorConn{Conn: a, report: func(string, error) { reported <- struct{}{} }}
	done := make(chan error, 1)
	go func() {
		_, err := c.Read(make([]byte, 1))
		done <- err
	}()
	c.Close()
	if err := <-done; err == nil {
		t.Fatal("local close did not interrupt read")
	}
	select {
	case <-reported:
		t.Fatal("local close reported as transport failure")
	default:
	}
}

func TestTransportErrorConnReportsTCPReset(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	peer, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.(*net.TCPConn).SetLinger(0); err != nil {
		peer.Close()
		t.Fatal(err)
	}
	peer.Close()
	var reported error
	c := &transportErrorConn{Conn: conn, report: func(operation string, err error) {
		if operation != "read" {
			t.Errorf("operation = %s", operation)
		}
		reported = err
	}}
	if err := c.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	_, err = c.Read(make([]byte, 1))
	if !errors.Is(err, syscall.ECONNRESET) || reported != err {
		t.Fatalf("read = %v; reported %v; want TCP reset", err, reported)
	}
}

type partialWriteFailure struct {
	net.Conn
	err error
}

func (c partialWriteFailure) Write([]byte) (int, error) { return 3, c.err }

func TestTransportErrorConnPreservesPartialWriteFailure(t *testing.T) {
	cause := errors.New("test connection reset")
	var got error
	c := &transportErrorConn{Conn: partialWriteFailure{err: cause}, report: func(operation string, err error) {
		if operation != "write" {
			t.Errorf("operation = %s", operation)
		}
		got = err
	}}
	if n, err := c.Write([]byte("private payload")); n != 3 || err != cause || got != cause {
		t.Fatalf("write = %d, %v; reported %v", n, err, got)
	}
}

type droppedWrites struct{ net.Conn }

func (c droppedWrites) Write(p []byte) (int, error) { return len(p), nil }

type keepaliveLog struct {
	sync.Mutex
	bytes.Buffer
	reported chan struct{}
	once     sync.Once
}

func (b *keepaliveLog) Write(p []byte) (int, error) {
	b.Lock()
	defer b.Unlock()
	n, err := b.Buffer.Write(p)
	if strings.Contains(string(p), "keepalive failed") {
		b.once.Do(func() { close(b.reported) })
	}
	return n, err
}

func TestDroppedMuxKeepaliveReplyIsLoggedAtWarn(t *testing.T) {
	output := &keepaliveLog{reported: make(chan struct{})}
	oldLogger := frplog.Logger
	frplog.Logger = log.New(log.WithLevel(log.WarnLevel), log.WithOutput(output))
	defer func() { frplog.Logger = oldLogger }()
	a, b := net.Pipe()
	serverConfig := fmux.DefaultConfig()
	serverConfig.EnableKeepAlive = false
	serverConfig.LogOutput = io.Discard
	server, err := fmux.Server(droppedWrites{b}, serverConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	clientConfig := fmux.DefaultConfig()
	clientConfig.KeepAliveInterval = 10 * time.Millisecond
	clientConfig.ConnectionWriteTimeout = 30 * time.Millisecond
	clientConfig.LogOutput = muxFailureWriter{logger: xlog.New()}
	observed := &transportErrorConn{Conn: a, report: func(operation string, err error) {
		t.Errorf("multiplexer local close reported as transport %s failure: %v", operation, err)
	}}
	client, err := fmux.Client(observed, clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	select {
	case <-output.reported:
	case <-time.After(3 * time.Second):
		t.Fatal("dropped keepalive reply closed the session without a warning")
	}
	select {
	case <-client.CloseChan():
	case <-time.After(3 * time.Second):
		t.Fatal("failed keepalive did not close the session")
	}
}

func TestMuxFailureWriterKeepsStreamMessagesQuiet(t *testing.T) {
	var output bytes.Buffer
	oldLogger := frplog.Logger
	frplog.Logger = log.New(log.WithLevel(log.WarnLevel), log.WithOutput(&output))
	defer func() { frplog.Logger = oldLogger }()
	writer := muxFailureWriter{logger: xlog.New()}
	for _, message := range []string{
		"[WARN] yamux: Discarding data for stream: 3",
		"[WARN] yamux: frame for missing stream: 3",
		"[ERR] yamux: unexpected FIN flag in state 4",
		"[ERR] yamux: Failed to read header: connection reset by peer",
	} {
		if n, err := writer.Write([]byte(message)); n != len(message) || err != nil {
			t.Fatalf("write = %d, %v", n, err)
		}
	}
	if output.Len() != 0 {
		t.Fatalf("stream or duplicate transport errors promoted to WARN: %s", output.String())
	}
	if _, err := writer.Write([]byte("[ERR] yamux: received protocol error go away")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "received protocol error go away") {
		t.Fatal("session-fatal protocol error was hidden")
	}
}
