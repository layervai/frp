// Copyright 2017 fatedier, fatedier@gmail.com
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
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	goliblog "github.com/fatedier/golib/log"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	frplog "github.com/fatedier/frp/pkg/util/log"
)

func TestUnixDomainSocketFailureClosesStreamWithoutLoggingPath(t *testing.T) {
	// Keep this test non-parallel: the FRP logger is process-global.
	var output bytes.Buffer
	previous := frplog.Logger
	frplog.Logger = goliblog.New(goliblog.WithOutput(&output), goliblog.WithLevel(goliblog.WarnLevel))
	t.Cleanup(func() { frplog.Logger = previous })
	const socketPath = "/nonexistent-private-origin/file.sock"
	plugin, err := NewUnixDomainSocketPlugin(PluginContext{}, &v1.UnixDomainSocketPluginOptions{UnixPath: socketPath})
	if err != nil {
		t.Fatal(err)
	}
	client, work := net.Pipe()
	defer client.Close()
	defer work.Close()
	if err := client.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	plugin.Handle(context.Background(), &ConnectionInfo{Conn: work})
	if _, err := client.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("failed origin did not close work stream: %v", err)
	}
	if !strings.Contains(output.String(), "local Unix socket origin is unavailable (") {
		t.Fatalf("missing origin warning: %s", output.String())
	}
	if strings.Contains(output.String(), "nonexistent-private-origin") || strings.Contains(output.String(), "file.sock") {
		t.Fatalf("dial warning leaked private path components: %s", output.String())
	}
}
