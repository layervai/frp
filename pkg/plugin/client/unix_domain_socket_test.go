//go:build !frps && !windows

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
	var output bytes.Buffer
	previous := frplog.Logger
	frplog.Logger = goliblog.New(goliblog.WithOutput(&output))
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
	if output.Len() == 0 || strings.Contains(output.String(), socketPath) || strings.Contains(output.String(), "nonexistent-private-origin") {
		t.Fatalf("dial failure must emit a path-free warning: %s", output.String())
	}
}
