package server

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/msg"
	plugin "github.com/fatedier/frp/pkg/plugin/server"
	"github.com/fatedier/frp/pkg/util/xlog"
	"github.com/fatedier/frp/server/proxy"
	"github.com/fatedier/frp/server/registry"
)

type closeControlConn struct {
	closeCalls atomic.Int32
	firstClose chan struct{}
	release    chan struct{}
	once       sync.Once
}

func newCloseControlConn(blockFirst bool) *closeControlConn {
	c := &closeControlConn{}
	if blockFirst {
		c.firstClose = make(chan struct{})
		c.release = make(chan struct{})
	}
	return c
}

func (c *closeControlConn) Read([]byte) (int, error)         { return 0, net.ErrClosed }
func (c *closeControlConn) Write([]byte) (int, error)        { return 0, net.ErrClosed }
func (c *closeControlConn) LocalAddr() net.Addr              { return closeControlAddr("local") }
func (c *closeControlConn) RemoteAddr() net.Addr             { return closeControlAddr("remote") }
func (c *closeControlConn) SetDeadline(time.Time) error      { return nil }
func (c *closeControlConn) SetReadDeadline(time.Time) error  { return nil }
func (c *closeControlConn) SetWriteDeadline(time.Time) error { return nil }

func (c *closeControlConn) Close() error {
	call := c.closeCalls.Add(1)
	if call > 1 {
		return net.ErrClosed
	}
	if c.firstClose != nil {
		c.once.Do(func() { close(c.firstClose) })
		<-c.release
	}
	return nil
}

type closeControlAddr string

func (a closeControlAddr) Network() string { return string(a) }
func (a closeControlAddr) String() string  { return string(a) }

func testControl(conn net.Conn) *Control {
	return &Control{
		sessionCtx: &SessionContext{Conn: conn},
		doneCh:     make(chan struct{}),
		xl:         xlog.New(),
	}
}

func TestServiceCloseControlAbsentOrFinishedReturnsFalse(t *testing.T) {
	manager := NewControlManager()
	service := &Service{ctlManager: manager}
	if service.CloseControl("missing") {
		t.Fatal("CloseControl(missing) = true, want false")
	}

	conn := newCloseControlConn(false)
	finished := testControl(conn)
	close(finished.doneCh)
	manager.Add("finished", finished)
	if service.CloseControl("finished") {
		t.Fatal("CloseControl(finished) = true, want false")
	}
	if got := conn.closeCalls.Load(); got != 0 {
		t.Fatalf("finished connection Close calls = %d, want 0", got)
	}
}

func TestServiceCloseControlClosesExactCapturedControlAcrossReplacement(t *testing.T) {
	manager := NewControlManager()
	service := &Service{ctlManager: manager}
	oldConn := newCloseControlConn(true)
	oldControl := testControl(oldConn)
	manager.Add("same-run", oldControl)

	result := make(chan bool, 1)
	go func() { result <- service.CloseControl("same-run") }()
	select {
	case <-oldConn.firstClose:
	case <-time.After(time.Second):
		t.Fatal("CloseControl did not begin closing captured control")
	}

	newConn := newCloseControlConn(false)
	newControl := testControl(newConn)
	manager.Add("same-run", newControl)
	close(oldConn.release)

	select {
	case got := <-result:
		if !got {
			t.Fatal("CloseControl(captured) = false, want true")
		}
	case <-time.After(time.Second):
		t.Fatal("CloseControl did not return")
	}
	if got := newConn.closeCalls.Load(); got != 0 {
		t.Fatalf("replacement connection Close calls = %d, want 0", got)
	}
	if got, ok := manager.GetByID("same-run"); !ok || got != newControl {
		t.Fatal("replacement control is not the current manager entry")
	}
}

func TestServiceCloseControlQueuedBeforeSameRunReplacementClosesCurrentControl(t *testing.T) {
	manager := NewControlManager()
	service := &Service{ctlManager: manager}
	oldConn := newCloseControlConn(false)
	manager.Add("same-run", testControl(oldConn))

	queued := make(chan struct{})
	execute := make(chan struct{})
	result := make(chan bool, 1)
	go func() {
		close(queued)
		<-execute
		result <- service.CloseControl("same-run")
	}()
	<-queued

	newConn := newCloseControlConn(false)
	newControl := testControl(newConn)
	manager.Add("same-run", newControl)
	close(execute)

	select {
	case got := <-result:
		if !got {
			t.Fatal("CloseControl(queued stale cycle) = false, want true")
		}
	case <-time.After(time.Second):
		t.Fatal("queued CloseControl did not return")
	}
	if got := oldConn.closeCalls.Load(); got != 1 {
		t.Fatalf("replaced old connection Close calls = %d, want 1", got)
	}
	if got := newConn.closeCalls.Load(); got != 1 {
		t.Fatalf("current same-run connection Close calls = %d, want 1", got)
	}
	if got, ok := manager.GetByID("same-run"); !ok || got != newControl {
		t.Fatal("closed current control is no longer the manager's exact entry before worker cleanup")
	}
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
			t.Fatalf("worker plugin op = %q, want %q", op, plugin.OpCloseProxy)
		}
		delivered <- content.(plugin.CloseProxyContent)
		return &plugin.Response{Unchange: true}, nil, nil
	}})
	t.Cleanup(pluginManager.Close)

	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() { _ = clientConn.Close() })
	ctl, err := NewControl(context.Background(), &SessionContext{
		PxyManager:     proxy.NewManager(),
		PluginManager:  pluginManager,
		Conn:           serverConn,
		LoginMsg:       &msg.Login{RunID: runID},
		ServerCfg:      &v1.ServerConfig{},
		ClientRegistry: registry.NewClientRegistry(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctl.proxies[proxyName] = newRegisteredProxy(
		newCloseResultTestProxy(proxyName),
		plugin.UserInfo{RunID: runID},
		attemptID,
	)

	manager := NewControlManager()
	manager.Add(runID, ctl)
	service := &Service{ctlManager: manager}
	go ctl.worker()
	if !service.CloseControl(runID) {
		t.Fatal("CloseControl(active worker) = false, want true")
	}
	select {
	case <-ctl.doneCh:
	case <-time.After(time.Second):
		t.Fatal("control worker did not finish after CloseControl")
	}
	select {
	case got := <-delivered:
		if got.User.RunID != runID || got.ProxyName != proxyName || got.AttemptID != attemptID {
			t.Fatalf("CloseProxy callback = %+v, want exact run/proxy/attempt identity", got)
		}
	case <-time.After(time.Second):
		t.Fatal("control worker did not emit CloseProxy callback")
	}
}

func TestServiceCloseControlAlreadyClosedConnectionStillSignalsActiveControl(t *testing.T) {
	manager := NewControlManager()
	service := &Service{ctlManager: manager}
	conn := newCloseControlConn(false)
	if err := conn.Close(); err != nil {
		t.Fatalf("pre-close connection: %v", err)
	}
	manager.Add("closed", testControl(conn))
	if !service.CloseControl("closed") {
		t.Fatal("CloseControl(active control with closed transport) = false, want true")
	}
	if !errors.Is(conn.Close(), net.ErrClosed) {
		t.Fatal("test connection did not retain closed state")
	}
}
