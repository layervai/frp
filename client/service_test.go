package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/samber/lo"

	"github.com/fatedier/frp/pkg/config/source"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/msg"
)

type failingConnector struct {
	err error
}

func (c *failingConnector) Open() error {
	return c.err
}

func (c *failingConnector) Connect() (net.Conn, error) {
	return nil, c.err
}

func (c *failingConnector) Close() error {
	return nil
}

func getFreeTCPPort(t *testing.T) int {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on ephemeral port: %v", err)
	}
	defer ln.Close()

	return ln.Addr().(*net.TCPAddr).Port
}

func waitForInstalledControlRunID(t *testing.T, svr *Service, want string) {
	t.Helper()

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	for {
		svr.ctlMu.RLock()
		ctl := svr.ctl
		got := ""
		if ctl != nil {
			got = ctl.sessionCtx.RunID
		}
		svr.ctlMu.RUnlock()
		if ctl != nil && got == want {
			return
		}

		select {
		case <-ticker.C:
		case <-timer.C:
			t.Fatalf("timed out waiting for installed control RunID %q; last RunID %q", want, got)
		}
	}
}

func TestRunSendsInitialRunIDOnFirstLoginAndReconnect(t *testing.T) {
	firstClientConn, firstServerConn := net.Pipe()
	secondClientConn, secondServerConn := net.Pipe()
	t.Cleanup(func() {
		_ = firstClientConn.Close()
		_ = firstServerConn.Close()
		_ = secondClientConn.Close()
		_ = secondServerConn.Close()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	secondLoginAccepted := make(chan struct{})
	serverErrCh := make(chan error, 1)
	go func() {
		defer firstServerConn.Close()
		defer secondServerConn.Close()

		rw := msg.NewV1ReadWriter(firstServerConn)
		var loginMsg msg.Login
		if err := rw.ReadMsgInto(&loginMsg); err != nil {
			serverErrCh <- err
			return
		}
		if loginMsg.RunID != "initial-run-id" {
			serverErrCh <- fmt.Errorf("first login RunID = %q, want %q", loginMsg.RunID, "initial-run-id")
			return
		}
		if err := rw.WriteMsg(&msg.LoginResp{RunID: "initial-run-id"}); err != nil {
			serverErrCh <- err
			return
		}
		if err := firstServerConn.Close(); err != nil {
			serverErrCh <- err
			return
		}

		rw = msg.NewV1ReadWriter(secondServerConn)
		loginMsg = msg.Login{}
		if err := rw.ReadMsgInto(&loginMsg); err != nil {
			serverErrCh <- err
			return
		}
		if loginMsg.RunID != "initial-run-id" {
			serverErrCh <- fmt.Errorf("reconnect login RunID = %q, want %q", loginMsg.RunID, "initial-run-id")
			return
		}
		if err := rw.WriteMsg(&msg.LoginResp{RunID: "reconnected-run-id"}); err != nil {
			serverErrCh <- err
			return
		}
		close(secondLoginAccepted)
		<-ctx.Done()
		serverErrCh <- nil
	}()

	var connectorCount atomic.Int32
	var callbacks atomic.Int32
	svr, err := NewService(ServiceOptions{
		Common:                 &v1.ClientCommonConfig{},
		ConfigSourceAggregator: source.NewAggregator(source.NewConfigSource()),
		InitialRunID:           "initial-run-id",
		ConnectorCreator: func(context.Context, *v1.ClientCommonConfig) Connector {
			switch connectorCount.Add(1) {
			case 1:
				return &testConnector{conn: &trackingConn{Conn: firstClientConn}}
			case 2:
				return &testConnector{conn: &trackingConn{Conn: secondClientConn}}
			default:
				return &failingConnector{err: context.Canceled}
			}
		},
		OnFirstLoginSuccess: func(runID string) error {
			if runID != "initial-run-id" {
				t.Errorf("OnFirstLoginSuccess RunID = %q, want initial-run-id", runID)
			}
			callbacks.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- svr.Run(ctx)
	}()

	select {
	case <-secondLoginAccepted:
		waitForInstalledControlRunID(t, svr, "reconnected-run-id")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for reconnect login")
	}
	cancel()

	select {
	case err := <-runErrCh:
		if err != nil {
			t.Fatalf("run service: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for service shutdown")
	}
	if err := <-serverErrCh; err != nil {
		t.Fatalf("mock server: %v", err)
	}

	if got := connectorCount.Load(); got != 2 {
		t.Fatalf("connector attempts = %d, want 2", got)
	}
	if got := callbacks.Load(); got != 1 {
		t.Fatalf("OnFirstLoginSuccess calls = %d, want 1 across reconnects", got)
	}
}

func TestServiceOnFirstLoginSuccessFiresOnlyAfterAcceptedLogin(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	trackedConn := &trackingConn{Conn: clientConn}
	connector := &testConnector{conn: trackedConn}
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	accepted := make(chan string, 1)
	serverErr := make(chan error, 1)
	go func() {
		defer serverConn.Close()
		rw := msg.NewV1ReadWriter(serverConn)
		var login msg.Login
		if err := rw.ReadMsgInto(&login); err != nil {
			serverErr <- err
			return
		}
		if err := rw.WriteMsg(&msg.LoginResp{RunID: "accepted-run"}); err != nil {
			serverErr <- err
			return
		}
		<-ctx.Done()
		serverErr <- nil
	}()

	svr, err := NewService(ServiceOptions{
		Common:                 &v1.ClientCommonConfig{},
		ConfigSourceAggregator: source.NewAggregator(source.NewConfigSource()),
		ConnectorCreator: func(context.Context, *v1.ClientCommonConfig) Connector {
			return connector
		},
		OnFirstLoginSuccess: func(runID string) error {
			accepted <- runID
			return nil
		},
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	runErrCh := make(chan error, 1)
	go func() { runErrCh <- svr.Run(ctx) }()

	select {
	case runID := <-accepted:
		if runID != "accepted-run" {
			t.Fatalf("OnFirstLoginSuccess RunID = %q, want accepted-run", runID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for accepted-login callback")
	}
	waitForInstalledControlRunID(t, svr, "accepted-run")
	select {
	case err := <-runErrCh:
		t.Fatalf("Service.Run returned before external cancellation: %v", err)
	default:
	}

	cancel()
	select {
	case err := <-runErrCh:
		if err != nil {
			t.Fatalf("run service: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for service shutdown")
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("server: %v", err)
	}
	if !trackedConn.closed.Load() {
		t.Fatal("control connection was not closed during Service.Run cleanup")
	}
	if !connector.closed.Load() {
		t.Fatal("connector was not closed during Service.Run cleanup")
	}
}

func TestServiceOnFirstLoginSuccessDoesNotFireOnAuthenticatedLoginRejection(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	serverErr := make(chan error, 1)
	go func() {
		rw := msg.NewV1ReadWriter(serverConn)
		var login msg.Login
		if err := rw.ReadMsgInto(&login); err != nil {
			serverErr <- err
			return
		}
		serverErr <- rw.WriteMsg(&msg.LoginResp{RunID: "rejected-run", Error: "token rejected"})
	}()

	loginFailExit := true
	var callbacks atomic.Int32
	svr, err := NewService(ServiceOptions{
		Common:                 &v1.ClientCommonConfig{LoginFailExit: &loginFailExit},
		ConfigSourceAggregator: source.NewAggregator(source.NewConfigSource()),
		ConnectorCreator: func(context.Context, *v1.ClientCommonConfig) Connector {
			return &testConnector{conn: &trackingConn{Conn: clientConn}}
		},
		OnFirstLoginSuccess: func(string) error {
			callbacks.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	if err := svr.Run(context.Background()); err == nil {
		t.Fatal("Run() error = nil, want authenticated LoginResp rejection")
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("server: %v", err)
	}
	if got := callbacks.Load(); got != 0 {
		t.Fatalf("OnFirstLoginSuccess calls = %d, want 0", got)
	}
}

func TestServiceOnFirstLoginSuccessErrorRejectsSessionBeforeControl(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	trackedConn := &trackingConn{Conn: clientConn}
	connector := &testConnector{conn: trackedConn}
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	serverErr := make(chan error, 1)
	go func() {
		rw := msg.NewV1ReadWriter(serverConn)
		var login msg.Login
		if err := rw.ReadMsgInto(&login); err != nil {
			serverErr <- err
			return
		}
		serverErr <- rw.WriteMsg(&msg.LoginResp{RunID: "untrusted-run"})
	}()

	hookCause := errors.New("sensitive hook detail: " + strings.Repeat("x", 8_192))
	var callbacks atomic.Int32
	svr, err := NewService(ServiceOptions{
		Common:                 &v1.ClientCommonConfig{},
		ConfigSourceAggregator: source.NewAggregator(source.NewConfigSource()),
		ConnectorCreator: func(context.Context, *v1.ClientCommonConfig) Connector {
			return connector
		},
		OnFirstLoginSuccess: func(runID string) error {
			callbacks.Add(1)
			if runID != "untrusted-run" {
				t.Errorf("OnFirstLoginSuccess RunID = %q, want untrusted-run", runID)
			}
			return hookCause
		},
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	err = svr.Run(context.Background())
	if err == nil {
		t.Fatal("Run() error = nil, want hook rejection")
	}
	if !errors.Is(err, hookCause) {
		t.Fatal("Run() error does not wrap the original hook cause")
	}
	if !strings.Contains(err.Error(), "first login success hook rejected authenticated session") {
		t.Fatalf("Run() error = %q, want bounded hook rejection", err)
	}
	if strings.Contains(err.Error(), "sensitive hook detail") || len(err.Error()) > 256 {
		t.Fatalf("Run() exposed unbounded hook error: length=%d error=%q", len(err.Error()), err)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("server: %v", err)
	}
	if got := callbacks.Load(); got != 1 {
		t.Fatalf("OnFirstLoginSuccess calls = %d, want 1", got)
	}
	if !trackedConn.closed.Load() {
		t.Fatal("authenticated session connection was not closed after hook rejection")
	}
	if !connector.closed.Load() {
		t.Fatal("authenticated session connector was not closed after hook rejection")
	}
	svr.ctlMu.RLock()
	ctl := svr.ctl
	svr.ctlMu.RUnlock()
	if ctl != nil {
		t.Fatal("control was installed after hook rejection")
	}
}

func TestRunStopsStartedComponentsOnInitialLoginFailure(t *testing.T) {
	port := getFreeTCPPort(t)
	agg := source.NewAggregator(source.NewConfigSource())

	svr, err := NewService(ServiceOptions{
		Common: &v1.ClientCommonConfig{
			LoginFailExit: lo.ToPtr(true),
			WebServer: v1.WebServerConfig{
				Addr: "127.0.0.1",
				Port: port,
			},
		},
		ConfigSourceAggregator: agg,
		ConnectorCreator: func(context.Context, *v1.ClientCommonConfig) Connector {
			return &failingConnector{err: errors.New("login boom")}
		},
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	err = svr.Run(context.Background())
	if err == nil {
		t.Fatal("expected run error, got nil")
	}
	if !strings.Contains(err.Error(), "login boom") {
		t.Fatalf("unexpected error: %v", err)
	}
	if svr.webServer != nil {
		t.Fatal("expected web server to be cleaned up after initial login failure")
	}

	ln, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		t.Fatalf("expected admin port to be released: %v", err)
	}
	_ = ln.Close()
}

func TestNewServiceDoesNotLeakAdminListenerOnAuthBuildFailure(t *testing.T) {
	port := getFreeTCPPort(t)
	agg := source.NewAggregator(source.NewConfigSource())

	_, err := NewService(ServiceOptions{
		Common: &v1.ClientCommonConfig{
			Auth: v1.AuthClientConfig{
				Method: v1.AuthMethodOIDC,
				OIDC: v1.AuthOIDCClientConfig{
					TokenEndpointURL: "://bad",
				},
			},
			WebServer: v1.WebServerConfig{
				Addr: "127.0.0.1",
				Port: port,
			},
		},
		ConfigSourceAggregator: agg,
	})
	if err == nil {
		t.Fatal("expected new service error, got nil")
	}
	if !strings.Contains(err.Error(), "auth.oidc.tokenEndpointURL") {
		t.Fatalf("unexpected error: %v", err)
	}

	ln, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		t.Fatalf("expected admin port to remain free: %v", err)
	}
	_ = ln.Close()
}

func TestUpdateConfigSourceRollsBackReloadCommonOnReplaceAllFailure(t *testing.T) {
	prevCommon := &v1.ClientCommonConfig{User: "old-user"}
	newCommon := &v1.ClientCommonConfig{User: "new-user"}

	svr := &Service{
		configSource: source.NewConfigSource(),
		reloadCommon: prevCommon,
	}

	invalidProxy := &v1.TCPProxyConfig{}
	err := svr.UpdateConfigSource(newCommon, []v1.ProxyConfigurer{invalidProxy}, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !strings.Contains(err.Error(), "proxy name cannot be empty") {
		t.Fatalf("unexpected error: %v", err)
	}

	if svr.reloadCommon != prevCommon {
		t.Fatalf("reloadCommon should roll back on ReplaceAll failure")
	}
}

func TestUpdateConfigSourceKeepsReloadCommonOnReloadFailure(t *testing.T) {
	prevCommon := &v1.ClientCommonConfig{User: "old-user"}
	newCommon := &v1.ClientCommonConfig{User: "new-user"}

	svr := &Service{
		// Keep configSource valid so ReplaceAll succeeds first.
		configSource: source.NewConfigSource(),
		reloadCommon: prevCommon,
		// Keep aggregator nil to force reload failure.
		aggregator: nil,
	}

	validProxy := &v1.TCPProxyConfig{
		ProxyBaseConfig: v1.ProxyBaseConfig{
			Name: "p1",
			Type: "tcp",
		},
	}
	err := svr.UpdateConfigSource(newCommon, []v1.ProxyConfigurer{validProxy}, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !strings.Contains(err.Error(), "config aggregator is not initialized") {
		t.Fatalf("unexpected error: %v", err)
	}

	if svr.reloadCommon != newCommon {
		t.Fatalf("reloadCommon should keep new value on reload failure")
	}
}

func TestReloadConfigFromSourcesDoesNotMutateStoreConfigs(t *testing.T) {
	storeSource, err := source.NewStoreSource(source.StoreSourceConfig{
		Path: filepath.Join(t.TempDir(), "store.json"),
	})
	if err != nil {
		t.Fatalf("new store source: %v", err)
	}

	proxyCfg := &v1.TCPProxyConfig{
		ProxyBaseConfig: v1.ProxyBaseConfig{
			Name: "store-proxy",
			Type: "tcp",
		},
	}
	visitorCfg := &v1.STCPVisitorConfig{
		VisitorBaseConfig: v1.VisitorBaseConfig{
			Name: "store-visitor",
			Type: "stcp",
		},
	}
	if err := storeSource.AddProxy(proxyCfg); err != nil {
		t.Fatalf("add proxy to store: %v", err)
	}
	if err := storeSource.AddVisitor(visitorCfg); err != nil {
		t.Fatalf("add visitor to store: %v", err)
	}

	agg := source.NewAggregator(source.NewConfigSource())
	agg.SetStoreSource(storeSource)
	svr := &Service{
		aggregator:   agg,
		configSource: agg.ConfigSource(),
		storeSource:  storeSource,
		reloadCommon: &v1.ClientCommonConfig{},
	}

	if err := svr.reloadConfigFromSources(); err != nil {
		t.Fatalf("reload config from sources: %v", err)
	}

	gotProxy := storeSource.GetProxy("store-proxy")
	if gotProxy == nil {
		t.Fatalf("proxy not found in store")
	}
	if gotProxy.GetBaseConfig().LocalIP != "" {
		t.Fatalf("store proxy localIP should stay empty, got %q", gotProxy.GetBaseConfig().LocalIP)
	}

	gotVisitor := storeSource.GetVisitor("store-visitor")
	if gotVisitor == nil {
		t.Fatalf("visitor not found in store")
	}
	if gotVisitor.GetBaseConfig().BindAddr != "" {
		t.Fatalf("store visitor bindAddr should stay empty, got %q", gotVisitor.GetBaseConfig().BindAddr)
	}

	svr.cfgMu.RLock()
	defer svr.cfgMu.RUnlock()

	if len(svr.proxyCfgs) != 1 {
		t.Fatalf("expected 1 runtime proxy, got %d", len(svr.proxyCfgs))
	}
	if svr.proxyCfgs[0].GetBaseConfig().LocalIP != "127.0.0.1" {
		t.Fatalf("runtime proxy localIP should be defaulted, got %q", svr.proxyCfgs[0].GetBaseConfig().LocalIP)
	}

	if len(svr.visitorCfgs) != 1 {
		t.Fatalf("expected 1 runtime visitor, got %d", len(svr.visitorCfgs))
	}
	if svr.visitorCfgs[0].GetBaseConfig().BindAddr != "127.0.0.1" {
		t.Fatalf("runtime visitor bindAddr should be defaulted, got %q", svr.visitorCfgs[0].GetBaseConfig().BindAddr)
	}
}
