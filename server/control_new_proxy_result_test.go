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

package server

import (
	"context"
	"encoding/hex"
	"errors"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/time/rate"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/msg"
	plugin "github.com/fatedier/frp/pkg/plugin/server"
	"github.com/fatedier/frp/pkg/util/util"
	"github.com/fatedier/frp/pkg/util/xlog"
	"github.com/fatedier/frp/server/controller"
	servermetrics "github.com/fatedier/frp/server/metrics"
	"github.com/fatedier/frp/server/proxy"
)

type controlResultPlugin struct {
	handle func(string, any) (*plugin.Response, any, error)
}

type controlContextPlugin struct {
	supported []string
	handle    func(context.Context, string, any) (*plugin.Response, any, error)
}

type closeTrackingControlConn struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
}

type closeResultTestProxy struct {
	name       string
	configurer v1.ProxyConfigurer
}

type controlMetricsRecorder struct {
	newProxyCalls   int
	closeProxyCalls int
	proxyCount      int
	name            string
	proxyType       string
	user            string
	clientID        string
}

func (*controlMetricsRecorder) NewClient()   {}
func (*controlMetricsRecorder) CloseClient() {}
func (m *controlMetricsRecorder) NewProxy(name, proxyType, user, clientID string) {
	m.newProxyCalls++
	m.proxyCount++
	m.name = name
	m.proxyType = proxyType
	m.user = user
	m.clientID = clientID
}

func (m *controlMetricsRecorder) CloseProxy(name, proxyType string) {
	m.closeProxyCalls++
	m.proxyCount--
	m.name = name
	m.proxyType = proxyType
}
func (*controlMetricsRecorder) OpenConnection(string, string)       {}
func (*controlMetricsRecorder) CloseConnection(string, string)      {}
func (*controlMetricsRecorder) AddTrafficIn(string, string, int64)  {}
func (*controlMetricsRecorder) AddTrafficOut(string, string, int64) {}

func newCloseResultTestProxy(name string) *closeResultTestProxy {
	return &closeResultTestProxy{
		name: name,
		configurer: &v1.TCPProxyConfig{ProxyBaseConfig: v1.ProxyBaseConfig{
			Name: name,
			Type: "tcp",
		}},
	}
}

func (*closeResultTestProxy) Context() context.Context { return context.Background() }
func (*closeResultTestProxy) Run() (string, error)     { return "", nil }
func (p *closeResultTestProxy) GetName() string        { return p.name }
func (p *closeResultTestProxy) GetConfigurer() v1.ProxyConfigurer {
	return p.configurer
}

func (*closeResultTestProxy) GetWorkConnFromPool(net.Addr, net.Addr) (net.Conn, error) {
	return nil, nil
}
func (*closeResultTestProxy) GetUsedPortsNum() int { return 0 }
func (*closeResultTestProxy) GetResourceController() *controller.ResourceController {
	return nil
}
func (*closeResultTestProxy) GetUserInfo() plugin.UserInfo { return plugin.UserInfo{} }
func (*closeResultTestProxy) GetLimiter() *rate.Limiter    { return nil }
func (*closeResultTestProxy) GetLoginMsg() *msg.Login      { return nil }
func (*closeResultTestProxy) Close()                       {}

func (*controlResultPlugin) Name() string { return "control-result-test" }

func (*controlResultPlugin) IsSupport(op string) bool {
	// Supporting CloseProxy here makes every failed-admission test a canary
	// against synthetic teardown compensation. The result callback must be the
	// only post-attempt operation.
	return op == plugin.OpNewProxy || op == plugin.OpNewProxyResult || op == plugin.OpCloseProxy
}

func (p *controlResultPlugin) Handle(_ context.Context, op string, content any) (*plugin.Response, any, error) {
	return p.handle(op, content)
}

func (*controlContextPlugin) Name() string { return "control-context-test" }

func (p *controlContextPlugin) IsSupport(op string) bool {
	return slices.Contains(p.supported, op)
}

func (p *controlContextPlugin) Handle(ctx context.Context, op string, content any) (*plugin.Response, any, error) {
	return p.handle(ctx, op, content)
}

func (c *closeTrackingControlConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

func TestProcessNewProxyAttemptReportsSuccessfulAdmissionSynchronously(t *testing.T) {
	t.Parallel()

	var order []string
	var preparedAttemptID string
	var result plugin.NewProxyResultContent
	manager := plugin.NewManager()
	manager.Register(&controlResultPlugin{handle: func(op string, content any) (*plugin.Response, any, error) {
		switch op {
		case plugin.OpNewProxy:
			order = append(order, "plugin")
			prepared := content.(plugin.NewProxyContent)
			assertValidNewProxyAttemptID(t, prepared.AttemptID)
			preparedAttemptID = prepared.AttemptID
			return &plugin.Response{Unchange: true}, nil, nil
		case plugin.OpNewProxyResult:
			order = append(order, "result")
			result = content.(plugin.NewProxyResultContent)
			return &plugin.Response{Unchange: true}, nil, nil
		default:
			t.Fatalf("unexpected op %q", op)
			return nil, nil, nil
		}
	}})

	content := newResultTestContent("0123456789abcdef", "proxy-Exact")
	effective, remoteAddr, admissionErr, resultErr := processNewProxyAttempt(
		manager,
		content,
		func(got *msg.NewProxy, gotUser plugin.UserInfo, gotAttemptID string) (string, error) {
			order = append(order, "register")
			if got.ProxyName != "proxy-Exact" {
				t.Fatalf("register ProxyName = %q, want exact input", got.ProxyName)
			}
			if gotAttemptID != preparedAttemptID {
				t.Fatalf("register AttemptID = %q, want %q", gotAttemptID, preparedAttemptID)
			}
			if gotUser.RunID != "0123456789abcdef" {
				t.Fatalf("register RunID = %q, want exact input", gotUser.RunID)
			}
			return "127.0.0.1:8080", nil
		},
	)

	if admissionErr != nil || resultErr != nil {
		t.Fatalf("errors = admission %v, result %v; want nil", admissionErr, resultErr)
	}
	if effective.ProxyName != "proxy-Exact" || remoteAddr != "127.0.0.1:8080" {
		t.Fatalf("effective = %+v, remoteAddr = %q", effective, remoteAddr)
	}
	if result.User.RunID != "0123456789abcdef" || result.AttemptID != preparedAttemptID || result.ProxyName != "proxy-Exact" || !result.Admitted {
		t.Fatalf("result = %+v, want exact admitted identity", result)
	}
	if !slices.Equal(order, []string{"plugin", "register", "result"}) {
		t.Fatalf("order = %v, want [plugin register result]", order)
	}
}

func TestConfirmNewProxyResultRejectRollsBackBeforeClientSuccess(t *testing.T) {
	t.Parallel()

	manager := plugin.NewManager()
	manager.Register(&controlResultPlugin{handle: func(op string, content any) (*plugin.Response, any, error) {
		if op != plugin.OpNewProxyResult {
			t.Fatalf("op = %q, want %q", op, plugin.OpNewProxyResult)
		}
		if got := content.(plugin.NewProxyResultContent); !got.Admitted {
			t.Fatalf("result = %+v, want admitted result", got)
		}
		return &plugin.Response{Reject: true, RejectReason: "external registration not committed"}, nil, nil
	}})

	rollbackCalls := 0
	finalErr, resultErr := confirmNewProxyResult(
		manager,
		&plugin.NewProxyResultContent{Admitted: true},
		nil,
		func() error {
			rollbackCalls++
			return nil
		},
	)
	if finalErr == nil || resultErr == nil {
		t.Fatalf("errors = final %v, result %v; want both non-nil", finalErr, resultErr)
	}
	if !strings.Contains(finalErr.Error(), "confirm new proxy admission") ||
		!strings.Contains(resultErr.Error(), "external registration not committed") {
		t.Fatalf("errors = final %q, result %q", finalErr, resultErr)
	}
	if rollbackCalls != 1 {
		t.Fatalf("rollback calls = %d, want 1", rollbackCalls)
	}
}

func TestConfirmNewProxyResultFailedAdmissionKeepsOriginalError(t *testing.T) {
	t.Parallel()

	manager := plugin.NewManager()
	manager.Register(&controlResultPlugin{handle: func(string, any) (*plugin.Response, any, error) {
		return nil, nil, errors.New("cleanup callback unavailable")
	}})

	admissionErr := errors.New("proxy name already exists")
	rollbackCalls := 0
	finalErr, resultErr := confirmNewProxyResult(
		manager,
		&plugin.NewProxyResultContent{Admitted: false},
		admissionErr,
		func() error {
			rollbackCalls++
			return nil
		},
	)
	if !errors.Is(finalErr, admissionErr) || resultErr == nil {
		t.Fatalf("errors = final %v, result %v; want original admission and callback error", finalErr, resultErr)
	}
	if rollbackCalls != 0 {
		t.Fatalf("rollback calls = %d, want 0", rollbackCalls)
	}
}

func TestConfirmNewProxyResultManagerCloseCancelsAndRollsBack(t *testing.T) {
	t.Parallel()

	requestStarted := make(chan struct{})
	requestCanceled := make(chan struct{})
	manager := plugin.NewManager()
	manager.Register(&controlContextPlugin{
		supported: []string{plugin.OpNewProxyResult},
		handle: func(ctx context.Context, _ string, _ any) (*plugin.Response, any, error) {
			close(requestStarted)
			<-ctx.Done()
			close(requestCanceled)
			return nil, nil, ctx.Err()
		},
	})

	type confirmationResult struct {
		finalErr  error
		resultErr error
	}
	rolledBack := make(chan struct{}, 1)
	resultCh := make(chan confirmationResult, 1)
	go func() {
		finalErr, resultErr := confirmNewProxyResult(
			manager,
			&plugin.NewProxyResultContent{Admitted: true},
			nil,
			func() error {
				rolledBack <- struct{}{}
				return nil
			},
		)
		resultCh <- confirmationResult{finalErr: finalErr, resultErr: resultErr}
	}()

	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("NewProxyResult plugin request did not start")
	}
	manager.Close()
	select {
	case <-requestCanceled:
	case <-time.After(time.Second):
		t.Fatal("plugin request context was not canceled when the manager closed")
	}

	select {
	case result := <-resultCh:
		if result.finalErr == nil || result.resultErr == nil ||
			!strings.Contains(result.resultErr.Error(), "result plugin delivery failed") ||
			strings.Contains(result.resultErr.Error(), context.Canceled.Error()) {
			t.Fatalf("confirmation errors = final %v, result %v; want sanitized cancellation and rollback",
				result.finalErr, result.resultErr)
		}
	case <-time.After(time.Second):
		t.Fatal("NewProxyResult did not stop when the plugin manager closed")
	}
	select {
	case <-rolledBack:
	default:
		t.Fatal("canceled NewProxyResult did not roll back tentative admission")
	}
}

func TestNewProxyResultDeliveryErrorsStaySanitizedForClientDetailModes(t *testing.T) {
	t.Parallel()

	const sensitiveTransportError = "dial tcp 10.0.0.9:9443: tls certificate secret-plugin.internal"
	manager := plugin.NewManager()
	manager.Register(&controlContextPlugin{
		supported: []string{plugin.OpNewProxyResult},
		handle: func(context.Context, string, any) (*plugin.Response, any, error) {
			return nil, nil, errors.New(sensitiveTransportError)
		},
	})
	t.Cleanup(manager.Close)

	rollbackCalls := 0
	finalErr, resultErr := confirmNewProxyResult(
		manager,
		&plugin.NewProxyResultContent{Admitted: true},
		nil,
		func() error {
			rollbackCalls++
			return nil
		},
	)
	if finalErr == nil || resultErr == nil || rollbackCalls != 1 {
		t.Fatalf("confirmation = final %v result %v rollback %d, want sanitized failure and one rollback",
			finalErr, resultErr, rollbackCalls)
	}

	for _, detailed := range []bool{true, false} {
		got := util.GenerateResponseErrorString("new proxy error", finalErr, detailed)
		if strings.Contains(got, sensitiveTransportError) || strings.Contains(got, "secret-plugin.internal") {
			t.Fatalf("detailed=%t client error %q leaked raw transport details", detailed, got)
		}
		if detailed && !strings.Contains(got, "result plugin delivery failed") {
			t.Fatalf("detailed client error = %q, want sanitized delivery reason", got)
		}
		if !detailed && got != "new proxy error" {
			t.Fatalf("non-detailed client error = %q, want summary", got)
		}
	}
}

func TestHandleNewProxyRefreshesHeartbeatBeforeSynchronousPlugin(t *testing.T) {
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	manager := plugin.NewManager()
	manager.Register(&controlContextPlugin{
		supported: []string{plugin.OpNewProxy},
		handle: func(_ context.Context, _ string, _ any) (*plugin.Response, any, error) {
			close(requestStarted)
			<-releaseRequest
			return &plugin.Response{Reject: true, RejectReason: "test complete"}, nil, nil
		},
	})
	t.Cleanup(manager.Close)

	serverConn, peerConn := net.Pipe()
	trackedConn := &closeTrackingControlConn{Conn: serverConn, closed: make(chan struct{})}
	t.Cleanup(func() {
		_ = trackedConn.Close()
		_ = peerConn.Close()
	})
	stopHeartbeat := make(chan struct{})
	ctl := &Control{
		sessionCtx: &SessionContext{
			Conn:          trackedConn,
			PluginManager: manager,
			LoginMsg:      &msg.Login{RunID: "heartbeat-run"},
			ServerCfg: &v1.ServerConfig{Transport: v1.ServerTransportConfig{
				HeartbeatTimeout: 2,
			}},
		},
		msgDispatcher: msg.NewDispatcher(trackedConn),
		xl:            xlog.FromContextSafe(context.Background()),
		doneCh:        stopHeartbeat,
	}
	stalePing := time.Now().Add(-time.Minute)
	ctl.lastPing.Store(stalePing)

	handlerDone := make(chan struct{})
	go func() {
		ctl.handleNewProxy(&msg.NewProxy{ProxyName: "heartbeat-refresh", ProxyType: "tcp"})
		close(handlerDone)
	}()
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("NewProxy plugin request did not start")
	}
	if refreshed := ctl.lastPing.Load().(time.Time); !refreshed.After(stalePing) {
		t.Fatalf("lastPing = %s, want refresh after %s", refreshed, stalePing)
	}

	ctl.heartbeatWorker()
	select {
	case <-trackedConn.closed:
		t.Fatal("heartbeat worker closed a live control during synchronous NewProxy callback")
	case <-time.After(100 * time.Millisecond):
	}
	close(stopHeartbeat)
	close(releaseRequest)
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("NewProxy handler did not finish after plugin release")
	}
}

func TestControlCloseProxyPropagatesNotificationAdmissionFailure(t *testing.T) {
	t.Parallel()

	manager := plugin.NewManager()
	manager.Register(&controlResultPlugin{handle: func(string, any) (*plugin.Response, any, error) {
		t.Fatal("closed plugin manager delivered CloseProxy callback")
		return nil, nil, nil
	}})
	manager.Close()

	const proxyName = "proxy-rollback"
	pxyManager := proxy.NewManager()
	ctl := &Control{
		proxies: map[string]registeredProxy{
			proxyName: newRegisteredProxy(
				newCloseResultTestProxy(proxyName),
				plugin.UserInfo{RunID: "0123456789abcdef"},
				"0123456789abcdef0123456789abcdef",
			),
		},
		sessionCtx: &SessionContext{
			PxyManager:    pxyManager,
			PluginManager: manager,
			ServerCfg:     &v1.ServerConfig{},
		},
		xl: xlog.FromContextSafe(context.Background()),
	}

	err := ctl.CloseProxy(&msg.CloseProxy{ProxyName: proxyName})
	if err == nil || !strings.Contains(err.Error(), "plugin manager is closed") {
		t.Fatalf("CloseProxy() error = %v, want plugin-manager closure", err)
	}
	if _, ok := ctl.proxies[proxyName]; ok {
		t.Fatal("CloseProxy() kept locally closed proxy registered after notification failure")
	}
}

func TestTentativeAdmissionRollbackBalancesProxyMetrics(t *testing.T) {
	recorder := &controlMetricsRecorder{}
	originalMetrics := servermetrics.Server
	servermetrics.Server = recorder
	t.Cleanup(func() { servermetrics.Server = originalMetrics })

	const (
		proxyName = "proxy-metric-rollback"
		attemptID = "0123456789abcdef0123456789abcdef"
	)
	pxy := newCloseResultTestProxy(proxyName)
	pxyManager := proxy.NewManager()
	if err := pxyManager.Add(proxyName, pxy); err != nil {
		t.Fatalf("add test proxy: %v", err)
	}
	manager := plugin.NewManager()
	manager.Register(&controlResultPlugin{handle: func(op string, _ any) (*plugin.Response, any, error) {
		switch op {
		case plugin.OpNewProxyResult:
			return &plugin.Response{Reject: true, RejectReason: "routing admission failed"}, nil, nil
		case plugin.OpCloseProxy:
			return &plugin.Response{Unchange: true}, nil, nil
		default:
			t.Fatalf("unexpected op %q", op)
			return nil, nil, nil
		}
	}})
	t.Cleanup(manager.Close)
	ctl := &Control{
		proxies: make(map[string]registeredProxy),
		sessionCtx: &SessionContext{
			PxyManager:    pxyManager,
			PluginManager: manager,
			LoginMsg: &msg.Login{
				User:     "metric-user",
				RunID:    "metric-run-id",
				ClientID: "metric-client-id",
			},
			ServerCfg: &v1.ServerConfig{},
		},
		xl: xlog.FromContextSafe(context.Background()),
	}

	ctl.addRegisteredProxy(pxy, plugin.UserInfo{RunID: "effective-run-id"}, attemptID)
	registered, ok := ctl.proxies[proxyName]
	if !ok || !registered.metricsRegistered {
		t.Fatalf("registered proxy = %+v, want metrics-recorded local admission", registered)
	}
	if recorder.newProxyCalls != 1 || recorder.closeProxyCalls != 0 || recorder.proxyCount != 1 {
		t.Fatalf("metrics after local admission = new %d close %d count %d, want 1/0/1",
			recorder.newProxyCalls, recorder.closeProxyCalls, recorder.proxyCount)
	}
	if recorder.name != proxyName || recorder.proxyType != "tcp" ||
		recorder.user != "metric-user" || recorder.clientID != "metric-client-id" {
		t.Fatalf("NewProxy metric identity = name %q type %q user %q client %q",
			recorder.name, recorder.proxyType, recorder.user, recorder.clientID)
	}

	finalErr, resultErr := confirmNewProxyResult(
		manager,
		&plugin.NewProxyResultContent{
			User:      plugin.UserInfo{RunID: "effective-run-id"},
			AttemptID: attemptID,
			ProxyName: proxyName,
			Admitted:  true,
		},
		nil,
		func() error { return ctl.CloseProxy(&msg.CloseProxy{ProxyName: proxyName}) },
	)
	if finalErr == nil || resultErr == nil {
		t.Fatalf("confirmation errors = final %v, result %v; want rejection and rollback", finalErr, resultErr)
	}
	if recorder.newProxyCalls != 1 || recorder.closeProxyCalls != 1 || recorder.proxyCount != 0 {
		t.Fatalf("metrics after rollback = new %d close %d count %d, want 1/1/0",
			recorder.newProxyCalls, recorder.closeProxyCalls, recorder.proxyCount)
	}
}

func TestProcessNewProxyAdmissionDefersResultPluginIO(t *testing.T) {
	t.Parallel()

	const attemptID = "0123456789abcdef0123456789abcdef"
	resultCalled := false
	manager := plugin.NewManager()
	manager.Register(&controlResultPlugin{handle: func(op string, _ any) (*plugin.Response, any, error) {
		if op == plugin.OpNewProxyResult {
			resultCalled = true
		}
		return &plugin.Response{Unchange: true}, nil, nil
	}})

	effective, remoteAddr, resultContent, admissionErr := processNewProxyAdmissionWithIDGenerator(
		manager,
		newResultTestContent("0123456789abcdef", "proxy-async-result"),
		func(*msg.NewProxy, plugin.UserInfo, string) (string, error) {
			return "127.0.0.1:8080", nil
		},
		func() (string, error) { return attemptID, nil },
	)
	if admissionErr != nil {
		t.Fatalf("admission error = %v", admissionErr)
	}
	if resultCalled {
		t.Fatal("admission helper performed synchronous NewProxyResult plugin I/O")
	}
	if effective.ProxyName != "proxy-async-result" || remoteAddr != "127.0.0.1:8080" {
		t.Fatalf("effective = %+v, remoteAddr = %q", effective, remoteAddr)
	}
	if resultContent == nil || resultContent.AttemptID != attemptID || !resultContent.Admitted {
		t.Fatalf("result content = %+v, want immutable admitted result", resultContent)
	}
}

func TestCloseProxyKeepsOldAttemptIDWhenReplacementUsesSameIdentity(t *testing.T) {
	t.Parallel()

	const (
		runID      = "0123456789abcdef"
		proxyName  = "proxy-exact"
		oldAttempt = "0123456789abcdef0123456789abcdef"
		newAttempt = "fedcba9876543210fedcba9876543210"
	)
	oldStarted := make(chan struct{})
	releaseOld := make(chan struct{})
	var releaseOldOnce sync.Once
	delivered := make(chan plugin.CloseProxyContent, 2)

	manager := plugin.NewManager()
	t.Cleanup(func() {
		releaseOldOnce.Do(func() { close(releaseOld) })
		manager.Close()
	})
	manager.Register(&controlResultPlugin{handle: func(_ string, content any) (*plugin.Response, any, error) {
		got := content.(plugin.CloseProxyContent)
		if got.AttemptID == oldAttempt {
			close(oldStarted)
			<-releaseOld
		}
		delivered <- got
		return &plugin.Response{Unchange: true}, nil, nil
	}})

	newControl := func() *Control {
		return &Control{sessionCtx: &SessionContext{
			PxyManager:    proxy.NewManager(),
			PluginManager: manager,
			LoginMsg:      &msg.Login{RunID: runID},
		}}
	}
	oldControl := newControl()
	replacementControl := newControl()

	if err := oldControl.closeProxy(newRegisteredProxy(
		newCloseResultTestProxy(proxyName),
		plugin.UserInfo{RunID: runID},
		oldAttempt,
	)); err != nil {
		t.Fatalf("close old proxy: %v", err)
	}
	select {
	case <-oldStarted:
	case <-time.After(time.Second):
		t.Fatal("old CloseProxy callback did not start")
	}

	if err := replacementControl.closeProxy(newRegisteredProxy(
		newCloseResultTestProxy(proxyName),
		plugin.UserInfo{RunID: runID},
		newAttempt,
	)); err != nil {
		t.Fatalf("close replacement proxy: %v", err)
	}
	select {
	case got := <-delivered:
		t.Fatalf("CloseProxy callback %q overtook blocked old callback", got.AttemptID)
	case <-time.After(50 * time.Millisecond):
	}

	releaseOldOnce.Do(func() { close(releaseOld) })
	select {
	case got := <-delivered:
		if got.User.RunID != runID || got.ProxyName != proxyName || got.AttemptID != oldAttempt {
			t.Fatalf("delayed old CloseProxy = %+v, want exact old identity", got)
		}
	case <-time.After(time.Second):
		t.Fatal("delayed old CloseProxy callback was not delivered")
	}
	select {
	case got := <-delivered:
		if got.User.RunID != runID || got.ProxyName != proxyName || got.AttemptID != newAttempt {
			t.Fatalf("replacement CloseProxy = %+v, want exact replacement identity", got)
		}
	case <-time.After(time.Second):
		t.Fatal("replacement CloseProxy callback was not delivered after old callback")
	}
}

func TestNewRegisteredProxyCopiesEffectiveUserIdentity(t *testing.T) {
	t.Parallel()

	metas := map[string]string{"identity": "old"}
	registered := newRegisteredProxy(
		newCloseResultTestProxy("proxy-exact"),
		plugin.UserInfo{User: "user", Metas: metas, RunID: "effective-run-id"},
		"0123456789abcdef0123456789abcdef",
	)
	metas["identity"] = "new"

	if registered.user.User != "user" || registered.user.RunID != "effective-run-id" ||
		registered.user.Metas["identity"] != "old" {
		t.Fatalf("registered user = %+v, want immutable effective identity", registered.user)
	}
}

func TestProcessNewProxyAttemptReportsRegisterFailureOnce(t *testing.T) {
	t.Parallel()

	registerErr := errors.New("proxy registration failed")
	resultCount := 0
	var preparedAttemptID string
	manager := plugin.NewManager()
	manager.Register(&controlResultPlugin{handle: func(op string, content any) (*plugin.Response, any, error) {
		switch op {
		case plugin.OpNewProxy:
			preparedAttemptID = content.(plugin.NewProxyContent).AttemptID
			assertValidNewProxyAttemptID(t, preparedAttemptID)
		case plugin.OpNewProxyResult:
			resultCount++
			result := content.(plugin.NewProxyResultContent)
			if result.User.RunID != "fedcba9876543210" || result.AttemptID != preparedAttemptID || result.ProxyName != "proxy-failed" || result.Admitted {
				t.Fatalf("result = %+v, want exact failed identity", result)
			}
		}
		return &plugin.Response{Unchange: true}, nil, nil
	}})

	_, _, admissionErr, resultErr := processNewProxyAttempt(
		manager,
		newResultTestContent("fedcba9876543210", "proxy-failed"),
		func(*msg.NewProxy, plugin.UserInfo, string) (string, error) { return "", registerErr },
	)
	if !errors.Is(admissionErr, registerErr) || resultErr != nil {
		t.Fatalf("errors = admission %v, result %v", admissionErr, resultErr)
	}
	if resultCount != 1 {
		t.Fatalf("NewProxyResult count = %d, want 1", resultCount)
	}
}

func TestProcessNewProxyAttemptReportsPluginRejectionWithoutRegistering(t *testing.T) {
	t.Parallel()

	registerCalled := false
	resultCount := 0
	var preparedAttemptID string
	manager := plugin.NewManager()
	manager.Register(&controlResultPlugin{handle: func(op string, content any) (*plugin.Response, any, error) {
		switch op {
		case plugin.OpNewProxy:
			preparedAttemptID = content.(plugin.NewProxyContent).AttemptID
			assertValidNewProxyAttemptID(t, preparedAttemptID)
			return &plugin.Response{Reject: true, RejectReason: "denied"}, nil, nil
		case plugin.OpNewProxyResult:
			resultCount++
			result := content.(plugin.NewProxyResultContent)
			if result.User.RunID != "0123456789abcdef" || result.AttemptID != preparedAttemptID || result.ProxyName != "proxy-denied" || result.Admitted {
				t.Fatalf("result = %+v, want original rejected identity", result)
			}
			return &plugin.Response{}, nil, nil
		default:
			t.Fatalf("unexpected op %q", op)
			return nil, nil, nil
		}
	}})

	_, _, admissionErr, resultErr := processNewProxyAttempt(
		manager,
		newResultTestContent("0123456789abcdef", "proxy-denied"),
		func(*msg.NewProxy, plugin.UserInfo, string) (string, error) {
			registerCalled = true
			return "", nil
		},
	)
	if admissionErr == nil || admissionErr.Error() != "denied" || resultErr != nil {
		t.Fatalf("errors = admission %v, result %v; want denied and nil", admissionErr, resultErr)
	}
	if registerCalled {
		t.Fatal("register called after plugin rejection")
	}
	if resultCount != 1 {
		t.Fatalf("NewProxyResult count = %d, want 1", resultCount)
	}
}

func TestProcessNewProxyAttemptUsesEffectivePluginIdentity(t *testing.T) {
	t.Parallel()

	var preparedAttemptID string
	manager := plugin.NewManager()
	manager.Register(&controlResultPlugin{handle: func(op string, content any) (*plugin.Response, any, error) {
		switch op {
		case plugin.OpNewProxy:
			modified := content.(plugin.NewProxyContent)
			preparedAttemptID = modified.AttemptID
			assertValidNewProxyAttemptID(t, preparedAttemptID)
			modified.User.RunID = "effective-run-id"
			modified.ProxyName = "effective-proxy"
			modified.AttemptID = "aliased-attempt"
			return &plugin.Response{Unchange: false}, &modified, nil
		case plugin.OpNewProxyResult:
			result := content.(plugin.NewProxyResultContent)
			if result.User.RunID != "effective-run-id" || result.AttemptID != preparedAttemptID || result.ProxyName != "effective-proxy" || !result.Admitted {
				t.Fatalf("result = %+v, want effective admitted identity", result)
			}
			return &plugin.Response{}, nil, nil
		default:
			t.Fatalf("unexpected op %q", op)
			return nil, nil, nil
		}
	}})

	effective, _, admissionErr, resultErr := processNewProxyAttempt(
		manager,
		newResultTestContent("original-run-id", "original-proxy"),
		func(got *msg.NewProxy, gotUser plugin.UserInfo, gotAttemptID string) (string, error) {
			if got.ProxyName != "effective-proxy" {
				t.Fatalf("register ProxyName = %q, want effective-proxy", got.ProxyName)
			}
			if gotAttemptID != preparedAttemptID {
				t.Fatalf("register AttemptID = %q, want immutable %q", gotAttemptID, preparedAttemptID)
			}
			if gotUser.RunID != "effective-run-id" {
				t.Fatalf("register RunID = %q, want effective-run-id", gotUser.RunID)
			}
			return "", nil
		},
	)
	if admissionErr != nil || resultErr != nil {
		t.Fatalf("errors = admission %v, result %v", admissionErr, resultErr)
	}
	if effective.ProxyName != "effective-proxy" {
		t.Fatalf("effective ProxyName = %q", effective.ProxyName)
	}
}

func TestProcessNewProxyAttemptIDsAreFresh(t *testing.T) {
	t.Parallel()

	manager := plugin.NewManager()
	var prepared []string
	var results []string
	manager.Register(&controlResultPlugin{handle: func(op string, content any) (*plugin.Response, any, error) {
		switch op {
		case plugin.OpNewProxy:
			attemptID := content.(plugin.NewProxyContent).AttemptID
			assertValidNewProxyAttemptID(t, attemptID)
			prepared = append(prepared, attemptID)
			return &plugin.Response{Unchange: true}, nil, nil
		case plugin.OpNewProxyResult:
			attemptID := content.(plugin.NewProxyResultContent).AttemptID
			assertValidNewProxyAttemptID(t, attemptID)
			results = append(results, attemptID)
			return &plugin.Response{}, nil, nil
		default:
			t.Fatalf("unexpected op %q", op)
			return nil, nil, nil
		}
	}})

	for _, proxyName := range []string{"proxy-first", "proxy-second"} {
		_, _, admissionErr, resultErr := processNewProxyAttempt(
			manager,
			newResultTestContent("0123456789abcdef", proxyName),
			func(*msg.NewProxy, plugin.UserInfo, string) (string, error) { return "", nil },
		)
		if admissionErr != nil || resultErr != nil {
			t.Fatalf("processNewProxyAttempt(%q) errors = admission %v, result %v", proxyName, admissionErr, resultErr)
		}
	}
	if len(prepared) != 2 || len(results) != 2 || prepared[0] != results[0] || prepared[1] != results[1] {
		t.Fatalf("attempt correlation = prepared %v, results %v", prepared, results)
	}
	if prepared[0] == prepared[1] {
		t.Fatalf("attempt IDs were reused: %q", prepared[0])
	}
}

func TestProcessNewProxyAttemptIDGenerationFailureFailsClosed(t *testing.T) {
	t.Parallel()

	generationErr := errors.New("entropy unavailable")
	pluginCalled := false
	registerCalled := false
	manager := plugin.NewManager()
	manager.Register(&controlResultPlugin{handle: func(string, any) (*plugin.Response, any, error) {
		pluginCalled = true
		return &plugin.Response{Unchange: true}, nil, nil
	}})
	content := newResultTestContent("0123456789abcdef", "proxy-no-id")
	effective, remoteAddr, admissionErr, resultErr := processNewProxyAttemptWithIDGenerator(
		manager,
		content,
		func(*msg.NewProxy, plugin.UserInfo, string) (string, error) {
			registerCalled = true
			return "", nil
		},
		func() (string, error) { return "", generationErr },
	)
	if !errors.Is(admissionErr, generationErr) || !strings.Contains(admissionErr.Error(), "generate NewProxy attempt ID") {
		t.Fatalf("admission error = %v, want explicit wrapped generation error", admissionErr)
	}
	if resultErr != nil || remoteAddr != "" || effective.ProxyName != "proxy-no-id" {
		t.Fatalf("result = effective %+v, remoteAddr %q, resultErr %v", effective, remoteAddr, resultErr)
	}
	if content.AttemptID != "" || pluginCalled || registerCalled {
		t.Fatalf("generation failure side effects: attemptID=%q pluginCalled=%v registerCalled=%v", content.AttemptID, pluginCalled, registerCalled)
	}
}

func TestGenerateNewProxyAttemptIDFormatAndUniqueness(t *testing.T) {
	t.Parallel()

	seen := make(map[string]struct{}, 64)
	for range 64 {
		attemptID, err := generateNewProxyAttemptID()
		if err != nil {
			t.Fatalf("generateNewProxyAttemptID() error = %v", err)
		}
		assertValidNewProxyAttemptID(t, attemptID)
		if _, duplicate := seen[attemptID]; duplicate {
			t.Fatalf("generateNewProxyAttemptID() reused %q", attemptID)
		}
		seen[attemptID] = struct{}{}
	}
}

func TestProcessNewProxyAttemptReturnsConfirmationErrorSeparately(t *testing.T) {
	t.Parallel()

	confirmErr := errors.New("confirmation unavailable at secret-plugin.internal")
	registerErr := errors.New("registration unavailable")
	for _, tc := range []struct {
		name        string
		registerErr error
	}{
		{name: "successful registration still requires confirmation"},
		{name: "failed registration keeps original error", registerErr: registerErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := plugin.NewManager()
			manager.Register(&controlResultPlugin{handle: func(op string, _ any) (*plugin.Response, any, error) {
				if op == plugin.OpNewProxyResult {
					return nil, nil, confirmErr
				}
				if op == plugin.OpCloseProxy {
					t.Fatal("failed NewProxy attempt emitted synthetic CloseProxy")
				}
				return &plugin.Response{Unchange: true}, nil, nil
			}})

			_, _, admissionErr, resultErr := processNewProxyAttempt(
				manager,
				newResultTestContent("0123456789abcdef", "proxy-notify-error"),
				func(*msg.NewProxy, plugin.UserInfo, string) (string, error) { return "", tc.registerErr },
			)
			if !errors.Is(admissionErr, tc.registerErr) {
				t.Fatalf("admission error = %v, want %v", admissionErr, tc.registerErr)
			}
			if resultErr == nil || !strings.Contains(resultErr.Error(), "result plugin delivery failed") ||
				strings.Contains(resultErr.Error(), confirmErr.Error()) {
				t.Fatalf("result error = %v, want sanitized confirmation failure", resultErr)
			}
		})
	}
}

func newResultTestContent(runID, proxyName string) *plugin.NewProxyContent {
	return &plugin.NewProxyContent{
		User: plugin.UserInfo{RunID: runID},
		NewProxy: msg.NewProxy{
			ProxyName: proxyName,
			ProxyType: "tcp",
		},
	}
}

func assertValidNewProxyAttemptID(t *testing.T, attemptID string) {
	t.Helper()
	if len(attemptID) != 32 || attemptID != strings.ToLower(attemptID) {
		t.Fatalf("AttemptID = %q, want 32 lowercase hexadecimal characters", attemptID)
	}
	if _, err := hex.DecodeString(attemptID); err != nil {
		t.Fatalf("AttemptID = %q, want hexadecimal: %v", attemptID, err)
	}
}
