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
	"github.com/fatedier/frp/server/controller"
	"github.com/fatedier/frp/server/proxy"
)

type controlResultPlugin struct {
	handle func(string, any) (*plugin.Response, any, error)
}

type closeResultTestProxy struct {
	name       string
	configurer v1.ProxyConfigurer
}

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
			return &plugin.Response{Reject: true, RejectReason: "ignored"}, nil, nil
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

	oldControl.closeProxy(newRegisteredProxy(
		newCloseResultTestProxy(proxyName),
		plugin.UserInfo{RunID: runID},
		oldAttempt,
	))
	select {
	case <-oldStarted:
	case <-time.After(time.Second):
		t.Fatal("old CloseProxy callback did not start")
	}

	replacementControl.closeProxy(newRegisteredProxy(
		newCloseResultTestProxy(proxyName),
		plugin.UserInfo{RunID: runID},
		newAttempt,
	))
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

func TestProcessNewProxyAttemptNotificationErrorDoesNotRewriteAdmission(t *testing.T) {
	t.Parallel()

	notifyErr := errors.New("notification unavailable")
	registerErr := errors.New("registration unavailable")
	for _, tc := range []struct {
		name        string
		registerErr error
	}{
		{name: "successful admission remains successful"},
		{name: "failed admission keeps original error", registerErr: registerErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := plugin.NewManager()
			manager.Register(&controlResultPlugin{handle: func(op string, _ any) (*plugin.Response, any, error) {
				if op == plugin.OpNewProxyResult {
					return nil, nil, notifyErr
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
			if resultErr == nil || !strings.Contains(resultErr.Error(), notifyErr.Error()) {
				t.Fatalf("result error = %v, want %q", resultErr, notifyErr)
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
