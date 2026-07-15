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
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/fatedier/frp/pkg/msg"
	plugin "github.com/fatedier/frp/pkg/plugin/server"
)

type controlResultPlugin struct {
	handle func(string, any) (*plugin.Response, any, error)
}

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
	var result plugin.NewProxyResultContent
	manager := plugin.NewManager()
	manager.Register(&controlResultPlugin{handle: func(op string, content any) (*plugin.Response, any, error) {
		switch op {
		case plugin.OpNewProxy:
			order = append(order, "plugin")
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
		func(got *msg.NewProxy) (string, error) {
			order = append(order, "register")
			if got.ProxyName != "proxy-Exact" {
				t.Fatalf("register ProxyName = %q, want exact input", got.ProxyName)
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
	if result.User.RunID != "0123456789abcdef" || result.ProxyName != "proxy-Exact" || !result.Admitted {
		t.Fatalf("result = %+v, want exact admitted identity", result)
	}
	if !slices.Equal(order, []string{"plugin", "register", "result"}) {
		t.Fatalf("order = %v, want [plugin register result]", order)
	}
}

func TestProcessNewProxyAttemptReportsRegisterFailureOnce(t *testing.T) {
	t.Parallel()

	registerErr := errors.New("proxy registration failed")
	resultCount := 0
	manager := plugin.NewManager()
	manager.Register(&controlResultPlugin{handle: func(op string, content any) (*plugin.Response, any, error) {
		if op == plugin.OpNewProxyResult {
			resultCount++
			result := content.(plugin.NewProxyResultContent)
			if result.User.RunID != "fedcba9876543210" || result.ProxyName != "proxy-failed" || result.Admitted {
				t.Fatalf("result = %+v, want exact failed identity", result)
			}
		}
		return &plugin.Response{Unchange: true}, nil, nil
	}})

	_, _, admissionErr, resultErr := processNewProxyAttempt(
		manager,
		newResultTestContent("fedcba9876543210", "proxy-failed"),
		func(*msg.NewProxy) (string, error) { return "", registerErr },
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
	manager := plugin.NewManager()
	manager.Register(&controlResultPlugin{handle: func(op string, content any) (*plugin.Response, any, error) {
		switch op {
		case plugin.OpNewProxy:
			return &plugin.Response{Reject: true, RejectReason: "denied"}, nil, nil
		case plugin.OpNewProxyResult:
			resultCount++
			result := content.(plugin.NewProxyResultContent)
			if result.User.RunID != "0123456789abcdef" || result.ProxyName != "proxy-denied" || result.Admitted {
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
		func(*msg.NewProxy) (string, error) {
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

	manager := plugin.NewManager()
	manager.Register(&controlResultPlugin{handle: func(op string, content any) (*plugin.Response, any, error) {
		switch op {
		case plugin.OpNewProxy:
			modified := content.(plugin.NewProxyContent)
			modified.User.RunID = "effective-run-id"
			modified.ProxyName = "effective-proxy"
			return &plugin.Response{Unchange: false}, &modified, nil
		case plugin.OpNewProxyResult:
			result := content.(plugin.NewProxyResultContent)
			if result.User.RunID != "effective-run-id" || result.ProxyName != "effective-proxy" || !result.Admitted {
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
		func(got *msg.NewProxy) (string, error) {
			if got.ProxyName != "effective-proxy" {
				t.Fatalf("register ProxyName = %q, want effective-proxy", got.ProxyName)
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
				func(*msg.NewProxy) (string, error) { return "", tc.registerErr },
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
