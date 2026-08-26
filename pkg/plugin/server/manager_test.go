// Copyright 2019 fatedier, fatedier@gmail.com
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
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	goliblog "github.com/fatedier/golib/log"

	"github.com/fatedier/frp/pkg/msg"
	frplog "github.com/fatedier/frp/pkg/util/log"
)

type testPlugin struct {
	name    string
	ops     map[string]bool
	handler func(context.Context, string, any) (*Response, any, error)
}

// Log-capturing subtests serialize global logger swaps; do not use t.Parallel.
var logCaptureMu sync.Mutex

type logCapture struct {
	bytes.Buffer
	levels []goliblog.Level
}

func (p testPlugin) Name() string {
	return p.name
}

func (p testPlugin) IsSupport(op string) bool {
	return p.ops[op]
}

func (p testPlugin) Handle(ctx context.Context, op string, content any) (*Response, any, error) {
	return p.handler(ctx, op, content)
}

func (w *logCapture) WriteLog(p []byte, level goliblog.Level, _ time.Time) (int, error) {
	w.levels = append(w.levels, level)
	return w.Write(p)
}

func captureLogOutput(t *testing.T) *logCapture {
	t.Helper()

	logCaptureMu.Lock()
	logOutput := &logCapture{}
	oldLogger := frplog.Logger
	frplog.Logger = goliblog.New(
		goliblog.WithOutput(logOutput),
		goliblog.WithLevel(goliblog.TraceLevel),
		goliblog.WithCaller(false),
	)
	t.Cleanup(func() {
		frplog.Logger = oldLogger
		logCaptureMu.Unlock()
	})
	return logOutput
}

var mutablePluginOps = []struct {
	name string
	op   string
}{
	{name: "login", op: OpLogin},
	{name: "new proxy", op: OpNewProxy},
	{name: "ping", op: OpPing},
	{name: "new work conn", op: OpNewWorkConn},
	{name: "new user conn", op: OpNewUserConn},
}

func callMutableWithUser(m *Manager, op string, user string) (string, error) {
	switch op {
	case OpLogin:
		got, err := m.Login(&LoginContent{Login: msg.Login{User: user}})
		if got == nil {
			return "", err
		}
		return got.User, err
	case OpNewProxy:
		got, err := m.NewProxy(&NewProxyContent{User: UserInfo{User: user}})
		if got == nil {
			return "", err
		}
		return got.User.User, err
	case OpPing:
		got, err := m.Ping(&PingContent{User: UserInfo{User: user}})
		if got == nil {
			return "", err
		}
		return got.User.User, err
	case OpNewWorkConn:
		got, err := m.NewWorkConn(&NewWorkConnContent{User: UserInfo{User: user}})
		if got == nil {
			return "", err
		}
		return got.User.User, err
	case OpNewUserConn:
		got, err := m.NewUserConn(&NewUserConnContent{User: UserInfo{User: user}})
		if got == nil {
			return "", err
		}
		return got.User.User, err
	default:
		panic("unsupported mutable op: " + op)
	}
}

func mutableUser(t *testing.T, op string, content any) string {
	t.Helper()

	switch op {
	case OpLogin:
		return content.(LoginContent).User
	case OpNewProxy:
		return content.(NewProxyContent).User.User
	case OpPing:
		return content.(PingContent).User.User
	case OpNewWorkConn:
		return content.(NewWorkConnContent).User.User
	case OpNewUserConn:
		return content.(NewUserConnContent).User.User
	default:
		t.Fatalf("unsupported mutable op: %s", op)
		return ""
	}
}

func mutateMutableContent(t *testing.T, op string, content any, user string) any {
	t.Helper()

	switch op {
	case OpLogin:
		got := content.(LoginContent)
		got.User = user
		return &got
	case OpNewProxy:
		got := content.(NewProxyContent)
		got.User.User = user
		return &got
	case OpPing:
		got := content.(PingContent)
		got.User.User = user
		return &got
	case OpNewWorkConn:
		got := content.(NewWorkConnContent)
		got.User.User = user
		return &got
	case OpNewUserConn:
		got := content.(NewUserConnContent)
		got.User.User = user
		return &got
	default:
		t.Fatalf("unsupported mutable op: %s", op)
		return nil
	}
}

func TestManagerMutableContentAcrossOps(t *testing.T) {
	for _, tt := range mutablePluginOps {
		t.Run(tt.name, func(t *testing.T) {
			m := NewManager()
			m.Register(testPlugin{
				name: "mutate",
				ops:  map[string]bool{tt.op: true},
				handler: func(ctx context.Context, op string, content any) (*Response, any, error) {
					if op != tt.op {
						t.Fatalf("unexpected op: %s", op)
					}
					if GetReqidFromContext(ctx) == "" {
						t.Fatal("expected request id in context")
					}
					if got := mutableUser(t, tt.op, content); got != "initial" {
						t.Fatalf("expected initial user, got %q", got)
					}
					return &Response{Unchange: false}, mutateMutableContent(t, tt.op, content, "mutated"), nil
				},
			})
			m.Register(testPlugin{
				name: "observe",
				ops:  map[string]bool{tt.op: true},
				handler: func(ctx context.Context, op string, content any) (*Response, any, error) {
					if op != tt.op {
						t.Fatalf("unexpected op: %s", op)
					}
					if GetReqidFromContext(ctx) == "" {
						t.Fatal("expected request id in context")
					}
					if got := mutableUser(t, tt.op, content); got != "mutated" {
						t.Fatalf("expected mutated user, got %q", got)
					}
					return &Response{Unchange: true}, mutateMutableContent(t, tt.op, content, "ignored"), nil
				},
			})

			got, err := callMutableWithUser(m, tt.op, "initial")
			if err != nil {
				t.Fatalf("mutable op failed: %v", err)
			}
			if got != "mutated" {
				t.Fatalf("expected mutated user, got %q", got)
			}
		})
	}
}

func TestManagerMutableContentRejectStopsChain(t *testing.T) {
	m := NewManager()

	var called bool
	m.Register(testPlugin{
		name: "reject",
		ops:  map[string]bool{OpPing: true},
		handler: func(context.Context, string, any) (*Response, any, error) {
			return &Response{Reject: true, RejectReason: "blocked"}, nil, nil
		},
	})
	m.Register(testPlugin{
		name: "unused",
		ops:  map[string]bool{OpPing: true},
		handler: func(context.Context, string, any) (*Response, any, error) {
			called = true
			return &Response{Unchange: true}, nil, nil
		},
	})

	got, err := m.Ping(&PingContent{})
	if err == nil {
		t.Fatal("expected reject error")
	}
	if got != nil {
		t.Fatalf("expected no returned content, got %#v", got)
	}
	if err.Error() != "blocked" {
		t.Fatalf("unexpected error: %v", err)
	}
	if called {
		t.Fatal("expected plugin chain to stop after reject")
	}
}

func TestManagerMutableContentPluginErrorLogLevel(t *testing.T) {
	tests := []struct {
		name  string
		op    string
		level goliblog.Level
	}{
		{name: "default warning", op: OpLogin, level: goliblog.WarnLevel},
		{name: "new user conn info", op: OpNewUserConn, level: goliblog.InfoLevel},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logOutput := captureLogOutput(t)
			m := NewManager()
			m.Register(testPlugin{
				name: "error",
				ops:  map[string]bool{tt.op: true},
				handler: func(context.Context, string, any) (*Response, any, error) {
					return nil, nil, errors.New("boom")
				},
			})

			_, err := callMutableWithUser(m, tt.op, "initial")
			if err == nil {
				t.Fatal("expected plugin error")
			}
			if want := "send " + tt.op + " request to plugin error"; err.Error() != want {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(logOutput.levels) != 1 || logOutput.levels[0] != tt.level {
				t.Fatalf("expected log level %v, got %v in %q", tt.level, logOutput.levels, logOutput.String())
			}
		})
	}
}

// TestManagerCloseProxyAggregatesErrors (upstream) is intentionally absent.
// It pins the synchronous CloseProxy contract -- that m.CloseProxy returns an
// aggregated error from every plugin. This fork replaces that contract with
// bounded asynchronous delivery and retry (see enqueueCloseProxy /
// runCloseProxyWorker), so CloseProxy no longer reports per-plugin failures to
// its caller; delivery outcomes are logged and covered by the close-proxy
// delivery tests below.

type resultTestPlugin struct {
	name      string
	supported []string
	handle    func(context.Context, string, any) (*Response, any, error)
}

func (p *resultTestPlugin) Name() string { return p.name }

func (p *resultTestPlugin) IsSupport(op string) bool {
	return slices.Contains(p.supported, op)
}

func (p *resultTestPlugin) Handle(ctx context.Context, op string, content any) (*Response, any, error) {
	return p.handle(ctx, op, content)
}

func TestDefaultCloseProxyRetryDelayIsCapped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		failureCount int
		want         time.Duration
	}{
		{failureCount: 1, want: 100 * time.Millisecond},
		{failureCount: 2, want: 200 * time.Millisecond},
		{failureCount: 6, want: 3200 * time.Millisecond},
		{failureCount: 7, want: 5 * time.Second},
		{failureCount: 1000, want: 5 * time.Second},
	}
	for _, tt := range tests {
		if got := defaultCloseProxyRetryDelay(tt.failureCount); got != tt.want {
			t.Fatalf("defaultCloseProxyRetryDelay(%d) = %s, want %s", tt.failureCount, got, tt.want)
		}
	}
}

func TestManagerNewProxyResultRejectsAdmissionAndNotifiesAllPlugins(t *testing.T) {
	t.Parallel()

	manager := NewManager()
	var called []string
	for _, name := range []string{"first", "second"} {
		manager.Register(&resultTestPlugin{
			name:      name,
			supported: []string{OpNewProxyResult},
			handle: func(_ context.Context, op string, content any) (*Response, any, error) {
				called = append(called, name)
				if op != OpNewProxyResult {
					t.Fatalf("op = %q, want %q", op, OpNewProxyResult)
				}
				got, ok := content.(NewProxyResultContent)
				if !ok {
					t.Fatalf("content type = %T, want NewProxyResultContent", content)
				}
				if got.User.RunID != "0123456789abcdef" || got.AttemptID != "0123456789abcdef0123456789abcdef" || got.ProxyName != "proxy-exact" || !got.Admitted {
					t.Fatalf("content = %+v, want exact admitted identity", got)
				}
				if name == "first" {
					return &Response{Reject: true, RejectReason: "external state not committed"}, nil, nil
				}
				return &Response{Unchange: true}, nil, nil
			},
		})
	}

	err := manager.NewProxyResult(&NewProxyResultContent{
		User:      UserInfo{RunID: "0123456789abcdef"},
		AttemptID: "0123456789abcdef0123456789abcdef",
		ProxyName: "proxy-exact",
		Admitted:  true,
	})
	if err == nil || !strings.Contains(err.Error(), "[first]: external state not committed") {
		t.Fatalf("NewProxyResult() error = %v, want first plugin rejection", err)
	}
	if !slices.Equal(called, []string{"first", "second"}) {
		t.Fatalf("notification order = %v, want [first second]", called)
	}
}

func TestManagerNewProxyResultAcceptsAllPluginConfirmations(t *testing.T) {
	t.Parallel()

	manager := NewManager()
	for _, name := range []string{"first", "second"} {
		manager.Register(&resultTestPlugin{
			name:      name,
			supported: []string{OpNewProxyResult},
			handle: func(context.Context, string, any) (*Response, any, error) {
				return &Response{Unchange: true}, nil, nil
			},
		})
	}

	if err := manager.NewProxyResult(&NewProxyResultContent{Admitted: true}); err != nil {
		t.Fatalf("NewProxyResult() error = %v", err)
	}
}

func TestManagerNewProxyPreservesAttemptIDAcrossMutationChain(t *testing.T) {
	t.Parallel()

	const attemptID = "0123456789abcdef0123456789abcdef"
	manager := NewManager()
	manager.Register(&resultTestPlugin{
		name:      "mutator",
		supported: []string{OpNewProxy},
		handle: func(_ context.Context, _ string, content any) (*Response, any, error) {
			modified := content.(NewProxyContent)
			modified.AttemptID = "aliased-attempt"
			modified.ProxyName = "modified-proxy"
			return &Response{Unchange: false}, &modified, nil
		},
	})
	manager.Register(&resultTestPlugin{
		name:      "observer",
		supported: []string{OpNewProxy},
		handle: func(_ context.Context, _ string, content any) (*Response, any, error) {
			got := content.(NewProxyContent)
			if got.AttemptID != attemptID {
				t.Fatalf("second plugin AttemptID = %q, want immutable %q", got.AttemptID, attemptID)
			}
			return &Response{Unchange: true}, nil, nil
		},
	})

	got, err := manager.NewProxy(&NewProxyContent{AttemptID: attemptID})
	if err != nil {
		t.Fatalf("NewProxy() error = %v", err)
	}
	if got.AttemptID != attemptID || got.ProxyName != "modified-proxy" {
		t.Fatalf("NewProxy() content = %+v, want immutable attempt ID and mutable proxy name", got)
	}
}

func TestManagerCloseProxyAttemptIDCannotBeRewrittenByPluginResponses(t *testing.T) {
	t.Parallel()

	const attemptID = "0123456789abcdef0123456789abcdef"
	manager := NewManager()
	t.Cleanup(manager.Close)
	observedAttemptIDs := make(chan string, 2)
	for _, name := range []string{"mutator", "observer"} {
		manager.Register(&resultTestPlugin{
			name:      name,
			supported: []string{OpCloseProxy},
			handle: func(_ context.Context, _ string, content any) (*Response, any, error) {
				got := content.(CloseProxyContent)
				observedAttemptIDs <- got.AttemptID
				got.AttemptID = "aliased-attempt"
				return &Response{Unchange: false}, &got, nil
			},
		})
	}

	content := &CloseProxyContent{AttemptID: attemptID}
	if err := manager.CloseProxy(content); err != nil {
		t.Fatalf("CloseProxy() error = %v", err)
	}
	if content.AttemptID != attemptID {
		t.Fatalf("CloseProxy() rewrote AttemptID to %q", content.AttemptID)
	}
	for range 2 {
		select {
		case got := <-observedAttemptIDs:
			if got != attemptID {
				t.Fatalf("CloseProxy plugin AttemptID = %q, want immutable %q", got, attemptID)
			}
		case <-time.After(time.Second):
			t.Fatal("CloseProxy notification was not delivered")
		}
	}
}

func TestManagerNewProxyResultAggregatesErrorsWithoutShortCircuit(t *testing.T) {
	t.Parallel()

	manager := NewManager()
	var called []string
	const sensitiveTransportError = "dial tcp 10.0.0.8:8443: certificate contains secret-internal-name"
	for _, name := range []string{"transport", "empty", "reject"} {
		manager.Register(&resultTestPlugin{
			name:      name,
			supported: []string{OpNewProxyResult},
			handle: func(_ context.Context, _ string, _ any) (*Response, any, error) {
				called = append(called, name)
				if name == "transport" {
					return nil, nil, errors.New(sensitiveTransportError)
				}
				if name == "empty" {
					return nil, nil, nil
				}
				return &Response{Reject: true, RejectReason: "routing registration rejected"}, nil, nil
			},
		})
	}

	err := manager.NewProxyResult(&NewProxyResultContent{})
	if err == nil {
		t.Fatal("NewProxyResult() error = nil, want aggregate delivery error")
	}
	for _, want := range []string{
		"send NewProxyResult request to plugin errors",
		"result plugin delivery failed",
		"[reject]: routing registration rejected",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("NewProxyResult() error = %q, want substring %q", err, want)
		}
	}
	if strings.Contains(err.Error(), sensitiveTransportError) {
		t.Fatalf("NewProxyResult() error = %q, leaked raw transport details", err)
	}
	if !slices.Equal(called, []string{"transport", "empty", "reject"}) {
		t.Fatalf("notification order = %v, want [transport empty reject]", called)
	}
}
