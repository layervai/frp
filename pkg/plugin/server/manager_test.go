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
)

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

func TestManagerNewProxyResultIgnoresResponsesAndNotifiesAllPlugins(t *testing.T) {
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
				// Notification responses are deliberately non-authoritative.
				return &Response{Reject: true, RejectReason: "ignored", Unchange: false}, nil, nil
			},
		})
	}

	err := manager.NewProxyResult(&NewProxyResultContent{
		User:      UserInfo{RunID: "0123456789abcdef"},
		AttemptID: "0123456789abcdef0123456789abcdef",
		ProxyName: "proxy-exact",
		Admitted:  true,
	})
	if err != nil {
		t.Fatalf("NewProxyResult() error = %v", err)
	}
	if !slices.Equal(called, []string{"first", "second"}) {
		t.Fatalf("notification order = %v, want [first second]", called)
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

func TestManagerNewProxyResultAggregatesErrorsWithoutShortCircuit(t *testing.T) {
	t.Parallel()

	manager := NewManager()
	var called []string
	for _, name := range []string{"first", "second"} {
		manager.Register(&resultTestPlugin{
			name:      name,
			supported: []string{OpNewProxyResult},
			handle: func(_ context.Context, _ string, _ any) (*Response, any, error) {
				called = append(called, name)
				return nil, nil, errors.New("delivery failed")
			},
		})
	}

	err := manager.NewProxyResult(&NewProxyResultContent{})
	if err == nil {
		t.Fatal("NewProxyResult() error = nil, want aggregate delivery error")
	}
	for _, want := range []string{"send NewProxyResult request to plugin errors", "[first]: delivery failed", "[second]: delivery failed"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("NewProxyResult() error = %q, want substring %q", err, want)
		}
	}
	if !slices.Equal(called, []string{"first", "second"}) {
		t.Fatalf("notification order = %v, want [first second]", called)
	}
}
