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
	"encoding/json"
	"testing"

	"github.com/fatedier/frp/pkg/msg"
)

func TestNewProxyContentAttemptIDWireFormat(t *testing.T) {
	t.Parallel()

	content := NewProxyContent{
		User:      UserInfo{RunID: "0123456789abcdef"},
		AttemptID: "0123456789abcdef0123456789abcdef",
		NewProxy:  msg.NewProxy{ProxyName: "proxy-exact", ProxyType: "tcp"},
	}
	got, err := json.Marshal(content)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	const want = `{"user":{"user":"","metas":null,"run_id":"0123456789abcdef"},` +
		`"attempt_id":"0123456789abcdef0123456789abcdef","proxy_name":"proxy-exact","proxy_type":"tcp"}`
	if string(got) != want {
		t.Fatalf("NewProxyContent JSON = %s, want %s", got, want)
	}
}

func TestNewProxyResultContentWireFormat(t *testing.T) {
	t.Parallel()

	content := NewProxyResultContent{
		User: UserInfo{
			User:  "exact-user",
			Metas: map[string]string{"key": "value"},
			RunID: " 0123456789abcdef ",
		},
		AttemptID: "0123456789abcdef0123456789abcdef",
		ProxyName: " proxy-Exact ",
		Admitted:  false,
	}
	got, err := json.Marshal(content)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	const want = `{"user":{"user":"exact-user","metas":{"key":"value"},"run_id":" 0123456789abcdef "},` +
		`"attempt_id":"0123456789abcdef0123456789abcdef","proxy_name":" proxy-Exact ","admitted":false}`
	if string(got) != want {
		t.Fatalf("NewProxyResultContent JSON = %s, want %s", got, want)
	}
}

func TestCloseProxyContentAttemptIDWireFormat(t *testing.T) {
	t.Parallel()

	content := CloseProxyContent{
		User: UserInfo{
			User:  "exact-user",
			Metas: map[string]string{"key": "value"},
			RunID: "0123456789abcdef",
		},
		AttemptID: "fedcba9876543210fedcba9876543210",
		CloseProxy: msg.CloseProxy{
			ProxyName: "proxy-exact",
		},
	}
	got, err := json.Marshal(content)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	const want = `{"user":{"user":"exact-user","metas":{"key":"value"},"run_id":"0123456789abcdef"},` +
		`"attempt_id":"fedcba9876543210fedcba9876543210","proxy_name":"proxy-exact"}`
	if string(got) != want {
		t.Fatalf("CloseProxyContent JSON = %s, want %s", got, want)
	}
}
