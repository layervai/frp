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

package validation

import (
	"strings"
	"testing"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	plugin "github.com/fatedier/frp/pkg/plugin/server"
	"github.com/fatedier/frp/pkg/policy/security"
)

func TestValidateServerConfigAcceptsNewProxyResultPluginOp(t *testing.T) {
	t.Parallel()

	cfg := &v1.ServerConfig{
		Auth: v1.AuthServerConfig{Method: "token"},
		HTTPPlugins: []v1.HTTPPluginOptions{{
			Name: "result-observer",
			Ops:  []string{plugin.OpNewProxyResult, plugin.OpCloseProxy},
		}},
	}
	if err := cfg.Complete(); err != nil {
		t.Fatalf("ServerConfig.Complete() error = %v", err)
	}
	validator := NewConfigValidator(security.NewUnsafeFeatures(nil))
	if _, err := validator.ValidateServerConfig(cfg); err != nil {
		t.Fatalf("ValidateServerConfig() error = %v", err)
	}
}

func TestValidateServerConfigRejectsNewProxyResultWithoutCloseProxy(t *testing.T) {
	t.Parallel()

	cfg := &v1.ServerConfig{
		Auth: v1.AuthServerConfig{Method: "token"},
		HTTPPlugins: []v1.HTTPPluginOptions{{
			Name: "unsafe-result-observer",
			Ops:  []string{plugin.OpNewProxyResult},
		}},
	}
	if err := cfg.Complete(); err != nil {
		t.Fatalf("ServerConfig.Complete() error = %v", err)
	}
	validator := NewConfigValidator(security.NewUnsafeFeatures(nil))
	_, err := validator.ValidateServerConfig(cfg)
	if err == nil || !strings.Contains(err.Error(), "NewProxyResult requires CloseProxy for admission rollback") {
		t.Fatalf("ValidateServerConfig() error = %v, want rollback pairing requirement", err)
	}
}

func TestValidateServerConfigBoundsSynchronousNewProxyCallbacksByHeartbeat(t *testing.T) {
	t.Parallel()

	resultOnlyPlugin := v1.HTTPPluginOptions{
		Name: "result-only",
		Ops:  []string{plugin.OpNewProxyResult, plugin.OpCloseProxy},
	}
	fullAdmissionPlugin := v1.HTTPPluginOptions{
		Name: "full-admission",
		Ops:  []string{plugin.OpNewProxy, plugin.OpNewProxyResult, plugin.OpCloseProxy},
	}
	for _, test := range []struct {
		name             string
		tcpMux           bool
		heartbeatTimeout int64
		plugins          []v1.HTTPPluginOptions
		wantError        string
	}{
		{
			name:   "disabled heartbeat accepts callbacks",
			tcpMux: true, heartbeatTimeout: -1,
			plugins: []v1.HTTPPluginOptions{fullAdmissionPlugin},
		},
		{
			name:             "non mux heartbeat exceeds full admission budget",
			heartbeatTimeout: 51,
			plugins:          []v1.HTTPPluginOptions{fullAdmissionPlugin},
		},
		{
			name:             "non mux heartbeat equals full admission budget",
			heartbeatTimeout: 50,
			plugins:          []v1.HTTPPluginOptions{fullAdmissionPlugin},
			wantError:        "heartbeatTimeout 50s must exceed aggregate synchronous NewProxy callback budget 50s (1 NewProxy + 1 NewProxyResult HTTP callbacks)",
		},
		{
			name:   "mux mode with explicit heartbeat is still bounded",
			tcpMux: true, heartbeatTimeout: 25,
			plugins:   []v1.HTTPPluginOptions{resultOnlyPlugin},
			wantError: "heartbeatTimeout 25s must exceed aggregate synchronous NewProxy callback budget 25s (0 NewProxy + 1 NewProxyResult HTTP callbacks)",
		},
		{
			name:             "aggregate includes every result plugin",
			heartbeatTimeout: 50,
			plugins: []v1.HTTPPluginOptions{
				resultOnlyPlugin,
				{Name: "second-result", Ops: []string{plugin.OpNewProxyResult, plugin.OpCloseProxy}},
			},
			wantError: "heartbeatTimeout 50s must exceed aggregate synchronous NewProxy callback budget 50s (0 NewProxy + 2 NewProxyResult HTTP callbacks)",
		},
		{
			name:             "existing new proxy only config is unchanged",
			heartbeatTimeout: 1,
			plugins:          []v1.HTTPPluginOptions{{Name: "legacy-admission", Ops: []string{plugin.OpNewProxy}}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			tcpMux := test.tcpMux
			cfg := &v1.ServerConfig{
				Auth:        v1.AuthServerConfig{Method: "token"},
				HTTPPlugins: test.plugins,
				Transport: v1.ServerTransportConfig{
					TCPMux:           &tcpMux,
					HeartbeatTimeout: test.heartbeatTimeout,
				},
			}
			if err := cfg.Complete(); err != nil {
				t.Fatalf("ServerConfig.Complete() error = %v", err)
			}
			validator := NewConfigValidator(security.NewUnsafeFeatures(nil))
			_, err := validator.ValidateServerConfig(cfg)
			if test.wantError == "" {
				if err != nil {
					t.Fatalf("ValidateServerConfig() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("ValidateServerConfig() error = %v, want substring %q", err, test.wantError)
			}
		})
	}
}
