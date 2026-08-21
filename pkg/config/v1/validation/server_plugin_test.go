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
