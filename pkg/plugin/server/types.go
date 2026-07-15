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
	"maps"

	"github.com/fatedier/frp/pkg/msg"
)

type Request struct {
	Version string `json:"version"`
	Op      string `json:"op"`
	Content any    `json:"content"`
}

type Response struct {
	Reject       bool   `json:"reject"`
	RejectReason string `json:"reject_reason"`
	Unchange     bool   `json:"unchange"`
	Content      any    `json:"content"`
}

type LoginContent struct {
	msg.Login

	ClientAddress string `json:"client_address,omitempty"`
}

type UserInfo struct {
	User  string            `json:"user"`
	Metas map[string]string `json:"metas"`
	RunID string            `json:"run_id"`
}

// Clone returns an independent copy of the plugin identity. Metas is mutable,
// so lifecycle callbacks and stored proxy records must not retain a caller's
// map even though the scalar identity fields can be copied directly.
func (u UserInfo) Clone() UserInfo {
	u.Metas = maps.Clone(u.Metas)
	return u
}

type NewProxyContent struct {
	User UserInfo `json:"user"`
	// AttemptID is immutable FRPS-owned correlation state. It is generated
	// before the first NewProxy plugin call and repeated in NewProxyResult.
	AttemptID string `json:"attempt_id"`
	msg.NewProxy
}

// NewProxyResultContent reports the final FRPS admission outcome for one
// NewProxy attempt after the NewProxy plugin chain has run. AttemptID is the
// immutable, cryptographically random identifier FRPS minted before invoking
// the NewProxy plugin chain. User.RunID and ProxyName are preserved byte-for-
// byte from the effective NewProxy content (or the original content when the
// plugin chain rejects it). Admitted is true only after RegisterProxy completes
// successfully.
//
// This is a notification, not another admission hook: plugin responses cannot
// change the outcome. Consumers that prepare external state during NewProxy can
// use the exact identity and outcome to commit or roll that state back without
// treating a failed admission as a CloseProxy event.
type NewProxyResultContent struct {
	User      UserInfo `json:"user"`
	AttemptID string   `json:"attempt_id"`
	ProxyName string   `json:"proxy_name"`
	Admitted  bool     `json:"admitted"`
}

type CloseProxyContent struct {
	User UserInfo `json:"user"`
	// AttemptID is the immutable FRPS-owned identifier minted for the
	// successful NewProxy attempt that created this proxy. It lets consumers
	// distinguish a delayed teardown from a replacement proxy with the same
	// RunID and ProxyName.
	AttemptID string `json:"attempt_id"`
	msg.CloseProxy
}

type PingContent struct {
	User UserInfo `json:"user"`
	msg.Ping
}

type NewWorkConnContent struct {
	User UserInfo `json:"user"`
	msg.NewWorkConn
}

type NewUserConnContent struct {
	User       UserInfo `json:"user"`
	ProxyName  string   `json:"proxy_name"`
	ProxyType  string   `json:"proxy_type"`
	RemoteAddr string   `json:"remote_addr"`
}
