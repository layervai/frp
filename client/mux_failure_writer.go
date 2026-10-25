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

package client

import (
	"strings"

	"github.com/fatedier/frp/pkg/util/xlog"
)

type muxFailureWriter struct{ logger *xlog.Logger }

func (w muxFailureWriter) Write(p []byte) (int, error) {
	message := strings.TrimSpace(string(p))
	// Per-stream races stay quiet; only errors that end the session need WARN.
	for _, cause := range []string{
		"keepalive failed:",
		"aborted stream open ",
		"Invalid protocol version:",
		"received protocol error go away",
		"received internal error go away",
		"received unexpected go away",
	} {
		if strings.Contains(message, "[ERR] yamux: "+cause) {
			w.logger.Warnf("%s", message)
			return len(p), nil
		}
	}
	w.logger.Tracef("%s", message)
	return len(p), nil
}
