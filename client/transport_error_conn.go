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
	"net"
	"sync"
	"sync/atomic"
)

// transportErrorConn observes the socket before multiplexing hides its error.
type transportErrorConn struct {
	net.Conn
	report func(string, error)
	once   sync.Once
	closed atomic.Bool
}

func (c *transportErrorConn) observe(operation string, err error) {
	if err != nil && !c.closed.Load() {
		c.once.Do(func() { c.report(operation, err) })
	}
}

func (c *transportErrorConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.observe("read", err)
	return n, err
}

func (c *transportErrorConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.observe("write", err)
	return n, err
}

func (c *transportErrorConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}
