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
	"context"
	"net"
	"sync"

	"github.com/fatedier/frp/pkg/msg"
)

// A proxy plugin can retain its connection after Handle returns, so count Close,
// not handler returns. Unassigned work-pool connections never enter this set.
type workConnTracker struct {
	mu       sync.Mutex
	active   map[*drainingWorkConn]string
	revoked  map[string]struct{}
	changed  chan struct{}
	draining bool
	forced   bool
}

type drainingWorkConn struct {
	net.Conn
	tracker *workConnTracker
	once    sync.Once
	err     error
}

func (t *workConnTracker) track(conn *msg.Conn, proxyName string) bool {
	t.mu.Lock()
	_, revoked := t.revoked[proxyName]
	if t.draining || t.forced || revoked {
		t.mu.Unlock()
		_ = conn.Close()
		return false
	}
	if t.active == nil {
		t.active = make(map[*drainingWorkConn]string)
		t.changed = make(chan struct{})
	}
	tracked := &drainingWorkConn{Conn: conn.Conn, tracker: t}
	t.active[tracked] = proxyName
	// Keep msg.Conn and its negotiated codec visible to UDP and custom handlers.
	conn.Conn = tracked
	t.mu.Unlock()
	return true
}

func (c *drainingWorkConn) Context() context.Context {
	if conn, ok := c.Conn.(interface{ Context() context.Context }); ok {
		return conn.Context()
	}
	return context.Background()
}

func (c *drainingWorkConn) WithContext(ctx context.Context) {
	if conn, ok := c.Conn.(interface{ WithContext(context.Context) }); ok {
		conn.WithContext(ctx)
	}
}

func (c *drainingWorkConn) Close() error {
	c.once.Do(func() {
		c.err = c.Conn.Close()
		c.tracker.mu.Lock()
		delete(c.tracker.active, c)
		if c.tracker.draining {
			close(c.tracker.changed)
			c.tracker.changed = make(chan struct{})
		}
		c.tracker.mu.Unlock()
	})
	return c.err
}

func (t *workConnTracker) beginDrain() {
	t.mu.Lock()
	t.draining = true
	t.mu.Unlock()
}

func (t *workConnTracker) isDraining() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.draining
}

func (t *workConnTracker) wait(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		t.mu.Lock()
		forced, count, changed := t.forced, len(t.active), t.changed
		t.mu.Unlock()
		if forced {
			return net.ErrClosed
		}
		if count == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (t *workConnTracker) closeAll() {
	t.mu.Lock()
	t.forced = true
	t.draining = true
	conns := make([]*drainingWorkConn, 0, len(t.active))
	for conn := range t.active {
		conns = append(conns, conn)
	}
	t.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

func (t *workConnTracker) closeProxy(proxyName string) {
	t.mu.Lock()
	if t.revoked == nil {
		t.revoked = make(map[string]struct{})
	}
	t.revoked[proxyName] = struct{}{}
	var conns []*drainingWorkConn
	for conn, name := range t.active {
		if name == proxyName {
			conns = append(conns, conn)
		}
	}
	t.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

func (t *workConnTracker) clearRevocations() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.revoked = nil
}
