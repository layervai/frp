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
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/msg"
)

func TestNewHTTPPluginOptionsSetsRequestTimeout(t *testing.T) {
	t.Parallel()

	for _, addr := range []string{"http://127.0.0.1:8080", "https://127.0.0.1:8443"} {
		plugin := NewHTTPPluginOptions(v1.HTTPPluginOptions{Addr: addr}).(*httpPlugin)
		if plugin.client.Timeout != HTTPPluginRequestTimeout {
			t.Fatalf("client timeout for %q = %s, want %s", addr, plugin.client.Timeout, HTTPPluginRequestTimeout)
		}
		if plugin.closeProxyClient == plugin.client {
			t.Fatalf("CloseProxy client for %q aliases general client", addr)
		}
		if plugin.closeProxyClient.Timeout != plugin.client.Timeout || plugin.closeProxyClient.Transport != plugin.client.Transport {
			t.Fatalf("CloseProxy client for %q does not share timeout/transport policy", addr)
		}
		if plugin.closeProxyClient.CheckRedirect == nil {
			t.Fatalf("CloseProxy client for %q has no terminal redirect policy", addr)
		}
	}
}

type cancellationRoundTripper struct {
	started chan struct{}
	done    chan struct{}
}

func (t *cancellationRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	close(t.started)
	<-req.Context().Done()
	close(t.done)
	return nil, req.Context().Err()
}

func TestHTTPPluginHandlePropagatesCancellationWithoutRoundTripTail(t *testing.T) {
	t.Parallel()

	transport := &cancellationRoundTripper{started: make(chan struct{}), done: make(chan struct{})}
	plugin := NewHTTPPluginOptions(v1.HTTPPluginOptions{Addr: "http://plugin.invalid"}).(*httpPlugin)
	plugin.client.Transport = transport
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, _, err := plugin.Handle(ctx, OpNewProxyResult, NewProxyResultContent{})
		result <- err
	}()

	select {
	case <-transport.started:
	case <-time.After(time.Second):
		t.Fatal("plugin request did not reach transport")
	}
	cancel()
	select {
	case err := <-result:
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("Handle() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Handle() did not return after context cancellation")
	}
	select {
	case <-transport.done:
	case <-time.After(time.Second):
		t.Fatal("HTTP round trip outlived canceled plugin request")
	}
}

type closeTrackingBody struct {
	io.Reader
	closed atomic.Bool
}

func (b *closeTrackingBody) Close() error {
	b.closed.Store(true)
	return nil
}

type staticRoundTripper struct {
	body *closeTrackingBody
}

func (t *staticRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       t.body,
	}, nil
}

func TestHTTPPluginDoClosesResponseBody(t *testing.T) {
	t.Parallel()

	body := &closeTrackingBody{Reader: strings.NewReader(`{"reject":false,"unchange":true,"content":{}}`)}
	plugin := NewHTTPPluginOptions(v1.HTTPPluginOptions{Addr: "http://plugin.invalid"}).(*httpPlugin)
	plugin.client.Transport = &staticRoundTripper{body: body}

	if _, _, err := plugin.Handle(context.Background(), OpNewProxyResult, NewProxyResultContent{}); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if !body.closed.Load() {
		t.Fatal("plugin response body was not closed")
	}
}

type redirectRoundTripper struct {
	body  *closeTrackingBody
	calls atomic.Int32
}

func (t *redirectRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	t.calls.Add(1)
	header := make(http.Header)
	header.Set("Location", "http://redirected.invalid")
	return &http.Response{
		StatusCode: http.StatusFound,
		Header:     header,
		Body:       t.body,
	}, nil
}

func TestHTTPPluginResponseWithClientErrorIsNotRetryable(t *testing.T) {
	t.Parallel()

	body := &closeTrackingBody{Reader: strings.NewReader("redirect")}
	transport := &redirectRoundTripper{body: body}
	plugin := NewHTTPPluginOptions(v1.HTTPPluginOptions{Addr: "http://plugin.invalid"}).(*httpPlugin)
	plugin.client.Transport = transport
	plugin.client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("redirect rejected")
	}

	_, _, err := plugin.Handle(context.Background(), OpNewProxyResult, NewProxyResultContent{})
	if err == nil {
		t.Fatal("Handle() error = nil, want redirect-policy error")
	}
	var transportErr retryableNotificationTransportError
	if errors.As(err, &transportErr) {
		t.Fatalf("Handle() error = %v, must be terminal after receiving redirect response", err)
	}
	if got := transport.calls.Load(); got != 1 {
		t.Fatalf("RoundTrip calls = %d, want 1", got)
	}
	if !body.closed.Load() {
		t.Fatal("redirect response body was not closed")
	}
}

func TestHTTPPluginDoesNotFollowCloseProxyRedirectResponses(t *testing.T) {
	t.Parallel()

	body := &closeTrackingBody{Reader: strings.NewReader("redirect")}
	transport := &redirectRoundTripper{body: body}
	plugin := NewHTTPPluginOptions(v1.HTTPPluginOptions{Addr: "http://plugin.invalid"}).(*httpPlugin)
	plugin.closeProxyClient.Transport = transport

	_, _, err := plugin.Handle(context.Background(), OpCloseProxy, CloseProxyContent{})
	if err == nil || !strings.Contains(err.Error(), "error code: 302") {
		t.Fatalf("Handle() error = %v, want terminal redirect status", err)
	}
	if got := transport.calls.Load(); got != 1 {
		t.Fatalf("RoundTrip calls = %d, want 1", got)
	}
	if !body.closed.Load() {
		t.Fatal("redirect response body was not closed")
	}
}

type closeProxyRequestObservation struct {
	attemptID string
	reqid     string
	body      string
	err       error
}

type closeProxyReadErrorBody struct{}

func (closeProxyReadErrorBody) Read([]byte) (int, error) {
	return 0, errors.New("response body read failed")
}

func (closeProxyReadErrorBody) Close() error { return nil }

type closeProxyRoundTripper struct {
	mu      sync.Mutex
	calls   int
	seen    chan closeProxyRequestObservation
	respond func(int, *http.Request) (*http.Response, error)
}

func (t *closeProxyRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	body, readErr := io.ReadAll(req.Body)
	var envelope struct {
		Content CloseProxyContent `json:"content"`
	}
	decodeErr := json.Unmarshal(body, &envelope)
	err := errors.Join(readErr, decodeErr)

	t.mu.Lock()
	t.calls++
	call := t.calls
	t.mu.Unlock()
	t.seen <- closeProxyRequestObservation{
		attemptID: envelope.Content.AttemptID,
		reqid:     req.Header.Get("X-Frp-Reqid"),
		body:      string(body),
		err:       err,
	}
	return t.respond(call, req)
}

func closeProxyHTTPResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func receiveCloseProxyRequest(t *testing.T, seen <-chan closeProxyRequestObservation) closeProxyRequestObservation {
	t.Helper()
	select {
	case observation := <-seen:
		if observation.err != nil {
			t.Fatalf("decode CloseProxy request: %v", observation.err)
		}
		return observation
	case <-time.After(time.Second):
		t.Fatal("CloseProxy request was not delivered")
		return closeProxyRequestObservation{}
	}
}

func assertNoCloseProxyRequest(t *testing.T, seen <-chan closeProxyRequestObservation) {
	t.Helper()
	select {
	case observation := <-seen:
		t.Fatalf("unexpected retried CloseProxy request for attempt %q", observation.attemptID)
	case <-time.After(50 * time.Millisecond):
	}
}

func newCloseProxyHTTPManager(
	t *testing.T,
	queueSize int,
	transport http.RoundTripper,
) *Manager {
	t.Helper()
	manager := newManagerWithCloseProxyDelivery(queueSize, func(int) time.Duration { return 0 })
	plugin := NewHTTPPluginOptions(v1.HTTPPluginOptions{
		Name: "close-proxy-test",
		Addr: "http://plugin.invalid",
		Ops:  []string{OpCloseProxy},
	}).(*httpPlugin)
	plugin.closeProxyClient.Transport = transport
	manager.Register(plugin)
	return manager
}

func TestManagerCloseProxyRetriesTransportFailuresUntilRecovery(t *testing.T) {
	t.Parallel()

	const attemptID = "0123456789abcdef0123456789abcdef"
	transport := &closeProxyRoundTripper{
		seen: make(chan closeProxyRequestObservation, 8),
		respond: func(call int, _ *http.Request) (*http.Response, error) {
			if call <= 3 {
				return nil, errors.New("temporary transport failure")
			}
			return closeProxyHTTPResponse(http.StatusOK, `{"reject":false,"unchange":true,"content":{}}`), nil
		},
	}
	manager := newCloseProxyHTTPManager(t, 2, transport)
	t.Cleanup(manager.Close)

	if err := manager.CloseProxy(&CloseProxyContent{
		User: UserInfo{
			User:  "tenant-exact",
			Metas: map[string]string{"tenant": "tenant-exact"},
			RunID: "run-exact",
		},
		AttemptID:  attemptID,
		CloseProxy: msg.CloseProxy{ProxyName: "proxy-exact"},
	}); err != nil {
		t.Fatalf("CloseProxy() error = %v", err)
	}
	var first closeProxyRequestObservation
	for range 4 {
		got := receiveCloseProxyRequest(t, transport.seen)
		if got.attemptID != attemptID {
			t.Fatalf("retried attempt_id = %q, want exact %q", got.attemptID, attemptID)
		}
		if first.body == "" {
			first = got
			if first.reqid == "" {
				t.Fatal("first CloseProxy request ID is empty")
			}
		} else if got.body != first.body || got.reqid != first.reqid {
			t.Fatalf("retried CloseProxy changed request identity: got body=%q reqid=%q, want body=%q reqid=%q",
				got.body, got.reqid, first.body, first.reqid)
		}
	}
	assertNoCloseProxyRequest(t, transport.seen)
}

func TestManagerCloseProxyDoesNotRetryAfterHTTPResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		response func() *http.Response
	}{
		{
			name: "semantic success",
			response: func() *http.Response {
				return closeProxyHTTPResponse(http.StatusOK, `{"reject":true,"reject_reason":"ignored","unchange":false,"content":{}}`)
			},
		},
		{
			name: "non-2xx",
			response: func() *http.Response {
				return closeProxyHTTPResponse(http.StatusServiceUnavailable, "unavailable")
			},
		},
		{
			name: "malformed body",
			response: func() *http.Response {
				return closeProxyHTTPResponse(http.StatusOK, "not-json")
			},
		},
		{
			name: "body read failure",
			response: func() *http.Response {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       closeProxyReadErrorBody{},
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			transport := &closeProxyRoundTripper{
				seen: make(chan closeProxyRequestObservation, 2),
				respond: func(_ int, _ *http.Request) (*http.Response, error) {
					return tt.response(), nil
				},
			}
			manager := newCloseProxyHTTPManager(t, 2, transport)
			t.Cleanup(manager.Close)

			if err := manager.CloseProxy(&CloseProxyContent{AttemptID: "0123456789abcdef0123456789abcdef"}); err != nil {
				t.Fatalf("CloseProxy() error = %v", err)
			}
			receiveCloseProxyRequest(t, transport.seen)
			assertNoCloseProxyRequest(t, transport.seen)
		})
	}
}

func TestManagerCloseProxyQueueAppliesBackpressure(t *testing.T) {
	t.Parallel()

	releaseFirst := make(chan struct{})
	transport := &closeProxyRoundTripper{
		seen: make(chan closeProxyRequestObservation, 4),
		respond: func(call int, req *http.Request) (*http.Response, error) {
			if call == 1 {
				select {
				case <-releaseFirst:
				case <-req.Context().Done():
					return nil, req.Context().Err()
				}
			}
			return closeProxyHTTPResponse(http.StatusOK, `{"reject":false,"unchange":true,"content":{}}`), nil
		},
	}
	manager := newCloseProxyHTTPManager(t, 1, transport)
	t.Cleanup(func() {
		select {
		case <-releaseFirst:
		default:
			close(releaseFirst)
		}
		manager.Close()
	})

	if err := manager.CloseProxy(&CloseProxyContent{AttemptID: "00000000000000000000000000000001"}); err != nil {
		t.Fatalf("first CloseProxy() error = %v", err)
	}
	receiveCloseProxyRequest(t, transport.seen)
	if err := manager.CloseProxy(&CloseProxyContent{AttemptID: "00000000000000000000000000000002"}); err != nil {
		t.Fatalf("second CloseProxy() error = %v", err)
	}

	thirdDone := make(chan error, 1)
	go func() {
		thirdDone <- manager.CloseProxy(&CloseProxyContent{AttemptID: "00000000000000000000000000000003"})
	}()
	select {
	case err := <-thirdDone:
		t.Fatalf("third CloseProxy() returned without queue capacity: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseFirst)

	select {
	case err := <-thirdDone:
		if err != nil {
			t.Fatalf("third CloseProxy() error after capacity became available = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("third CloseProxy() remained blocked after capacity became available")
	}
}

func TestManagerCloseCancelsDeliveryAndBlockedEnqueue(t *testing.T) {
	t.Parallel()

	transportCanceled := make(chan struct{})
	transport := &closeProxyRoundTripper{
		seen: make(chan closeProxyRequestObservation, 2),
		respond: func(_ int, req *http.Request) (*http.Response, error) {
			<-req.Context().Done()
			close(transportCanceled)
			return nil, req.Context().Err()
		},
	}
	manager := newCloseProxyHTTPManager(t, 1, transport)
	if err := manager.CloseProxy(&CloseProxyContent{AttemptID: "00000000000000000000000000000001"}); err != nil {
		t.Fatalf("first CloseProxy() error = %v", err)
	}
	receiveCloseProxyRequest(t, transport.seen)
	if err := manager.CloseProxy(&CloseProxyContent{AttemptID: "00000000000000000000000000000002"}); err != nil {
		t.Fatalf("second CloseProxy() error = %v", err)
	}

	blockedDone := make(chan error, 1)
	go func() {
		blockedDone <- manager.CloseProxy(&CloseProxyContent{AttemptID: "00000000000000000000000000000003"})
	}()
	select {
	case err := <-blockedDone:
		t.Fatalf("third CloseProxy() returned without queue capacity: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	closeDone := make(chan struct{})
	go func() {
		manager.Close()
		close(closeDone)
	}()
	select {
	case <-transportCanceled:
	case <-time.After(time.Second):
		t.Fatal("Manager.Close() did not cancel the in-flight HTTP request")
	}
	select {
	case err := <-blockedDone:
		if err == nil || !strings.Contains(err.Error(), "plugin manager is closed") {
			t.Fatalf("blocked CloseProxy() error = %v, want manager closed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Manager.Close() did not release blocked CloseProxy enqueue")
	}
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Manager.Close() did not wait for worker cancellation")
	}
}
