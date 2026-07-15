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
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/fatedier/frp/pkg/config/v1"
)

func TestNewHTTPPluginOptionsSetsRequestTimeout(t *testing.T) {
	t.Parallel()

	for _, addr := range []string{"http://127.0.0.1:8080", "https://127.0.0.1:8443"} {
		plugin := NewHTTPPluginOptions(v1.HTTPPluginOptions{Addr: addr}).(*httpPlugin)
		if plugin.client.Timeout != HTTPPluginRequestTimeout {
			t.Fatalf("client timeout for %q = %s, want %s", addr, plugin.client.Timeout, HTTPPluginRequestTimeout)
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
