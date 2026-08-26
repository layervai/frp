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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"time"

	v1 "github.com/fatedier/frp/pkg/config/v1"
)

// HTTPPluginRequestTimeout bounds one complete server-plugin HTTP exchange,
// including connection setup, request upload, response headers, and response
// body reads. Keep this below qURL Reverse Tunnel Server's 30-second HTTP write
// timeout so FRPS has a deterministic upper bound instead of leaving a
// NewProxy admission callback blocked indefinitely.
const HTTPPluginRequestTimeout = 25 * time.Second

type httpPlugin struct {
	options v1.HTTPPluginOptions

	url              string
	client           *http.Client
	closeProxyClient *http.Client
}

// pluginNotificationTransportError marks a callback for which http.Client
// returned no response. CloseProxy does not follow redirects, so it may safely
// retry this narrow failure class: the plugin has not acknowledged the
// notification, and the immutable attempt_id makes the qURL lifecycle consumer
// idempotent. Errors accompanied by a response are deliberately unmarked
// because the plugin may have already applied the notification; semantic
// responses are never retried.
type pluginNotificationTransportError struct {
	err error
}

func (e *pluginNotificationTransportError) Error() string {
	return e.err.Error()
}

func (e *pluginNotificationTransportError) Unwrap() error {
	return e.err
}

func (e *pluginNotificationTransportError) retryableNotificationTransport() {}

func NewHTTPPluginOptions(options v1.HTTPPluginOptions) Plugin {
	url := fmt.Sprintf("%s%s", options.Addr, options.Path)

	var client *http.Client
	if strings.HasPrefix(url, "https://") {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: !options.TLSVerify},
		}
		client = &http.Client{Transport: tr, Timeout: HTTPPluginRequestTimeout}
	} else {
		client = &http.Client{Timeout: HTTPPluginRequestTimeout}
	}

	if !strings.HasPrefix(url, "https://") && !strings.HasPrefix(url, "http://") {
		url = "http://" + url
	}
	// Copy only during construction, before either client is used. The two
	// clients share the same transport and timeout, while CloseProxy alone keeps
	// the first redirect response terminal. http.Client must not be copied after
	// use, so retain this dedicated instance for every callback and retry.
	closeProxyClient := *client
	closeProxyClient.CheckRedirect = keepPluginRedirectResponse
	return &httpPlugin{
		options:          options,
		url:              url,
		client:           client,
		closeProxyClient: &closeProxyClient,
	}
}

// keepPluginRedirectResponse makes the first received CloseProxy response
// terminal. Following a redirect could otherwise receive a response and then
// surface a nil-response transport error from the redirected request, causing
// CloseProxy to retry an already-acknowledged lifecycle notification.
func keepPluginRedirectResponse(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

func (p *httpPlugin) Name() string {
	return p.options.Name
}

func (p *httpPlugin) IsSupport(op string) bool {
	return slices.Contains(p.options.Ops, op)
}

func (p *httpPlugin) Handle(ctx context.Context, op string, content any) (*Response, any, error) {
	r := &Request{
		Version: APIVersion,
		Op:      op,
		Content: content,
	}
	var res Response
	res.Content = reflect.New(reflect.TypeOf(content)).Interface()
	if err := p.do(ctx, r, &res); err != nil {
		return nil, nil, err
	}
	return &res, res.Content, nil
}

func (p *httpPlugin) do(ctx context.Context, r *Request, res *Response) error {
	buf, err := json.Marshal(r)
	if err != nil {
		return err
	}
	v := url.Values{}
	v.Set("version", r.Version)
	v.Set("op", r.Op)
	req, err := http.NewRequest("POST", p.url+"?"+v.Encode(), bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	req.Header.Set("X-Frp-Reqid", GetReqidFromContext(ctx))
	req.Header.Set("Content-Type", "application/json")
	client := p.client
	if r.Op == OpCloseProxy {
		client = p.closeProxyClient
	}
	resp, err := client.Do(req)
	if err != nil {
		if resp == nil {
			return &pluginNotificationTransportError{err: err}
		}
		// net/http may return both a response and an error for redirect-policy
		// failures. A response means the remote side answered, so this is not a
		// retryable pre-delivery transport failure. Close defensively before
		// returning the original semantic/client error.
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("do http request error code: %d", resp.StatusCode)
	}
	buf, err = io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(buf, res)
}
