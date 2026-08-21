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
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/fatedier/frp/pkg/util/util"
	"github.com/fatedier/frp/pkg/util/xlog"
)

const (
	closeProxyNotificationQueueSize = 256
	closeProxyNotificationTimeout   = 30 * time.Second
	closeProxyRetryInitialDelay     = 100 * time.Millisecond
	closeProxyRetryMaxDelay         = 5 * time.Second
)

var (
	errPluginManagerClosed         = errors.New("plugin manager is closed")
	errPluginNotificationQueueFull = errors.New("plugin notification queue is full")
)

type retryableNotificationTransportError interface {
	error
	retryableNotificationTransport()
}

type closeProxyNotification struct {
	plugin  Plugin
	content CloseProxyContent
	ctx     context.Context
	xl      *xlog.Logger
}

type Manager struct {
	loginPlugins          []Plugin
	newProxyPlugins       []Plugin
	newProxyResultPlugins []Plugin
	closeProxyPlugins     []Plugin
	pingPlugins           []Plugin
	newWorkConnPlugins    []Plugin
	newUserConnPlugins    []Plugin

	closeProxyQueue      chan closeProxyNotification
	closeProxyTimeout    time.Duration
	closeProxyRetryDelay func(int) time.Duration
	notificationCtx      context.Context
	notificationCancel   context.CancelFunc
	notificationMu       sync.Mutex
	notificationWG       sync.WaitGroup
	closeProxyWorkerOn   bool
	closed               bool
}

func NewManager() *Manager {
	return newManagerWithCloseProxyDelivery(
		closeProxyNotificationQueueSize,
		defaultCloseProxyRetryDelay,
		closeProxyNotificationTimeout,
	)
}

func newManagerWithCloseProxyDelivery(
	queueSize int,
	retryDelay func(int) time.Duration,
	deliveryTimeout time.Duration,
) *Manager {
	if queueSize < 1 {
		queueSize = 1
	}
	if deliveryTimeout <= 0 {
		deliveryTimeout = closeProxyNotificationTimeout
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		loginPlugins:          make([]Plugin, 0),
		newProxyPlugins:       make([]Plugin, 0),
		newProxyResultPlugins: make([]Plugin, 0),
		closeProxyPlugins:     make([]Plugin, 0),
		pingPlugins:           make([]Plugin, 0),
		newWorkConnPlugins:    make([]Plugin, 0),
		newUserConnPlugins:    make([]Plugin, 0),
		closeProxyQueue:       make(chan closeProxyNotification, queueSize),
		closeProxyTimeout:     deliveryTimeout,
		closeProxyRetryDelay:  retryDelay,
		notificationCtx:       ctx,
		notificationCancel:    cancel,
	}
}

func defaultCloseProxyRetryDelay(failureCount int) time.Duration {
	delay := closeProxyRetryInitialDelay
	for i := 1; i < failureCount && delay < closeProxyRetryMaxDelay; i++ {
		delay = min(delay*2, closeProxyRetryMaxDelay)
	}
	return delay
}

func (m *Manager) Register(p Plugin) {
	if p.IsSupport(OpLogin) {
		m.loginPlugins = append(m.loginPlugins, p)
	}
	if p.IsSupport(OpNewProxy) {
		m.newProxyPlugins = append(m.newProxyPlugins, p)
	}
	if p.IsSupport(OpNewProxyResult) {
		m.newProxyResultPlugins = append(m.newProxyResultPlugins, p)
	}
	if p.IsSupport(OpCloseProxy) {
		m.closeProxyPlugins = append(m.closeProxyPlugins, p)
	}
	if p.IsSupport(OpPing) {
		m.pingPlugins = append(m.pingPlugins, p)
	}
	if p.IsSupport(OpNewWorkConn) {
		m.newWorkConnPlugins = append(m.newWorkConnPlugins, p)
	}
	if p.IsSupport(OpNewUserConn) {
		m.newUserConnPlugins = append(m.newUserConnPlugins, p)
	}
}

func (m *Manager) Login(content *LoginContent) (*LoginContent, error) {
	if len(m.loginPlugins) == 0 {
		return content, nil
	}

	var (
		res = &Response{
			Reject:   false,
			Unchange: true,
		}
		retContent any
		err        error
	)
	reqid, _ := util.RandID()
	xl := xlog.New().AppendPrefix("reqid: " + reqid)
	ctx := xlog.NewContext(context.Background(), xl)
	ctx = NewReqidContext(ctx, reqid)

	for _, p := range m.loginPlugins {
		res, retContent, err = p.Handle(ctx, OpLogin, *content)
		if err != nil {
			xl.Warnf("send Login request to plugin [%s] error: %v", p.Name(), err)
			return nil, errors.New("send Login request to plugin error")
		}
		if res.Reject {
			return nil, fmt.Errorf("%s", res.RejectReason)
		}
		if !res.Unchange {
			content = retContent.(*LoginContent)
		}
	}
	return content, nil
}

func (m *Manager) NewProxy(content *NewProxyContent) (*NewProxyContent, error) {
	if len(m.newProxyPlugins) == 0 {
		return content, nil
	}
	attemptID := content.AttemptID

	var (
		res = &Response{
			Reject:   false,
			Unchange: true,
		}
		retContent any
		err        error
	)
	reqid, _ := util.RandID()
	xl := xlog.New().AppendPrefix("reqid: " + reqid)
	ctx := xlog.NewContext(context.Background(), xl)
	ctx = NewReqidContext(ctx, reqid)

	for _, p := range m.newProxyPlugins {
		res, retContent, err = p.Handle(ctx, OpNewProxy, *content)
		if err != nil {
			xl.Warnf("send NewProxy request to plugin [%s] error: %v", p.Name(), err)
			return nil, errors.New("send NewProxy request to plugin error")
		}
		if res.Reject {
			return nil, fmt.Errorf("%s", res.RejectReason)
		}
		if !res.Unchange {
			content = retContent.(*NewProxyContent)
			// AttemptID is FRPS-owned correlation state, not mutable proxy
			// configuration. Preserve it across every plugin in the chain so a
			// response cannot alias a later NewProxyResult to another attempt.
			content.AttemptID = attemptID
		}
	}
	return content, nil
}

func (m *Manager) CloseProxy(content *CloseProxyContent) error {
	if len(m.closeProxyPlugins) == 0 {
		return nil
	}

	reqid, _ := util.RandID()
	for _, p := range m.closeProxyPlugins {
		xl := xlog.New().AppendPrefix("reqid: " + reqid)
		ctx := xlog.NewContext(m.notificationCtx, xl)
		ctx = NewReqidContext(ctx, reqid)
		task := closeProxyNotification{
			plugin: p,
			content: CloseProxyContent{
				User:       content.User.Clone(),
				AttemptID:  content.AttemptID,
				CloseProxy: content.CloseProxy,
			},
			ctx: ctx,
			xl:  xl,
		}
		if err := m.enqueueCloseProxy(task); err != nil {
			return err
		}
	}
	return nil
}

// Close cancels in-flight plugin notifications and stops their workers.
// Pending notifications are intentionally not drained during process shutdown;
// consumers must reconcile their own snapshot when they stop.
func (m *Manager) Close() {
	m.notificationMu.Lock()
	if !m.closed {
		m.closed = true
		m.notificationCancel()
	}
	m.notificationMu.Unlock()
	m.notificationWG.Wait()
}

func (m *Manager) enqueueCloseProxy(task closeProxyNotification) error {
	m.notificationMu.Lock()
	defer m.notificationMu.Unlock()
	if m.closed {
		return errPluginManagerClosed
	}
	if !m.closeProxyWorkerOn {
		m.closeProxyWorkerOn = true
		m.notificationWG.Add(1)
		go m.runCloseProxyWorker()
	}
	select {
	case m.closeProxyQueue <- task:
		return nil
	default:
		return errPluginNotificationQueueFull
	}
}

func (m *Manager) runCloseProxyWorker() {
	defer m.notificationWG.Done()
	for {
		// Give shutdown priority over a buffered task. A single select could
		// randomly drain queued notifications after cancellation, contrary to
		// the bounded-shutdown contract.
		select {
		case <-m.notificationCtx.Done():
			return
		default:
		}

		select {
		case <-m.notificationCtx.Done():
			return
		case task := <-m.closeProxyQueue:
			if m.notificationCtx.Err() != nil {
				return
			}
			m.deliverCloseProxy(task)
		}
	}
}

func (m *Manager) deliverCloseProxy(task closeProxyNotification) {
	ctx, cancel := context.WithTimeout(task.ctx, m.closeProxyTimeout)
	defer cancel()
	task.ctx = ctx
	attempts := 0
	defer func() {
		if errors.Is(task.ctx.Err(), context.DeadlineExceeded) {
			task.xl.Warnf(
				"send %s request to plugin [%s] abandoned after %d attempts: delivery budget %s exhausted",
				OpCloseProxy,
				task.plugin.Name(),
				attempts,
				m.closeProxyTimeout,
			)
		}
	}()
	for failureCount := 0; ; failureCount++ {
		if task.ctx.Err() != nil {
			return
		}

		attempts = failureCount + 1
		_, _, err := task.plugin.Handle(task.ctx, OpCloseProxy, task.content)
		if err == nil {
			return
		}
		if task.ctx.Err() != nil {
			return
		}

		var transportErr retryableNotificationTransportError
		if !errors.As(err, &transportErr) {
			task.xl.Warnf("send %s request to plugin [%s] error: %v", OpCloseProxy, task.plugin.Name(), err)
			return
		}

		delay := m.closeProxyRetryDelay(failureCount + 1)
		task.xl.Warnf(
			"send %s request to plugin [%s] transport error (attempt %d), retrying in %s: %v",
			OpCloseProxy,
			task.plugin.Name(),
			failureCount+1,
			delay,
			err,
		)
		timer := time.NewTimer(delay)
		select {
		case <-task.ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-timer.C:
		}
	}
}

// NewProxyResult synchronously asks every interested plugin to confirm the
// final FRPS admission outcome. An admitted proxy is not reported to the client
// until every plugin acknowledges the result. A reject or delivery failure is
// therefore authoritative for the admission transaction even though plugins
// cannot mutate the result content.
func (m *Manager) NewProxyResult(content *NewProxyResultContent) error {
	if len(m.newProxyResultPlugins) == 0 {
		return nil
	}

	reqid, _ := util.RandID()
	xl := xlog.New().AppendPrefix("reqid: " + reqid)
	ctx := xlog.NewContext(m.notificationCtx, xl)
	ctx = NewReqidContext(ctx, reqid)
	return m.deliverNewProxyResult(ctx, xl, content)
}

func (m *Manager) deliverNewProxyResult(
	ctx context.Context,
	xl *xlog.Logger,
	content *NewProxyResultContent,
) error {
	errs := make([]string, 0)
	deliveryFailed := false
	for _, p := range m.newProxyResultPlugins {
		res, _, err := p.Handle(ctx, OpNewProxyResult, *content)
		if err != nil {
			xl.Warnf("send %s request to plugin [%s] error: %v", OpNewProxyResult, p.Name(), err)
			deliveryFailed = true
			continue
		}
		if res == nil {
			err = errors.New("empty plugin response")
			xl.Warnf("send %s request to plugin [%s] error: %v", OpNewProxyResult, p.Name(), err)
			deliveryFailed = true
			continue
		}
		if res.Reject {
			reason := strings.TrimSpace(res.RejectReason)
			if reason == "" {
				reason = "rejected without reason"
			}
			xl.Warnf("plugin [%s] rejected %s: %s", p.Name(), OpNewProxyResult, reason)
			errs = append(errs, fmt.Sprintf("[%s]: %s", p.Name(), reason))
		}
	}

	if deliveryFailed {
		errs = append([]string{"result plugin delivery failed"}, errs...)
	}
	if len(errs) > 0 {
		return fmt.Errorf("send %s request to plugin errors: %s", OpNewProxyResult, strings.Join(errs, "; "))
	}
	return nil
}

func (m *Manager) Ping(content *PingContent) (*PingContent, error) {
	if len(m.pingPlugins) == 0 {
		return content, nil
	}

	var (
		res = &Response{
			Reject:   false,
			Unchange: true,
		}
		retContent any
		err        error
	)
	reqid, _ := util.RandID()
	xl := xlog.New().AppendPrefix("reqid: " + reqid)
	ctx := xlog.NewContext(context.Background(), xl)
	ctx = NewReqidContext(ctx, reqid)

	for _, p := range m.pingPlugins {
		res, retContent, err = p.Handle(ctx, OpPing, *content)
		if err != nil {
			xl.Warnf("send Ping request to plugin [%s] error: %v", p.Name(), err)
			return nil, errors.New("send Ping request to plugin error")
		}
		if res.Reject {
			return nil, fmt.Errorf("%s", res.RejectReason)
		}
		if !res.Unchange {
			content = retContent.(*PingContent)
		}
	}
	return content, nil
}

func (m *Manager) NewWorkConn(content *NewWorkConnContent) (*NewWorkConnContent, error) {
	if len(m.newWorkConnPlugins) == 0 {
		return content, nil
	}

	var (
		res = &Response{
			Reject:   false,
			Unchange: true,
		}
		retContent any
		err        error
	)
	reqid, _ := util.RandID()
	xl := xlog.New().AppendPrefix("reqid: " + reqid)
	ctx := xlog.NewContext(context.Background(), xl)
	ctx = NewReqidContext(ctx, reqid)

	for _, p := range m.newWorkConnPlugins {
		res, retContent, err = p.Handle(ctx, OpNewWorkConn, *content)
		if err != nil {
			xl.Warnf("send NewWorkConn request to plugin [%s] error: %v", p.Name(), err)
			return nil, errors.New("send NewWorkConn request to plugin error")
		}
		if res.Reject {
			return nil, fmt.Errorf("%s", res.RejectReason)
		}
		if !res.Unchange {
			content = retContent.(*NewWorkConnContent)
		}
	}
	return content, nil
}

func (m *Manager) NewUserConn(content *NewUserConnContent) (*NewUserConnContent, error) {
	if len(m.newUserConnPlugins) == 0 {
		return content, nil
	}

	var (
		res = &Response{
			Reject:   false,
			Unchange: true,
		}
		retContent any
		err        error
	)
	reqid, _ := util.RandID()
	xl := xlog.New().AppendPrefix("reqid: " + reqid)
	ctx := xlog.NewContext(context.Background(), xl)
	ctx = NewReqidContext(ctx, reqid)

	for _, p := range m.newUserConnPlugins {
		res, retContent, err = p.Handle(ctx, OpNewUserConn, *content)
		if err != nil {
			xl.Infof("send NewUserConn request to plugin [%s] error: %v", p.Name(), err)
			return nil, errors.New("send NewUserConn request to plugin error")
		}
		if res.Reject {
			return nil, fmt.Errorf("%s", res.RejectReason)
		}
		if !res.Unchange {
			content = retContent.(*NewUserConnContent)
		}
	}
	return content, nil
}
