package plugin

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"

	plugin "github.com/fatedier/frp/pkg/plugin/server"
	"github.com/fatedier/frp/pkg/transport"
	"github.com/fatedier/frp/test/e2e/framework"
	"github.com/fatedier/frp/test/e2e/framework/consts"
	pluginpkg "github.com/fatedier/frp/test/e2e/pkg/plugin"
)

var _ = ginkgo.Describe("[Feature: Server-Plugins]", func() {
	f := framework.NewDefaultFramework()

	ginkgo.Describe("Login", func() {
		newFunc := func() *plugin.Request {
			var r plugin.Request
			r.Content = &plugin.LoginContent{}
			return &r
		}

		ginkgo.It("Auth for custom meta token", func() {
			localPort := f.AllocPort()

			clientAddressGot := false
			handler := func(req *plugin.Request) *plugin.Response {
				var ret plugin.Response
				content := req.Content.(*plugin.LoginContent)
				if content.ClientAddress != "" {
					clientAddressGot = true
				}
				if content.Metas["token"] == "123" {
					ret.Unchange = true
				} else {
					ret.Reject = true
					ret.RejectReason = "invalid token"
				}
				return &ret
			}
			pluginServer := pluginpkg.NewHTTPPluginServer(localPort, newFunc, handler, nil)

			f.RunServer("", pluginServer)

			serverConf := consts.DefaultServerConfig + fmt.Sprintf(`
			[[httpPlugins]]
			name = "user-manager"
			addr = "127.0.0.1:%d"
			path = "/handler"
			ops = ["Login"]
			`, localPort)
			clientConf := consts.DefaultClientConfig

			remotePort := f.AllocPort()
			clientConf += fmt.Sprintf(`
			metadatas.token = "123"

			[[proxies]]
			name = "tcp"
			type = "tcp"
			localPort = {{ .%s }}
			remotePort = %d
			`, framework.TCPEchoServerPort, remotePort)

			remotePort2 := f.AllocPort()
			invalidTokenClientConf := consts.DefaultClientConfig + fmt.Sprintf(`
			[[proxies]]
			name = "tcp2"
			type = "tcp"
			localPort = {{ .%s }}
			remotePort = %d
			`, framework.TCPEchoServerPort, remotePort2)

			f.RunProcesses(serverConf, []string{clientConf, invalidTokenClientConf})

			framework.NewRequestExpect(f).Port(remotePort).Ensure()
			framework.NewRequestExpect(f).Port(remotePort2).ExpectError(true).Ensure()

			framework.ExpectTrue(clientAddressGot)
		})
	})

	ginkgo.Describe("NewProxy", func() {
		newFunc := func() *plugin.Request {
			var r plugin.Request
			r.Content = &plugin.NewProxyContent{}
			return &r
		}

		ginkgo.It("Validate Info", func() {
			localPort := f.AllocPort()
			handler := func(req *plugin.Request) *plugin.Response {
				var ret plugin.Response
				content := req.Content.(*plugin.NewProxyContent)
				if content.ProxyName == "tcp" {
					ret.Unchange = true
				} else {
					ret.Reject = true
				}
				return &ret
			}
			pluginServer := pluginpkg.NewHTTPPluginServer(localPort, newFunc, handler, nil)

			f.RunServer("", pluginServer)

			serverConf := consts.DefaultServerConfig + fmt.Sprintf(`
			[[httpPlugins]]
			name = "test"
			addr = "127.0.0.1:%d"
			path = "/handler"
			ops = ["NewProxy"]
			`, localPort)
			clientConf := consts.DefaultClientConfig

			remotePort := f.AllocPort()
			clientConf += fmt.Sprintf(`
			[[proxies]]
			name = "tcp"
			type = "tcp"
			localPort = {{ .%s }}
			remotePort = %d
			`, framework.TCPEchoServerPort, remotePort)

			f.RunProcesses(serverConf, []string{clientConf})

			framework.NewRequestExpect(f).Port(remotePort).Ensure()
		})

		ginkgo.It("Modify RemotePort", func() {
			localPort := f.AllocPort()
			remotePort := f.AllocPort()
			handler := func(req *plugin.Request) *plugin.Response {
				var ret plugin.Response
				content := req.Content.(*plugin.NewProxyContent)
				content.RemotePort = remotePort
				ret.Content = content
				return &ret
			}
			pluginServer := pluginpkg.NewHTTPPluginServer(localPort, newFunc, handler, nil)

			f.RunServer("", pluginServer)

			serverConf := consts.DefaultServerConfig + fmt.Sprintf(`
			[[httpPlugins]]
			name = "test"
			addr = "127.0.0.1:%d"
			path = "/handler"
			ops = ["NewProxy"]
			`, localPort)
			clientConf := consts.DefaultClientConfig

			clientConf += fmt.Sprintf(`
			[[proxies]]
			name = "tcp"
			type = "tcp"
			localPort = {{ .%s }}
			remotePort = 0
			`, framework.TCPEchoServerPort)

			f.RunProcesses(serverConf, []string{clientConf})

			framework.NewRequestExpect(f).Port(remotePort).Ensure()
		})
	})

	ginkgo.Describe("NewProxyResult", func() {
		newFunc := func() *plugin.Request {
			var r plugin.Request
			r.Content = new(json.RawMessage)
			return &r
		}

		ginkgo.It("reports accepted and late-failed attempts before client success", func() {
			localPort := f.AllocPort()
			results := make(chan plugin.NewProxyResultContent, 8)
			handler := func(req *plugin.Request) *plugin.Response {
				if req.Op == plugin.OpCloseProxy {
					return &plugin.Response{Unchange: true}
				}
				framework.ExpectEqual(plugin.OpNewProxyResult, req.Op)
				var content plugin.NewProxyResultContent
				framework.ExpectNoError(json.Unmarshal(*req.Content.(*json.RawMessage), &content))
				results <- content
				return &plugin.Response{Unchange: true}
			}
			pluginServer := pluginpkg.NewHTTPPluginServer(localPort, newFunc, handler, nil)
			f.RunServer("", pluginServer)

			serverConf := consts.DefaultServerConfig + fmt.Sprintf(`
			[[httpPlugins]]
			name = "result-observer"
			addr = "127.0.0.1:%d"
			path = "/handler"
			ops = ["NewProxyResult", "CloseProxy"]
			`, localPort)
			remotePort := f.AllocPort()
			clientConf := consts.DefaultClientConfig + fmt.Sprintf(`
			[[proxies]]
			name = "result-first"
			type = "tcp"
			localPort = {{ .%s }}
			remotePort = %d

			[[proxies]]
			name = "result-second"
			type = "tcp"
			localPort = {{ .%s }}
			remotePort = %d
			`, framework.TCPEchoServerPort, remotePort, framework.TCPEchoServerPort, remotePort)

			f.RunProcesses(serverConf, []string{clientConf})
			// Exactly one of the two attempts owns the shared remote port. Its
			// confirmed result-plugin response keeps the proxy reachable; the
			// other attempt fails earlier inside RegisterProxy.
			framework.NewRequestExpect(f).Port(remotePort).Ensure()

			got := make([]plugin.NewProxyResultContent, 0, 2)
			for len(got) < 2 {
				select {
				case result := <-results:
					got = append(got, result)
				case <-time.After(10 * time.Second):
					ginkgo.Fail(fmt.Sprintf("timed out waiting for NewProxyResult callbacks; got %+v", got))
				}
			}

			names := map[string]bool{}
			attemptIDs := map[string]bool{}
			admitted := 0
			failed := 0
			var runID string
			for _, result := range got {
				names[result.ProxyName] = true
				framework.ExpectEqual(32, len(result.AttemptID))
				framework.ExpectEqual(strings.ToLower(result.AttemptID), result.AttemptID)
				_, err := hex.DecodeString(result.AttemptID)
				framework.ExpectNoError(err)
				attemptIDs[result.AttemptID] = true
				framework.ExpectNotEqual("", result.User.RunID)
				if runID == "" {
					runID = result.User.RunID
				}
				framework.ExpectEqual(runID, result.User.RunID)
				if result.Admitted {
					admitted++
				} else {
					failed++
				}
			}
			framework.ExpectTrue(names["result-first"])
			framework.ExpectTrue(names["result-second"])
			framework.ExpectEqual(2, len(attemptIDs))
			framework.ExpectEqual(1, admitted)
			framework.ExpectEqual(1, failed)
		})

		ginkgo.It("rolls back a tentative proxy when result confirmation rejects it", func() {
			localPort := f.AllocPort()
			results := make(chan plugin.NewProxyResultContent, 16)
			closes := make(chan plugin.CloseProxyContent, 16)
			handler := func(req *plugin.Request) *plugin.Response {
				raw := *req.Content.(*json.RawMessage)
				switch req.Op {
				case plugin.OpNewProxyResult:
					var content plugin.NewProxyResultContent
					framework.ExpectNoError(json.Unmarshal(raw, &content))
					results <- content
					return &plugin.Response{Reject: true, RejectReason: "external route rejected"}
				case plugin.OpCloseProxy:
					var content plugin.CloseProxyContent
					framework.ExpectNoError(json.Unmarshal(raw, &content))
					closes <- content
					return &plugin.Response{Unchange: true}
				default:
					ginkgo.Fail(fmt.Sprintf("unexpected plugin op %q", req.Op))
					return nil
				}
			}
			pluginServer := pluginpkg.NewHTTPPluginServer(localPort, newFunc, handler, nil)
			f.RunServer("", pluginServer)

			serverConf := consts.DefaultServerConfig + fmt.Sprintf(`
			[[httpPlugins]]
			name = "result-rejector"
			addr = "127.0.0.1:%d"
			path = "/handler"
			ops = ["NewProxyResult", "CloseProxy"]
			`, localPort)
			remotePort := f.AllocPort()
			clientConf := consts.DefaultClientConfig + fmt.Sprintf(`
			[[proxies]]
			name = "result-rejected"
			type = "tcp"
			localPort = {{ .%s }}
			remotePort = %d
			`, framework.TCPEchoServerPort, remotePort)

			_, clients := f.RunProcesses(serverConf, []string{clientConf})
			framework.ExpectNoError(clients[0].WaitForOutput("external route rejected", 1, 10*time.Second))

			var result plugin.NewProxyResultContent
			select {
			case result = <-results:
			case <-time.After(10 * time.Second):
				ginkgo.Fail("timed out waiting for rejected NewProxyResult callback")
			}
			framework.ExpectTrue(result.Admitted)
			framework.ExpectEqual("result-rejected", result.ProxyName)
			framework.ExpectNotEqual("", result.User.RunID)
			framework.ExpectEqual(32, len(result.AttemptID))

			var closeContent plugin.CloseProxyContent
			select {
			case closeContent = <-closes:
			case <-time.After(10 * time.Second):
				ginkgo.Fail("timed out waiting for compensating CloseProxy callback")
			}
			framework.ExpectEqual(result.AttemptID, closeContent.AttemptID)
			framework.ExpectEqual(result.ProxyName, closeContent.ProxyName)
			framework.ExpectEqual(result.User.RunID, closeContent.User.RunID)
			framework.NewRequestExpect(f).Port(remotePort).ExpectError(true).Ensure()
		})
	})

	ginkgo.Describe("CloseProxy", func() {
		newFunc := func() *plugin.Request {
			var r plugin.Request
			r.Content = &plugin.CloseProxyContent{}
			return &r
		}

		ginkgo.It("Validate Info", func() {
			localPort := f.AllocPort()
			var recordProxyName string
			handler := func(req *plugin.Request) *plugin.Response {
				var ret plugin.Response
				content := req.Content.(*plugin.CloseProxyContent)
				recordProxyName = content.ProxyName
				return &ret
			}
			pluginServer := pluginpkg.NewHTTPPluginServer(localPort, newFunc, handler, nil)

			f.RunServer("", pluginServer)

			serverConf := consts.DefaultServerConfig + fmt.Sprintf(`
			[[httpPlugins]]
			name = "test"
			addr = "127.0.0.1:%d"
			path = "/handler"
			ops = ["CloseProxy"]
			`, localPort)
			clientConf := consts.DefaultClientConfig

			remotePort := f.AllocPort()
			clientConf += fmt.Sprintf(`
			[[proxies]]
			name = "tcp"
			type = "tcp"
			localPort = {{ .%s }}
			remotePort = %d
			`, framework.TCPEchoServerPort, remotePort)

			_, clients := f.RunProcesses(serverConf, []string{clientConf})

			framework.NewRequestExpect(f).Port(remotePort).Ensure()

			for _, c := range clients {
				_ = c.Stop()
			}

			time.Sleep(1 * time.Second)

			framework.ExpectEqual(recordProxyName, "tcp")
		})
	})

	ginkgo.Describe("Ping", func() {
		newFunc := func() *plugin.Request {
			var r plugin.Request
			r.Content = &plugin.PingContent{}
			return &r
		}

		ginkgo.It("Validate Info", func() {
			localPort := f.AllocPort()

			var record string
			handler := func(req *plugin.Request) *plugin.Response {
				var ret plugin.Response
				content := req.Content.(*plugin.PingContent)
				record = content.PrivilegeKey
				ret.Unchange = true
				return &ret
			}
			pluginServer := pluginpkg.NewHTTPPluginServer(localPort, newFunc, handler, nil)

			f.RunServer("", pluginServer)

			serverConf := consts.DefaultServerConfig + fmt.Sprintf(`
			[[httpPlugins]]
			name = "test"
			addr = "127.0.0.1:%d"
			path = "/handler"
			ops = ["Ping"]
			`, localPort)

			remotePort := f.AllocPort()
			clientConf := consts.DefaultClientConfig
			clientConf += fmt.Sprintf(`
			transport.heartbeatInterval = 1
			auth.additionalScopes = ["HeartBeats"]

			[[proxies]]
			name = "tcp"
			type = "tcp"
			localPort = {{ .%s }}
			remotePort = %d
			`, framework.TCPEchoServerPort, remotePort)

			f.RunProcesses(serverConf, []string{clientConf})

			framework.NewRequestExpect(f).Port(remotePort).Ensure()

			time.Sleep(3 * time.Second)
			framework.ExpectNotEqual("", record)
		})
	})

	ginkgo.Describe("NewWorkConn", func() {
		newFunc := func() *plugin.Request {
			var r plugin.Request
			r.Content = &plugin.NewWorkConnContent{}
			return &r
		}

		ginkgo.It("Validate Info", func() {
			localPort := f.AllocPort()

			var record string
			handler := func(req *plugin.Request) *plugin.Response {
				var ret plugin.Response
				content := req.Content.(*plugin.NewWorkConnContent)
				record = content.RunID
				ret.Unchange = true
				return &ret
			}
			pluginServer := pluginpkg.NewHTTPPluginServer(localPort, newFunc, handler, nil)

			f.RunServer("", pluginServer)

			serverConf := consts.DefaultServerConfig + fmt.Sprintf(`
			[[httpPlugins]]
			name = "test"
			addr = "127.0.0.1:%d"
			path = "/handler"
			ops = ["NewWorkConn"]
			`, localPort)

			remotePort := f.AllocPort()
			clientConf := consts.DefaultClientConfig
			clientConf += fmt.Sprintf(`
			[[proxies]]
			name = "tcp"
			type = "tcp"
			localPort = {{ .%s }}
			remotePort = %d
			`, framework.TCPEchoServerPort, remotePort)

			f.RunProcesses(serverConf, []string{clientConf})

			framework.NewRequestExpect(f).Port(remotePort).Ensure()

			framework.ExpectNotEqual("", record)
		})
	})

	ginkgo.Describe("NewUserConn", func() {
		newFunc := func() *plugin.Request {
			var r plugin.Request
			r.Content = &plugin.NewUserConnContent{}
			return &r
		}
		ginkgo.It("Validate Info", func() {
			localPort := f.AllocPort()

			var record string
			handler := func(req *plugin.Request) *plugin.Response {
				var ret plugin.Response
				content := req.Content.(*plugin.NewUserConnContent)
				record = content.RemoteAddr
				ret.Unchange = true
				return &ret
			}
			pluginServer := pluginpkg.NewHTTPPluginServer(localPort, newFunc, handler, nil)

			f.RunServer("", pluginServer)

			serverConf := consts.DefaultServerConfig + fmt.Sprintf(`
			[[httpPlugins]]
			name = "test"
			addr = "127.0.0.1:%d"
			path = "/handler"
			ops = ["NewUserConn"]
			`, localPort)

			remotePort := f.AllocPort()
			clientConf := consts.DefaultClientConfig
			clientConf += fmt.Sprintf(`
			[[proxies]]
			name = "tcp"
			type = "tcp"
			localPort = {{ .%s }}
			remotePort = %d
			`, framework.TCPEchoServerPort, remotePort)

			f.RunProcesses(serverConf, []string{clientConf})

			framework.NewRequestExpect(f).Port(remotePort).Ensure()

			framework.ExpectNotEqual("", record)
		})
	})

	ginkgo.Describe("HTTPS Protocol", func() {
		newFunc := func() *plugin.Request {
			var r plugin.Request
			r.Content = &plugin.NewUserConnContent{}
			return &r
		}
		ginkgo.It("Validate Login Info, disable tls verify", func() {
			localPort := f.AllocPort()

			var record string
			handler := func(req *plugin.Request) *plugin.Response {
				var ret plugin.Response
				content := req.Content.(*plugin.NewUserConnContent)
				record = content.RemoteAddr
				ret.Unchange = true
				return &ret
			}
			tlsConfig, err := transport.NewServerTLSConfig("", "", "")
			framework.ExpectNoError(err)
			pluginServer := pluginpkg.NewHTTPPluginServer(localPort, newFunc, handler, tlsConfig)

			f.RunServer("", pluginServer)

			serverConf := consts.DefaultServerConfig + fmt.Sprintf(`
			[[httpPlugins]]
			name = "test"
			addr = "https://127.0.0.1:%d"
			path = "/handler"
			ops = ["NewUserConn"]
			`, localPort)

			remotePort := f.AllocPort()
			clientConf := consts.DefaultClientConfig
			clientConf += fmt.Sprintf(`
			[[proxies]]
			name = "tcp"
			type = "tcp"
			localPort = {{ .%s }}
			remotePort = %d
			`, framework.TCPEchoServerPort, remotePort)

			f.RunProcesses(serverConf, []string{clientConf})

			framework.NewRequestExpect(f).Port(remotePort).Ensure()

			framework.ExpectNotEqual("", record)
		})
	})
})
