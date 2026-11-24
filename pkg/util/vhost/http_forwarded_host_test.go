package vhost

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// qURL's trusted ingress supplies a different browser host.
// A fix must not trust arbitrary forwarded headers on public FRP listeners.
func TestQURLTrustedBrowserHostSurvivesFRPVhost(t *testing.T) {
	const routingHost = "c-internal-routing.qurl.site"
	const browserHost = "b-browser-session.qurl.site"
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, r.Header.Get("X-Forwarded-Host"))
	}))
	defer backend.Close()
	for _, trusted := range []bool{false, true} {
		proxy := NewHTTPReverseProxy(HTTPReverseProxyOptions{TrustForwardedHost: trusted}, NewRouters())
		err := proxy.Register(RouteConfig{Domain: routingHost, CreateConnFn: func(string) (net.Conn, error) {
			return net.Dial("tcp", strings.TrimPrefix(backend.URL, "http://"))
		}})
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			name, forwarded string
			invalid         bool
		}{
			{"qurl_ingress_host", browserHost, false},
			{"client_header", "attacker.example", false},
			{"missing_header", "", false},
			{"comma_list", "attacker.example, " + browserHost, true},
			{"duplicate_lines", browserHost, true},
			{"path", "browser.example/app", true},
			{"userinfo", "attacker@browser.example", true},
			{"invalid_port", "browser.example:bad", true},
			{"valid_port", "browser.example:8443", false},
		} {
			t.Run(fmt.Sprintf("trusted=%t/%s", trusted, tc.name), func(t *testing.T) {
				request := httptest.NewRequest("GET", "http://"+routingHost+"/", nil)
				if tc.forwarded != "" {
					request.Header.Set("X-Forwarded-Host", tc.forwarded)
				}
				if tc.name == "duplicate_lines" {
					request.Header.Add("X-Forwarded-Host", "attacker.example")
				}
				response := httptest.NewRecorder()
				proxy.ServeHTTP(response, request)
				if response.Code != http.StatusOK {
					t.Fatalf("proxy status %d", response.Code)
				}
				want := routingHost
				if trusted && tc.forwarded != "" && !tc.invalid {
					want = tc.forwarded
				}
				if response.Body.String() != want {
					t.Fatalf("forwarded host: got %q, want %q", response.Body.String(), want)
				}
			})
		}
	}
}

func TestTrustedForwardedHostAbsoluteAppAsset(t *testing.T) {
	const routingHost = "c-internal-routing.qurl.site"
	const browserHost = "b-browser-session.qurl.site"
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Forwarded-Host") != browserHost {
			http.Error(w, "wrong browser host", http.StatusBadRequest)
			return
		}
		if r.URL.Path == "/app.js" {
			w.Header().Set("Content-Type", "text/javascript")
			io.WriteString(w, "window.privateAppLoaded = true;")
			return
		}
		// Lerd sets HTTPS for its TLS tunnel; this test covers host preservation only.
		io.WriteString(w, "https://"+r.Header.Get("X-Forwarded-Host")+"/app.js")
	}))
	defer backend.Close()
	proxy := NewHTTPReverseProxy(HTTPReverseProxyOptions{TrustForwardedHost: true}, NewRouters())
	if err := proxy.Register(RouteConfig{Domain: routingHost, CreateConnFn: func(string) (net.Conn, error) {
		return net.Dial("tcp", strings.TrimPrefix(backend.URL, "http://"))
	}}); err != nil {
		t.Fatal(err)
	}
	request := func(path string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "http://"+routingHost+path, nil)
		r.Header.Set("X-Forwarded-Host", browserHost)
		w := httptest.NewRecorder()
		proxy.ServeHTTP(w, r)
		return w
	}
	page := request("/")
	if page.Code != http.StatusOK || page.Body.String() != "https://"+browserHost+"/app.js" {
		t.Fatalf("absolute asset address: status=%d body=%q", page.Code, page.Body.String())
	}
	asset := request(strings.TrimPrefix(page.Body.String(), "https://"+browserHost))
	if asset.Code != http.StatusOK || asset.Body.String() != "window.privateAppLoaded = true;" {
		t.Fatalf("asset: status=%d body=%q", asset.Code, asset.Body.String())
	}
}

func TestValidForwardedHost(t *testing.T) {
	for _, host := range []string{"app.example", "r_session.qurl.site", "app.example:8443", "127.0.0.1", "[::1]", "[::1]:8443"} {
		if !validForwardedHost(host) {
			t.Errorf("rejected valid host %q", host)
		}
	}
	for _, host := range []string{"", "a,b", "a b", "x\"><script>.example", "a;b=c", "a%2eb", "app:0", "app:65536", "app:+80", "a..b", "user@app", "app/path"} {
		if validForwardedHost(host) {
			t.Errorf("accepted invalid host %q", host)
		}
	}
}
