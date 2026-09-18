package client

import (
	"context"
	"crypto/tls"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	v1 "github.com/fatedier/frp/pkg/config/v1"
)

func TestConnectorCertificateVerification(t *testing.T) {
	t.Setenv("http_proxy", "")
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	host, port, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, ca, serverName string
		verify, wantError    bool
	}{
		{"legacy", "", "example.com", false, false},
		{"system roots reject unknown issuer", "", "example.com", true, true},
		{"custom root", ca, "example.com", true, false},
		{"wrong name", ca, "wrong.example", true, true},
		{"custom root always verifies", ca, "wrong.example", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &v1.ClientCommonConfig{ServerAddr: host}
			cfg.ServerPort, _ = strconv.Atoi(port)
			if err := cfg.Complete(); err != nil {
				t.Fatal(err)
			}
			cfg.Transport.TLS.VerifyServerCertificate = tc.verify
			cfg.Transport.TLS.TrustedCaFile = tc.ca
			cfg.Transport.TLS.ServerName = tc.serverName
			conn, err := (&defaultConnectorImpl{ctx: context.Background(), cfg: cfg}).realConnect()
			if conn != nil {
				if err == nil {
					_, err = conn.Write([]byte("GET / HTTP/1.0\r\n\r\n"))
				}
				conn.Close()
			}
			if (err != nil) != tc.wantError {
				t.Fatalf("connection error = %v, want error %v", err, tc.wantError)
			}
		})
	}
}

func TestQUICCertificateVerification(t *testing.T) {
	certificate := httptest.NewTLSServer(http.NotFoundHandler())
	defer certificate.Close()
	listener, err := quic.ListenAddr("127.0.0.1:0", &tls.Config{Certificates: certificate.TLS.Certificates, NextProtos: []string{"frp"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx := t.Context()
	go func() {
		for {
			if _, err := listener.Accept(ctx); err != nil {
				return
			}
		}
	}()
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, caFile := range []string{"", ca} {
		for _, enabled := range []bool{false, true} {
			cfg := &v1.ClientCommonConfig{ServerAddr: "127.0.0.1", ServerPort: listener.Addr().(*net.UDPAddr).Port}
			cfg.Transport.Protocol = "quic"
			cfg.Transport.TLS.Enable = &enabled
			cfg.Transport.TLS.VerifyServerCertificate = true
			cfg.Transport.TLS.ServerName = "example.com"
			cfg.Transport.TLS.TrustedCaFile = caFile
			if err := cfg.Complete(); err != nil {
				t.Fatal(err)
			}
			attemptCtx, stop := context.WithTimeout(ctx, 10*time.Second)
			connector := NewConnector(attemptCtx, cfg)
			err := connector.Open()
			connector.Close()
			stop()
			if caFile != "" {
				if err != nil {
					t.Fatalf("QUIC custom CA with TLS.Enable=%t: %v", enabled, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "unknown authority") {
				t.Fatalf("QUIC error with TLS.Enable=%t: %v; want unknown authority", enabled, err)
			}
		}
	}
}

func TestCertificateVerificationRejectsPlaintext(t *testing.T) {
	cfg := &v1.ClientCommonConfig{}
	disabled := false
	cfg.Transport.TLS.Enable = &disabled
	cfg.Transport.TLS.VerifyServerCertificate = true
	if err := cfg.Complete(); err != nil {
		t.Fatal(err)
	}
	if _, err := (&defaultConnectorImpl{ctx: t.Context(), cfg: cfg}).realConnect(); err == nil || !strings.Contains(err.Error(), "requires TLS") {
		t.Fatalf("plaintext error = %v, want TLS requirement", err)
	}
}
