package tls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/credentials"

	"github.com/hunkvm/locator/server/config"
)

// Loader manages TLS certificate material for a single gRPC channel.
// It loads certificates from disk at construction time and optionally
// reloads them on a configurable interval, enabling hot rotation without
// a process restart.
type Loader struct {
	cfg    config.TLSConfig
	logger *zap.Logger

	cert   atomic.Pointer[tls.Certificate]
	caPool atomic.Pointer[x509.CertPool]

	stopCh chan struct{}
}

// New loads certificate material from the paths in cfg and returns a Loader.
// If RefreshInterval > 0, a background goroutine begins reloading certificates
// at that interval. Returns an error if any file is missing or contains invalid
// PEM data.
func New(cfg config.TLSConfig, logger *zap.Logger) (*Loader, error) {
	l := &Loader{
		cfg:    cfg,
		logger: logger,
		stopCh: make(chan struct{}),
	}
	if err := l.load(); err != nil {
		return nil, err
	}
	if cfg.RefreshInterval > 0 {
		go l.reloadLoop()
	}
	return l, nil
}

// ServerCredentials returns gRPC transport credentials for use as a server option.
// The returned credentials require client certificate verification (mTLS). The
// active certificate and CA pool are read atomically on each TLS handshake, so
// hot-rotated certificates take effect without restarting the gRPC server.
func (l *Loader) ServerCredentials() credentials.TransportCredentials {
	return credentials.NewTLS(&tls.Config{
		GetConfigForClient: func(_ *tls.ClientHelloInfo) (*tls.Config, error) {
			return &tls.Config{
				Certificates: []tls.Certificate{*l.cert.Load()},
				ClientCAs:    l.caPool.Load(),
				ClientAuth:   tls.RequireAndVerifyClientCert,
			}, nil
		},
	})
}

// ClientCredentials returns gRPC transport credentials for use when dialing a peer.
// serverName is the expected server name in the peer's certificate (typically the
// peer's hostname). The active certificate and CA pool are read atomically on each
// TLS handshake.
func (l *Loader) ClientCredentials(serverName string) credentials.TransportCredentials {
	return credentials.NewTLS(&tls.Config{
		ServerName: serverName,
		RootCAs:    l.caPool.Load(),
		GetClientCertificate: func(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return l.cert.Load(), nil
		},
	})
}

// Stop terminates the background reload goroutine. It is safe to call multiple
// times and is a no-op if no reload goroutine was started.
func (l *Loader) Stop() {
	select {
	case <-l.stopCh:
	default:
		close(l.stopCh)
	}
}

func (l *Loader) load() error {
	cert, err := loadCert(l.cfg.CertificateFile, l.cfg.PrivateKeyFile)
	if err != nil {
		return fmt.Errorf("tls: load cert/key: %w", err)
	}
	pool, err := loadCAPool(l.cfg.CaCertificateFile)
	if err != nil {
		return fmt.Errorf("tls: load CA cert: %w", err)
	}
	l.cert.Store(cert)
	l.caPool.Store(pool)
	return nil
}

func (l *Loader) reloadLoop() {
	ticker := time.NewTicker(l.cfg.RefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := l.load(); err != nil {
				l.logger.Error("tls: certificate reload failed; retaining previous certs", zap.Error(err))
			} else {
				l.logger.Info("tls: certificates reloaded")
			}
		case <-l.stopCh:
			return
		}
	}
}

func loadCert(certPath, keyPath string) (*tls.Certificate, error) {
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("read cert file %q: %w", certPath, err)
	}
	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("read key file %q: %w", keyPath, err)
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("parse cert/key (%q, %q): %w", certPath, keyPath, err)
	}
	return &cert, nil
}

func loadCAPool(caPath string) (*x509.CertPool, error) {
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read CA cert file %q: %w", caPath, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("parse CA cert file %q: no valid PEM certificates found", caPath)
	}
	return pool, nil
}
