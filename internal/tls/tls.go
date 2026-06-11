// Package tls provides TLS/mTLS configuration for the quoll gRPC server.
//
// Three modes:
//  1. No TLS — dev only.
//  2. Server-side TLS — server presents a cert, client verifies it; traffic
//     is encrypted but the server doesn't know who the client is.
//  3. Mutual TLS — both sides present certs; the server verifies client identity,
//     which enables certificate-based service authorization.
package tls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// Config holds paths to TLS certificate files.
type Config struct {
	CertFile string // Server certificate (PEM)
	KeyFile  string // Server private key (PEM)
	CAFile   string // CA certificate for verifying client certs (mTLS)
}

// ServerTLSConfig builds a *tls.Config for the gRPC server.
//
// If CAFile is provided, mTLS is enabled: the server will require and verify
// client certificates signed by that CA. Without CAFile, it's server-side
// TLS only (encrypted but no client identity verification).
func ServerTLSConfig(cfg Config) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("tls: load keypair: %w", err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	// If CA file provided, enable mTLS
	if cfg.CAFile != "" {
		caPEM, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("tls: read CA file: %w", err)
		}

		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("tls: failed to parse CA certificate")
		}

		// RequireAndVerifyClientCert = full mTLS
		// The server will reject any client that doesn't present a valid cert.
		tlsCfg.ClientCAs = caPool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return tlsCfg, nil
}

// ClientTLSConfig builds a *tls.Config for gRPC clients.
// Used in tests (bufconn) and by external clients connecting to the server.
func ClientTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("tls: read CA file: %w", err)
	}

	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("tls: failed to parse CA certificate")
	}

	tlsCfg := &tls.Config{
		RootCAs:    caPool,
		MinVersion: tls.VersionTLS12,
	}

	// If client cert provided, load it (for mTLS)
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("tls: load client keypair: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return tlsCfg, nil
}
