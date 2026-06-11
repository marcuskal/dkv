// cmd/healthcheck probes the dkv health endpoint for use as a Docker HEALTHCHECK.
// Exits 0 if the node is live, 1 otherwise.
package main

import (
	"flag"
	"net/http"
	"os"
)

func main() {
	addr := flag.String("addr", "http://localhost:9093/healthz", "health endpoint URL")
	flag.Parse()

	resp, err := http.Get(*addr) //nolint:gosec,noctx
	if err != nil || resp.StatusCode != http.StatusOK {
		os.Exit(1)
	}
}
