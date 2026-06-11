// cmd/stress/main.go — DKV stress test / load generator.
//
// Usage:
//
//	go run ./cmd/stress --addr localhost:50051 --concurrency 50 --duration 60s
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	v1 "github.com/marcuskal/dkv/pkg/api"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "DKV gRPC address")
	concurrency := flag.Int("concurrency", 20, "Number of parallel workers")
	duration := flag.Duration("duration", 30*time.Second, "How long to run")
	forever := flag.Bool("forever", false, "Run until Ctrl-C instead of stopping after duration")
	keys := flag.Int("keys", 1000, "Key-space size (key-0 .. key-N)")
	readRatio := flag.Float64("read-ratio", 0.7, "Fraction of GETs (0..1)")
	valueSize := flag.Int("value-size", 128, "Bytes per PUT value")
	flag.Parse()

	fmt.Printf("DKV Stress Test\n")
	runMode := duration.String()
	if *forever || *duration <= 0 {
		runMode = "until Ctrl-C"
	}
	fmt.Printf("  addr=%s  workers=%d  duration=%s\n", *addr, *concurrency, runMode)
	fmt.Printf("  keys=%d  read-ratio=%.0f%%  value-size=%dB\n\n", *keys, *readRatio*100, *valueSize)

	conn, err := grpc.NewClient(*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype("json")),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	client := v1.NewClient(conn)

	// Seed the key space so GETs don't all return NotFound.
	fmt.Printf("Seeding %d keys...\n", *keys)
	seedVal := make([]byte, *valueSize)
	for i := range seedVal {
		seedVal[i] = 'x'
	}
	for i := 0; i < *keys; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, _ = client.Put(ctx, &v1.PutRequest{Key: fmt.Sprintf("stress-key-%d", i), Value: seedVal})
		cancel()
	}
	fmt.Printf("Seeding done. Starting load...\n\n")

	var (
		totalOps   atomic.Int64
		totalPuts  atomic.Int64
		totalGets  atomic.Int64
		totalDels  atomic.Int64
		totalErrs  atomic.Int64
		totalBytes atomic.Int64
	)

	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	if *forever || *duration <= 0 {
		ctx, cancel = context.WithCancel(context.Background())
	} else {
		ctx, cancel = context.WithTimeout(context.Background(), *duration)
	}
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigCh; cancel() }()

	value := make([]byte, *valueSize)
	rand.Read(value)

	var wg sync.WaitGroup
	start := time.Now()

	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(rand.Int63()))
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				key := fmt.Sprintf("stress-key-%d", rng.Intn(*keys))
				opCtx, opCancel := context.WithTimeout(ctx, 2*time.Second)
				roll := rng.Float64()
				var opErr error
				switch {
				case roll < *readRatio:
					resp, err := client.Get(opCtx, &v1.GetRequest{Key: key})
					if err == nil {
						totalBytes.Add(int64(len(resp.Value)))
						totalGets.Add(1)
					} else {
						opErr = err
					}
				case roll < *readRatio+((1-*readRatio)/2):
					_, err := client.Put(opCtx, &v1.PutRequest{Key: key, Value: value})
					if err == nil {
						totalPuts.Add(1)
					} else {
						opErr = err
					}
				default:
					_, _ = client.Delete(opCtx, &v1.DeleteRequest{Key: key})
					_, err := client.Put(opCtx, &v1.PutRequest{Key: key, Value: value})
					if err == nil {
						totalDels.Add(1)
					} else {
						opErr = err
					}
				}
				opCancel()
				totalOps.Add(1)
				if opErr != nil {
					totalErrs.Add(1)
				}
			}
		}()
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var lastOps int64

	fmt.Printf("%-8s %-10s %-10s %-10s %-10s %-10s %-10s\n",
		"Time", "Ops/s", "Total", "PUTs", "GETs", "DELs", "Errors")
	fmt.Println("-----------------------------------------------------------------------")

	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			wg.Wait()
			elapsed := time.Since(start)
			ops := totalOps.Load()
			errs := totalErrs.Load()
			errPct := 0.0
			if ops > 0 {
				errPct = float64(errs) / float64(ops) * 100
			}
			bytesGB := float64(totalBytes.Load()) / 1e6
			fmt.Println("\n-----------------------------------------------------------------------")
			fmt.Printf("FINAL SUMMARY  elapsed: %s\n", elapsed.Round(time.Millisecond))
			fmt.Printf("  Total ops:  %d  (%.0f ops/sec)\n", ops, float64(ops)/elapsed.Seconds())
			fmt.Printf("  PUTs:       %d\n", totalPuts.Load())
			fmt.Printf("  GETs:       %d  (%.1f MB read)\n", totalGets.Load(), bytesGB)
			fmt.Printf("  DELs:       %d\n", totalDels.Load())
			fmt.Printf("  Errors:     %d  (%.2f%%)\n", errs, errPct)
			return

		case t := <-ticker.C:
			cur := totalOps.Load()
			elapsed := t.Sub(start).Round(time.Second)
			fmt.Printf("%-8s %-10d %-10d %-10d %-10d %-10d %-10d\n",
				elapsed, cur-lastOps, cur,
				totalPuts.Load(), totalGets.Load(), totalDels.Load(), totalErrs.Load())
			lastOps = cur
		}
	}
}
