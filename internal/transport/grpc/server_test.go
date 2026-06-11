package server_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/marcuskal/dkv/internal/config"
	"github.com/marcuskal/dkv/internal/engine"
	"github.com/marcuskal/dkv/internal/hashring"
	dkvraft "github.com/marcuskal/dkv/internal/raft"
	"github.com/marcuskal/dkv/internal/router"
	server "github.com/marcuskal/dkv/internal/transport/grpc"
	v1 "github.com/marcuskal/dkv/pkg/api"
)

const bufSize = 1024 * 1024

func testLogger() zerolog.Logger {
	return zerolog.New(os.Stderr).Level(zerolog.Disabled)
}

// --- Single-node tests (backward compatibility) ---

func setupSingleNode(t *testing.T) (*server.Server, *grpc.ClientConn, func()) {
	t.Helper()

	dir := t.TempDir()
	log := testLogger()

	eng, err := engine.New(dir, "always", 1024, 1048576, log)
	if err != nil {
		t.Fatalf("create engine: %v", err)
	}

	cfg := config.Config{
		GRPC: config.GRPCConfig{
			ListenAddr:     ":0",
			RequestTimeout: 5 * time.Second,
		},
	}

	// No raft, no router → single-node mode.
	srv, err := server.New(eng, nil, nil, "", cfg, log, nil, nil)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}

	lis := bufconn.Listen(bufSize)
	go srv.ServeOnListener(lis)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype("json")),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}

	cleanup := func() {
		conn.Close()
		srv.GracefulStop()
		eng.Close()
	}

	return srv, conn, cleanup
}

func TestSingleNode_PutGetDelete(t *testing.T) {
	_, conn, cleanup := setupSingleNode(t)
	defer cleanup()

	client := v1.NewClient(conn)
	ctx := context.Background()

	_, err := client.Put(ctx, &v1.PutRequest{Key: "greeting", Value: []byte("hello")})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	resp, err := client.Get(ctx, &v1.GetRequest{Key: "greeting"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(resp.Value) != "hello" {
		t.Fatalf("expected 'hello', got '%s'", string(resp.Value))
	}

	_, err = client.Delete(ctx, &v1.DeleteRequest{Key: "greeting"})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err = client.Get(ctx, &v1.GetRequest{Key: "greeting"})
	if err == nil {
		t.Fatal("expected error after delete, got nil")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestSingleNode_GetNotFound(t *testing.T) {
	_, conn, cleanup := setupSingleNode(t)
	defer cleanup()

	client := v1.NewClient(conn)
	_, err := client.Get(context.Background(), &v1.GetRequest{Key: "nonexistent"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestSingleNode_EmptyKey(t *testing.T) {
	_, conn, cleanup := setupSingleNode(t)
	defer cleanup()

	client := v1.NewClient(conn)
	_, err := client.Put(context.Background(), &v1.PutRequest{Key: "", Value: []byte("v")})
	if err == nil {
		t.Fatal("expected error for empty key")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

// --- Cluster-mode tests ---

func setupRaftNode(t *testing.T, nodeID string, bootstrap bool, raftPort, grpcPort int) (*server.Server, *engine.Engine, *dkvraft.Node, *hashring.Ring, *grpc.ClientConn, func()) {
	t.Helper()

	dir := t.TempDir()
	log := testLogger()

	eng, err := engine.New(dir+"/wal", "always", 1024, 1048576, log)
	if err != nil {
		t.Fatalf("create engine: %v", err)
	}

	raftCfg := config.RaftConfig{
		NodeID:             nodeID,
		BindAddr:           fmt.Sprintf("127.0.0.1:%d", raftPort),
		DataDir:            dir + "/raft",
		Bootstrap:          bootstrap,
		HeartbeatTimeout:   300 * time.Millisecond,
		ElectionTimeout:    300 * time.Millisecond,
		LeaderLeaseTimeout: 200 * time.Millisecond,
		CommitTimeout:      50 * time.Millisecond,
		SnapshotInterval:   30 * time.Second,
		SnapshotThreshold:  128,
		TrailingLogs:       256,
	}

	raftNode, err := dkvraft.NewNode(eng, raftCfg, log)
	if err != nil {
		t.Fatalf("create raft node: %v", err)
	}

	// Create hash ring + router for this node.
	ring := hashring.New(64)
	ring.AddNode(nodeID)
	rtr := router.New(ring, nodeID, log)

	cfg := config.Config{
		GRPC: config.GRPCConfig{
			ListenAddr:     fmt.Sprintf("127.0.0.1:%d", grpcPort),
			RequestTimeout: 5 * time.Second,
		},
		Raft: raftCfg,
	}

	srv, err := server.New(eng, raftNode, rtr, nodeID, cfg, log, nil, nil)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}

	lis := bufconn.Listen(bufSize)
	go srv.ServeOnListener(lis)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype("json")),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	cleanup := func() {
		conn.Close()
		srv.GracefulStop()
		rtr.Shutdown()
		raftNode.Shutdown()
		eng.Close()
	}

	return srv, eng, raftNode, ring, conn, cleanup
}

func TestCluster_LeaderWrite(t *testing.T) {
	_, _, raftNode, _, conn, cleanup := setupRaftNode(t, "node-1", true, 19091, 19191)
	defer cleanup()

	if err := raftNode.WaitForLeader(3 * time.Second); err != nil {
		t.Fatalf("wait for leader: %v", err)
	}

	client := v1.NewClient(conn)
	ctx := context.Background()

	_, err := client.Put(ctx, &v1.PutRequest{Key: "cluster-key", Value: []byte("cluster-value")})
	if err != nil {
		t.Fatalf("Put on leader: %v", err)
	}

	resp, err := client.Get(ctx, &v1.GetRequest{Key: "cluster-key"})
	if err != nil {
		t.Fatalf("Get on leader: %v", err)
	}
	if string(resp.Value) != "cluster-value" {
		t.Fatalf("expected 'cluster-value', got '%s'", string(resp.Value))
	}
}

func TestCluster_MultiNode(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-node test in short mode")
	}

	// Node 1: bootstrap leader
	_, _, node1, _, conn1, cleanup1 := setupRaftNode(t, "node-1", true, 19291, 19391)
	defer cleanup1()

	if err := node1.WaitForLeader(3 * time.Second); err != nil {
		t.Fatalf("node-1 wait for leader: %v", err)
	}

	// Node 2: follower
	_, _, _, _, conn2, cleanup2 := setupRaftNode(t, "node-2", false, 19292, 19392)
	defer cleanup2()

	// Node 3: follower
	_, _, _, _, conn3, cleanup3 := setupRaftNode(t, "node-3", false, 19293, 19393)
	defer cleanup3()

	// Leader adds voters
	if err := node1.AddVoter("node-2", "127.0.0.1:19292"); err != nil {
		t.Fatalf("add voter node-2: %v", err)
	}
	if err := node1.AddVoter("node-3", "127.0.0.1:19293"); err != nil {
		t.Fatalf("add voter node-3: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	client1 := v1.NewClient(conn1)
	client2 := v1.NewClient(conn2)
	client3 := v1.NewClient(conn3)
	ctx := context.Background()

	// Write on leader
	_, err := client1.Put(ctx, &v1.PutRequest{Key: "replicated", Value: []byte("data")})
	if err != nil {
		t.Fatalf("Put on leader: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	// Read on followers
	resp, err := client2.Get(ctx, &v1.GetRequest{Key: "replicated"})
	if err != nil {
		t.Fatalf("Get on node-2: %v", err)
	}
	if string(resp.Value) != "data" {
		t.Fatalf("node-2: expected 'data', got '%s'", string(resp.Value))
	}

	resp, err = client3.Get(ctx, &v1.GetRequest{Key: "replicated"})
	if err != nil {
		t.Fatalf("Get on node-3: %v", err)
	}
	if string(resp.Value) != "data" {
		t.Fatalf("node-3: expected 'data', got '%s'", string(resp.Value))
	}

	// Write on follower → Unavailable
	_, err = client2.Put(ctx, &v1.PutRequest{Key: "bad", Value: []byte("write")})
	if err == nil {
		t.Fatal("expected error writing to follower")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %v", err)
	}

	// Delete on leader, verify replication
	_, err = client1.Delete(ctx, &v1.DeleteRequest{Key: "replicated"})
	if err != nil {
		t.Fatalf("Delete on leader: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	_, err = client2.Get(ctx, &v1.GetRequest{Key: "replicated"})
	if err == nil {
		t.Fatal("expected NotFound after replicated delete")
	}
	st, ok = status.FromError(err)
	if !ok || st.Code() != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}

	servers, err := node1.GetConfiguration()
	if err != nil {
		t.Fatalf("get configuration: %v", err)
	}
	if len(servers) != 3 {
		t.Fatalf("expected 3 servers, got %d", len(servers))
	}
}

// --- Hash ring routing test ---

func TestHashRing_LocalKeyRouting(t *testing.T) {
	// Verify that a single-node ring routes all keys locally.
	ring := hashring.New(64)
	ring.AddNode("node-1")
	log := testLogger()
	rtr := router.New(ring, "node-1", log)
	defer rtr.Shutdown()

	// All keys should be local when there's only one node.
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		if !rtr.IsLocalKey(key) {
			t.Fatalf("key %s should be local on single-node ring", key)
		}
	}
}

func TestHashRing_MultiNodeOwnership(t *testing.T) {
	ring := hashring.New(64)
	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")

	log := testLogger()
	rtr := router.New(ring, "node-1", log)
	defer rtr.Shutdown()

	localCount := 0
	total := 1000
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key-%d", i)
		if rtr.IsLocalKey(key) {
			localCount++
		}
	}

	// With 3 nodes, ~33% of keys should be local.
	ratio := float64(localCount) / float64(total)
	if ratio < 0.15 || ratio > 0.55 {
		t.Fatalf("expected ~33%% local keys, got %.1f%% (%d/%d)", ratio*100, localCount, total)
	}
	t.Logf("local keys: %d/%d (%.1f%%)", localCount, total, ratio*100)
}
