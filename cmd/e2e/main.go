// Command e2e is a black-box, Docker-based end-to-end test for convergeKV.
//
// It brings up a real multi-container cluster with `docker compose`, drives it
// over the public gRPC API, and manipulates the Docker network to create
// partitions and delays. It then verifies the store's strong-eventual-
// consistency guarantees across five scenarios, printing PASS/FAIL after each:
//
//  1. basic replication of a normal write,
//  2. concurrent writes to the same key on DIFFERENT fields across replicas,
//  3. concurrent writes to the same key on the SAME field across replicas,
//  4. delayed / out-of-order delivery,
//  5. eventual convergence after a temporary partition heals.
//
// How it works (see docs/concepts and CLAUDE.md for the model):
//
//   - The cluster is N=4 (seed + 3 nodes), RF=3, so every key lives on exactly
//     3 of the 4 nodes. Verification first resolves a key's owners (partition =
//     xxhash(key) % P, then the owners table from Debug.Inspect) and compares
//     only those three.
//   - Verification reads use Debug.DumpDocuments, which returns what a node
//     PHYSICALLY holds (local-only, never a forwarded read) — so an equal dump
//     across owners genuinely proves replication, not read-time forwarding.
//   - A write is always applied by the partition's top-HRW owner (the applier).
//     When nodes share a membership view they share an applier, so two
//     "concurrent" writes would just be serialized. To create genuine
//     concurrency, scenarios 2–5 ISOLATE the key's applier (owners[0]) from the
//     cluster network and write to both the isolated island and the majority
//     side, then heal and verify the merge.
//   - Two Docker networks (see docker-compose.e2e.yml): an INTERNAL `cluster`
//     network carries all inter-node gossip/RPC and is what we disconnect to
//     partition a node; a separate `edge` network owns the published client
//     ports and host access and is never disconnected — so the host can still
//     read/write a node even while it is partitioned off the cluster.
//   - On heal we reconnect a node with its ORIGINAL cluster-network IP
//     (docker network connect --ip), because its advertised gossip/RPC address
//     was pinned to that static IP at boot (CONVERGEKV_ADVERTISE_ADDR).
//
// Run with `make e2e` or `go run ./cmd/e2e`. Requires Docker. Pass -keep to
// leave the cluster running for inspection after the run.
package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/janthoXO/convergeKV/pkg/proto"
)

const (
	project     = "convergekv-e2e"
	composeFile = "docker-compose.e2e.yml"
	// clusterNet is the INTERNAL network carrying gossip/RPC; disconnecting a
	// node from it partitions that node. (Compose prefixes the project name.)
	// The published client port lives on the separate `edge` network, which is
	// never disconnected, so the host can still reach a partitioned node.
	clusterNet = project + "_cluster"
	rpcTimeout = 8 * time.Second
)

// node is one cluster member and our client handle to it.
type node struct {
	svc       string // compose service ("seed" / "node")
	name      string // container name (for display)
	id        string // container ID (for docker network commands)
	hostAddr  string // 127.0.0.1:<published 7000> (via the edge network)
	ip        string // static IP on the cluster network (restored on reconnect)
	connected bool   // attached to the cluster network?

	conn   *grpc.ClientConn
	kv     pb.KVClient
	dbg    pb.DebugClient
	nodeID [16]byte
}

// docEntry is one document as a node physically holds it.
type docEntry struct {
	found     bool
	doc       string            // rendered JSON
	ctxHash   string            // hex of the causal-context hash
	tombstone bool              // deleted residual
	fields    map[string]string // field name -> raw JSON value of the LWW winner
}

type harness struct {
	nodes []*node
	byID  map[[16]byte]*node
	p     uint64 // partition count P
	keep  bool
}

func main() {
	keep := flag.Bool("keep", false, "leave the cluster running after the test")
	flag.Parse()

	h := &harness{byID: map[[16]byte]*node{}, keep: *keep}
	code := h.run()
	os.Exit(code)
}

func (h *harness) run() int {
	ctx := context.Background()

	fmt.Println("== convergeKV end-to-end test ==")
	if err := h.setup(ctx); err != nil {
		fmt.Printf("setup failed: %v\n", err)
		h.teardown()
		return 1
	}
	defer h.teardown()

	scenarios := []struct {
		name string
		fn   func(context.Context) bool
	}{
		{"1. basic replication", h.scenarioBasicReplication},
		{"2. concurrent writes, different fields", h.scenarioConcurrentDifferentFields},
		{"3. concurrent writes, same field", h.scenarioConcurrentSameField},
		{"4. delayed / out-of-order delivery", h.scenarioDelayedDelivery},
		{"5. convergence after partition heals", h.scenarioPartitionHeal},
	}

	allPass := true
	for _, s := range scenarios {
		fmt.Printf("\n--- Scenario %s ---\n", s.name)
		pass := s.fn(ctx)
		if pass {
			fmt.Printf("RESULT: PASS  (%s)\n", s.name)
		} else {
			fmt.Printf("RESULT: FAIL  (%s)\n", s.name)
			allPass = false
		}
	}

	fmt.Println("\n== summary ==")
	if allPass {
		fmt.Println("ALL SCENARIOS PASSED")
		return 0
	}
	fmt.Println("SOME SCENARIOS FAILED")
	return 1
}

// --- setup / teardown --------------------------------------------------------

func (h *harness) setup(ctx context.Context) error {
	fmt.Println("[setup] bringing up the cluster (docker compose up --build)...")
	if err := composeUp(ctx); err != nil {
		return fmt.Errorf("compose up: %w", err)
	}

	fmt.Println("[setup] discovering containers...")
	if err := h.discover(ctx); err != nil {
		return err
	}

	fmt.Println("[setup] dialing gRPC and waiting for the cluster to form...")
	for _, n := range h.nodes {
		conn, err := grpc.NewClient(n.hostAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial %s: %w", n.hostAddr, err)
		}
		n.conn = conn
		n.kv = pb.NewKVClient(conn)
		n.dbg = pb.NewDebugClient(conn)
	}

	if !waitUntil(90*time.Second, 2*time.Second, func() bool { return h.clusterFormed(ctx) }) {
		return errors.New("cluster did not form within timeout")
	}
	if err := h.cacheNodeIDs(ctx); err != nil {
		return err
	}
	if !waitUntil(60*time.Second, 1*time.Second, func() bool { return h.clusterReady(ctx) }) {
		return errors.New("partitions did not all activate within timeout")
	}
	fmt.Printf("[setup] cluster ready: %d nodes, P=%d, RF=3\n", len(h.nodes), h.p)
	return nil
}

func (h *harness) teardown() {
	ctx := context.Background()
	// Always restore connectivity so compose down is clean.
	for _, n := range h.nodes {
		h.reconnect(ctx, n)
		if n.conn != nil {
			_ = n.conn.Close()
		}
	}
	if h.keep {
		fmt.Println("\n[teardown] -keep set; leaving the cluster up. Inspect with:")
		fmt.Printf("           docker compose -p %s -f %s ps\n", project, composeFile)
		fmt.Printf("           docker compose -p %s -f %s down -v   # when done\n", project, composeFile)
		return
	}
	fmt.Println("\n[teardown] tearing down the cluster...")
	if _, err := docker(ctx, "compose", "-p", project, "-f", composeFile, "down", "-v"); err != nil {
		fmt.Printf("[teardown] compose down: %v\n", err)
	}
}

// discover lists the compose containers and records each one's published 7000
// port and cluster-network IP.
func (h *harness) discover(ctx context.Context) error {
	out, err := docker(ctx, "compose", "-p", project, "-f", composeFile, "ps", "-q")
	if err != nil {
		return fmt.Errorf("compose ps: %w", err)
	}
	ids := strings.Fields(out)
	if len(ids) == 0 {
		return errors.New("no containers found")
	}

	args := append([]string{"inspect"}, ids...)
	raw, err := docker(ctx, args...)
	if err != nil {
		return fmt.Errorf("inspect: %w", err)
	}

	var inspected []struct {
		ID     string `json:"Id"`
		Name   string `json:"Name"`
		Config struct {
			Labels map[string]string `json:"Labels"`
		} `json:"Config"`
		NetworkSettings struct {
			Ports map[string][]struct {
				HostIP   string `json:"HostIp"`
				HostPort string `json:"HostPort"`
			} `json:"Ports"`
			Networks map[string]struct {
				IPAddress string `json:"IPAddress"`
			} `json:"Networks"`
		} `json:"NetworkSettings"`
	}
	if err := json.Unmarshal([]byte(raw), &inspected); err != nil {
		return fmt.Errorf("parse inspect: %w", err)
	}

	for _, in := range inspected {
		binds := in.NetworkSettings.Ports["7000/tcp"]
		if len(binds) == 0 {
			return fmt.Errorf("container %s has no published 7000/tcp port", in.Name)
		}
		net, ok := in.NetworkSettings.Networks[clusterNet]
		if !ok {
			return fmt.Errorf("container %s not on network %s", in.Name, clusterNet)
		}
		h.nodes = append(h.nodes, &node{
			svc:       in.Config.Labels["com.docker.compose.service"],
			name:      strings.TrimPrefix(in.Name, "/"),
			id:        in.ID,
			hostAddr:  "127.0.0.1:" + binds[0].HostPort,
			ip:        net.IPAddress,
			connected: true,
		})
	}
	// Stable order: seed first, then node-1, node-2, ... (for readable output).
	sort.Slice(h.nodes, func(i, j int) bool {
		if (h.nodes[i].svc == "seed") != (h.nodes[j].svc == "seed") {
			return h.nodes[i].svc == "seed"
		}
		return h.nodes[i].name < h.nodes[j].name
	})
	return nil
}

func (h *harness) cacheNodeIDs(ctx context.Context) error {
	for _, n := range h.nodes {
		resp, err := h.inspect(ctx, n)
		if err != nil {
			return fmt.Errorf("inspect %s: %w", n.name, err)
		}
		if len(resp.GetNodeId()) != 16 {
			return fmt.Errorf("node %s returned a %d-byte id", n.name, len(resp.GetNodeId()))
		}
		copy(n.nodeID[:], resp.GetNodeId())
		h.byID[n.nodeID] = n
		if h.p == 0 {
			h.p = uint64(resp.GetPartitions())
		}
	}
	if h.p == 0 {
		return errors.New("partition count P is zero")
	}
	return nil
}

// --- scenarios ---------------------------------------------------------------

func (h *harness) scenarioBasicReplication(ctx context.Context) bool {
	const key = "s1-basic"
	owners, err := h.ownersOf(ctx, key)
	if err != nil || len(owners) < 3 {
		fmt.Printf("  could not resolve owners: %v\n", err)
		return false
	}
	step("owners of %q: %s", key, names(owners))

	writer := h.nodes[0] // any node; it forwards to the applier
	step("PUT %s = {\"v\":1} via %s", key, writer.name)
	if err := h.put(ctx, writer, key, `{"v":1}`); err != nil {
		fmt.Printf("  put failed: %v\n", err)
		return false
	}

	step("waiting for replication to all owners...")
	if !h.waitConverged(ctx, owners, key) {
		fmt.Println("  did not converge")
		return false
	}
	return h.verify(ctx, key, owners, func(d docEntry) error {
		return wantFields(d, map[string]string{"v": "1"})
	})
}

func (h *harness) scenarioConcurrentDifferentFields(ctx context.Context) bool {
	const key = "s2-difffields"
	owners, ok := h.partitionApplier(ctx, key)
	if !ok {
		return false
	}
	o0, o1 := owners[0], owners[1]

	step("concurrent PATCH on each side of the partition")
	step("  PATCH %s += {\"a\":1} via isolated %s", key, o0.name)
	step("  PATCH %s += {\"b\":2} via %s", key, o1.name)
	err1 := h.patch(ctx, o0, key, `{"a":1}`)
	err2 := h.patch(ctx, o1, key, `{"b":2}`)
	if err1 != nil || err2 != nil {
		fmt.Printf("  patch failed: %v / %v\n", err1, err2)
		h.reconnect(ctx, o0)
		return false
	}

	if !h.healAndConverge(ctx, o0, owners, key) {
		return false
	}
	return h.verify(ctx, key, owners, func(d docEntry) error {
		return wantFields(d, map[string]string{"a": "1", "b": "2"})
	})
}

func (h *harness) scenarioConcurrentSameField(ctx context.Context) bool {
	const key = "s3-samefield"
	owners, ok := h.partitionApplier(ctx, key)
	if !ok {
		return false
	}
	o0, o1 := owners[0], owners[1]

	step("conflicting PATCH on the SAME field on each side")
	step("  PATCH %s = {\"x\":\"o0\"} via isolated %s", key, o0.name)
	step("  PATCH %s = {\"x\":\"o1\"} via %s", key, o1.name)
	err1 := h.patch(ctx, o0, key, `{"x":"o0"}`)
	err2 := h.patch(ctx, o1, key, `{"x":"o1"}`)
	if err1 != nil || err2 != nil {
		fmt.Printf("  patch failed: %v / %v\n", err1, err2)
		h.reconnect(ctx, o0)
		return false
	}

	if !h.healAndConverge(ctx, o0, owners, key) {
		return false
	}
	return h.verify(ctx, key, owners, func(d docEntry) error {
		// LWW must pick ONE winner; verify() already proved all owners agree.
		got, ok := d.fields["x"]
		if !ok {
			return errors.New("field x missing")
		}
		if got != `"o0"` && got != `"o1"` {
			return fmt.Errorf("x=%s is neither candidate", got)
		}
		step("  conflict resolved consistently: x=%s on every owner", got)
		return nil
	})
}

func (h *harness) scenarioDelayedDelivery(ctx context.Context) bool {
	const key = "s4-delayed"
	owners, ok := h.partitionApplier(ctx, key)
	if !ok {
		return false
	}
	o0, o1 := owners[0], owners[1]

	step("write on the isolated side gets DELAYED; two more writes happen meanwhile")
	step("  PATCH %s += {\"x\":1} via isolated %s (delivery delayed until heal)", key, o0.name)
	step("  PATCH %s += {\"y\":1} via %s", key, o1.name)
	step("  PATCH %s += {\"z\":1} via %s (later)", key, o1.name)
	err1 := h.patch(ctx, o0, key, `{"x":1}`)
	err2 := h.patch(ctx, o1, key, `{"y":1}`)
	err3 := h.patch(ctx, o1, key, `{"z":1}`)
	if err1 != nil || err2 != nil || err3 != nil {
		fmt.Printf("  patch failed: %v / %v / %v\n", err1, err2, err3)
		h.reconnect(ctx, o0)
		return false
	}

	if !h.healAndConverge(ctx, o0, owners, key) {
		return false
	}
	return h.verify(ctx, key, owners, func(d docEntry) error {
		return wantFields(d, map[string]string{"x": "1", "y": "1", "z": "1"})
	})
}

func (h *harness) scenarioPartitionHeal(ctx context.Context) bool {
	const key = "s5-heal"
	owners, err := h.ownersOf(ctx, key)
	if err != nil || len(owners) < 3 {
		fmt.Printf("  could not resolve owners: %v\n", err)
		return false
	}
	o0, o1 := owners[0], owners[1]
	step("owners of %q: %s", key, names(owners))

	step("PUT %s = {\"val\":\"first\"} and replicate to all owners", key)
	if err := h.put(ctx, h.nodes[0], key, `{"val":"first"}`); err != nil {
		fmt.Printf("  put failed: %v\n", err)
		return false
	}
	if !h.waitConverged(ctx, owners, key) {
		fmt.Println("  initial write did not converge")
		return false
	}

	step("partition applier %s away from the cluster", o0.name)
	h.disconnect(ctx, o0)
	if !h.waitSeenDead(ctx, o1, o0) {
		fmt.Printf("  %s never observed %s as down\n", o1.name, o0.name)
		h.reconnect(ctx, o0)
		return false
	}

	step("PUT %s = {\"val\":\"second\"} via %s while %s is partitioned", key, o1.name, o0.name)
	if err := h.put(ctx, o1, key, `{"val":"second"}`); err != nil {
		fmt.Printf("  put failed: %v\n", err)
		h.reconnect(ctx, o0)
		return false
	}

	if !h.healAndConverge(ctx, o0, owners, key) {
		return false
	}
	return h.verify(ctx, key, owners, func(d docEntry) error {
		// Later write wins by HLC, consistently everywhere.
		return wantFields(d, map[string]string{"val": `"second"`})
	})
}

// partitionApplier resolves a key's owners, isolates the applier (owners[0])
// from the cluster network, and waits until the majority side sees it as down —
// the precondition for two independent appliers.
func (h *harness) partitionApplier(ctx context.Context, key string) ([]*node, bool) {
	owners, err := h.ownersOf(ctx, key)
	if err != nil || len(owners) < 3 {
		fmt.Printf("  could not resolve owners: %v\n", err)
		return nil, false
	}
	step("owners of %q: %s", key, names(owners))
	o0, o1 := owners[0], owners[1]
	step("partition applier %s away from the cluster", o0.name)
	h.disconnect(ctx, o0)
	if !h.waitSeenDead(ctx, o1, o0) {
		fmt.Printf("  %s never observed %s as down\n", o1.name, o0.name)
		h.reconnect(ctx, o0)
		return nil, false
	}
	return owners, true
}

// healAndConverge reconnects the isolated node and waits for anti-entropy to
// reconcile every owner.
func (h *harness) healAndConverge(ctx context.Context, isolated *node, owners []*node, key string) bool {
	step("heal: reconnect %s and wait for anti-entropy to reconcile", isolated.name)
	h.reconnect(ctx, isolated)
	if !waitUntil(60*time.Second, 2*time.Second, func() bool { return h.clusterStable(ctx) }) {
		fmt.Println("  cluster did not re-form after heal")
		return false
	}
	if !h.waitConverged(ctx, owners, key) {
		fmt.Println("  owners did not converge after heal")
		return false
	}
	return true
}

// --- verification ------------------------------------------------------------

// verify freezes the cluster (disconnects every node from the cluster network so
// no live repair can mask a bug), reads each owner's LOCAL copy, asserts they
// are byte-identical and satisfy check, then restores connectivity.
func (h *harness) verify(ctx context.Context, key string, owners []*node, check func(docEntry) error) bool {
	step("freeze: disconnect all nodes from the cluster network, then read each owner locally")
	h.disconnectAll(ctx)
	defer func() {
		h.reconnectAll(ctx)
		waitUntil(60*time.Second, 2*time.Second, func() bool { return h.clusterStable(ctx) })
	}()

	entries := make([]docEntry, len(owners))
	for i, o := range owners {
		dump, err := h.dump(ctx, o)
		if err != nil {
			fmt.Printf("  dump %s failed: %v\n", o.name, err)
			return false
		}
		entries[i] = dump[key]
	}

	fmt.Println("  owner                         found  ctxHash   document")
	for i, o := range owners {
		e := entries[i]
		fmt.Printf("  %-28s  %-5t  %-8s  %s\n", o.name, e.found, short(e.ctxHash), e.doc)
	}

	base := entries[0]
	if !base.found || base.tombstone {
		fmt.Printf("  owner %s does not hold %q\n", owners[0].name, key)
		return false
	}
	for i := 1; i < len(entries); i++ {
		if entries[i].ctxHash != base.ctxHash || entries[i].doc != base.doc {
			fmt.Printf("  DIVERGENCE: %s != %s\n", owners[i].name, owners[0].name)
			return false
		}
	}
	if check != nil {
		if err := check(base); err != nil {
			fmt.Printf("  value check failed: %v\n", err)
			return false
		}
	}
	fmt.Println("  all owners agree ✓")
	return true
}

// --- gRPC helpers ------------------------------------------------------------

func (h *harness) inspect(ctx context.Context, n *node) (*pb.InspectResponse, error) {
	cctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	return n.dbg.Inspect(cctx, &pb.InspectRequest{})
}

func (h *harness) put(ctx context.Context, n *node, key, value string) error {
	return retry(func() error {
		cctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		defer cancel()
		_, err := n.kv.Put(cctx, &pb.PutRequest{Key: key, Value: []byte(value)})
		return err
	})
}

func (h *harness) patch(ctx context.Context, n *node, key, value string) error {
	return retry(func() error {
		cctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		defer cancel()
		_, err := n.kv.Patch(cctx, &pb.PatchRequest{Key: key, Value: []byte(value)})
		return err
	})
}

// dump streams every document a node physically holds into a map keyed by user
// key, retrying to absorb any brief connection blip on the edge network while
// the cluster network is being connected/disconnected.
func (h *harness) dump(ctx context.Context, n *node) (map[string]docEntry, error) {
	var out map[string]docEntry
	err := retry(func() error {
		m, err := h.dumpOnce(ctx, n)
		if err != nil {
			return err
		}
		out = m
		return nil
	})
	return out, err
}

func (h *harness) dumpOnce(ctx context.Context, n *node) (map[string]docEntry, error) {
	cctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	stream, err := n.dbg.DumpDocuments(cctx, &pb.DumpRequest{})
	if err != nil {
		return nil, err
	}
	out := map[string]docEntry{}
	for {
		d, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		fields := map[string]string{}
		for _, f := range d.GetFields() {
			fields[f.GetName()] = string(f.GetValue())
		}
		out[string(d.GetKey())] = docEntry{
			found:     !d.GetTombstone(),
			doc:       string(d.GetDocument()),
			ctxHash:   hex.EncodeToString(d.GetContextHash()),
			tombstone: d.GetTombstone(),
			fields:    fields,
		}
	}
	return out, nil
}

// ownersOf returns the owners of a key's partition, in HRW rank order, mapped to
// the cluster nodes. Read from a currently-connected reference node.
func (h *harness) ownersOf(ctx context.Context, key string) ([]*node, error) {
	part := xxhash.Sum64String(key) % h.p
	ref := h.refNode()
	if ref == nil {
		return nil, errors.New("no connected node to read the owners table")
	}
	resp, err := h.inspect(ctx, ref)
	if err != nil {
		return nil, err
	}
	for _, po := range resp.GetPartitionsTable() {
		if uint64(po.GetPartition()) != part {
			continue
		}
		// Owners are determined by HRW membership; take the first RF non-dead
		// entries in rank order. Their gossiped status flag may briefly lag
		// activation right after a view change, so it is NOT used here.
		var owners []*node
		for _, ow := range po.GetOwners() {
			if ow.GetDead() { // skip dead-within-grace phantoms
				continue
			}
			var id [16]byte
			copy(id[:], ow.GetId())
			if nd := h.byID[id]; nd != nil {
				owners = append(owners, nd)
			}
			if len(owners) == 3 { // RF: the three replicas, HRW order
				break
			}
		}
		return owners, nil
	}
	return nil, fmt.Errorf("partition %d not in owners table", part)
}

// --- wait conditions ---------------------------------------------------------

func (h *harness) clusterFormed(ctx context.Context) bool {
	for _, n := range h.nodes {
		resp, err := h.inspect(ctx, n)
		if err != nil {
			return false
		}
		alive := 0
		for _, m := range resp.GetMembers() {
			if !m.GetDead() {
				alive++
			}
		}
		if alive != len(h.nodes) {
			return false
		}
	}
	return true
}

// clusterReady reports whether every partition's applier (the first non-dead
// owner in HRW order) is serving from the reference node's view — i.e. status
// has propagated and writes will route cleanly.
func (h *harness) clusterReady(ctx context.Context) bool {
	ref := h.refNode()
	if ref == nil {
		return false
	}
	resp, err := h.inspect(ctx, ref)
	if err != nil {
		return false
	}
	for _, po := range resp.GetPartitionsTable() {
		serving := false
		for _, ow := range po.GetOwners() {
			if ow.GetDead() {
				continue
			}
			serving = ow.GetStatus() == 2 || ow.GetStatus() == 3 // active or draining
			break
		}
		if !serving {
			return false
		}
	}
	return true
}

// clusterStable means membership re-formed AND appliers are active again.
func (h *harness) clusterStable(ctx context.Context) bool {
	return h.clusterFormed(ctx) && h.clusterReady(ctx)
}

func (h *harness) waitSeenDead(ctx context.Context, observer, target *node) bool {
	return waitUntil(40*time.Second, 1*time.Second, func() bool {
		resp, err := h.inspect(ctx, observer)
		if err != nil {
			return false
		}
		for _, m := range resp.GetMembers() {
			if bytes.Equal(m.GetId(), target.nodeID[:]) {
				return m.GetDead() // present but flagged dead-within-grace
			}
		}
		return true // dropped from the view entirely
	})
}

func (h *harness) waitConverged(ctx context.Context, owners []*node, key string) bool {
	return waitUntil(60*time.Second, 2*time.Second, func() bool {
		var base *docEntry
		for _, o := range owners {
			dump, err := h.dump(ctx, o)
			if err != nil {
				return false
			}
			e := dump[key]
			if !e.found {
				return false
			}
			if base == nil {
				cp := e
				base = &cp
				continue
			}
			if e.ctxHash != base.ctxHash || e.doc != base.doc {
				return false
			}
		}
		return base != nil
	})
}

// --- docker network control --------------------------------------------------

func (h *harness) disconnect(ctx context.Context, n *node) {
	if !n.connected {
		return
	}
	if _, err := docker(ctx, "network", "disconnect", clusterNet, n.id); err != nil {
		fmt.Printf("  warn: disconnect %s: %v\n", n.name, err)
		return
	}
	n.connected = false
}

func (h *harness) reconnect(ctx context.Context, n *node) {
	if n.connected {
		return
	}
	// Restore the original IP: the node's advertised gossip/RPC address was
	// pinned to it at boot.
	if _, err := docker(ctx, "network", "connect", "--ip", n.ip, clusterNet, n.id); err != nil {
		fmt.Printf("  warn: reconnect %s: %v\n", n.name, err)
		return
	}
	n.connected = true
}

func (h *harness) disconnectAll(ctx context.Context) {
	for _, n := range h.nodes {
		h.disconnect(ctx, n)
	}
}

func (h *harness) reconnectAll(ctx context.Context) {
	for _, n := range h.nodes {
		h.reconnect(ctx, n)
	}
}

func (h *harness) refNode() *node {
	for _, n := range h.nodes {
		if n.connected {
			return n
		}
	}
	return nil
}

// --- small utilities ---------------------------------------------------------

func composeUp(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "docker", "compose", "-p", project, "-f", composeFile,
		"up", "--build", "-d")
	cmd.Stdout = os.Stderr // surface build/start progress
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// docker runs a docker command and returns trimmed stdout.
func docker(ctx context.Context, args ...string) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(cctx, "docker", args...)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("docker %s: %v: %s",
			strings.Join(args, " "), err, strings.TrimSpace(errb.String()))
	}
	return strings.TrimSpace(out.String()), nil
}

func retry(fn func() error) error {
	var err error
	for i := 0; i < 6; i++ {
		if err = fn(); err == nil {
			return nil
		}
		time.Sleep(time.Second)
	}
	return err
}

func waitUntil(timeout, poll time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(poll)
	}
}

// wantFields asserts the document holds exactly the expected field->value set.
func wantFields(d docEntry, want map[string]string) error {
	if len(d.fields) != len(want) {
		return fmt.Errorf("have fields %v, want %v", d.fields, want)
	}
	for k, v := range want {
		got, ok := d.fields[k]
		if !ok {
			return fmt.Errorf("missing field %q", k)
		}
		if got != v {
			return fmt.Errorf("field %q = %s, want %s", k, got, v)
		}
	}
	return nil
}

func names(ns []*node) string {
	s := make([]string, len(ns))
	for i, n := range ns {
		s[i] = n.name
	}
	return strings.Join(s, ", ")
}

func short(h string) string {
	if len(h) > 8 {
		return h[:8]
	}
	return h
}

func step(format string, args ...any) {
	fmt.Printf("  • "+format+"\n", args...)
}
