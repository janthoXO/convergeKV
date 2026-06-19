package clustertest

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	pb "github.com/janthoXO/convergeKV/pkg/proto"
)

func TestWriteViaNonOwnerVisibleOnAllOwners(t *testing.T) {
	h := Start(t, 5)
	ctx := context.Background()

	const key = "non-owner-write"
	client := h.Client(h.NonOwner(key))

	if _, err := client.Put(ctx, &pb.PutRequest{
		Key: key, Value: []byte(`{"a": 1, "b": {"nested": true}}`),
	}); err != nil {
		t.Fatalf("put via non-owner: %v", err)
	}
	h.WaitOwnersConverged(key, time.Second)

	got, err := client.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil || !got.GetFound() {
		t.Fatalf("get: %v found=%v", err, got.GetFound())
	}
	var doc map[string]any
	if err := json.Unmarshal(got.GetValue(), &doc); err != nil {
		t.Fatal(err)
	}
	if doc["a"] != float64(1) {
		t.Fatalf("unexpected doc: %s", got.GetValue())
	}
}

func TestWriteSucceedsWithTwoOfThreeOwnersDown(t *testing.T) {
	h := Start(t, 3) // 3 nodes: every node owns every partition
	ctx := context.Background()

	h.Kill(1)
	h.Kill(2)
	h.WaitConverged(1)

	client := h.Client(0)
	if _, err := client.Put(ctx, &pb.PutRequest{
		Key: "lonely", Value: []byte(`{"v": "still works"}`),
	}); err != nil {
		t.Fatalf("put with 2/3 owners down: %v", err)
	}
	got, err := client.Get(ctx, &pb.GetRequest{Key: "lonely"})
	if err != nil || !got.GetFound() {
		t.Fatalf("get: %v found=%v", err, got.GetFound())
	}
}

// Patch is the additive op: setting different fields never removes the others,
// so two clients patching distinct fields both survive (Put would replace).
func TestConcurrentDifferentFieldsBothSurvive(t *testing.T) {
	h := Start(t, 4)
	ctx := context.Background()

	const key = "two-fields"
	c1, c2 := h.Client(0), h.Client(1)
	var wg sync.WaitGroup
	var err1, err2 error
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err1 = c1.Patch(ctx, &pb.PatchRequest{Key: key, Value: []byte(`{"a": "from-c1"}`)})
	}()
	go func() {
		defer wg.Done()
		_, err2 = c2.Patch(ctx, &pb.PatchRequest{Key: key, Value: []byte(`{"b": "from-c2"}`)})
	}()
	wg.Wait()
	if err1 != nil || err2 != nil {
		t.Fatalf("patches failed: %v / %v", err1, err2)
	}
	h.WaitOwnersConverged(key, time.Second)

	got, err := h.Client(2).Get(ctx, &pb.GetRequest{Key: key})
	if err != nil || !got.GetFound() {
		t.Fatalf("get: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(got.GetValue(), &doc); err != nil {
		t.Fatal(err)
	}
	if doc["a"] != "from-c1" || doc["b"] != "from-c2" {
		t.Fatalf("concurrent fields lost: %s", got.GetValue())
	}
}

// Put replaces: fields omitted from a later Put are removed.
func TestPutReplacesOmittedFields(t *testing.T) {
	h := Start(t, 4)
	ctx := context.Background()

	const key = "replace-me"
	client := h.Client(0)
	if _, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: []byte(`{"a": 1, "b": 2}`)}); err != nil {
		t.Fatal(err)
	}
	h.WaitOwnersConverged(key, time.Second)
	if _, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: []byte(`{"a": 9}`)}); err != nil {
		t.Fatal(err)
	}
	h.WaitOwnersConverged(key, time.Second)

	got, err := h.Client(1).Get(ctx, &pb.GetRequest{Key: key})
	if err != nil || !got.GetFound() {
		t.Fatalf("get: %v found=%v", err, got.GetFound())
	}
	var doc map[string]any
	if err := json.Unmarshal(got.GetValue(), &doc); err != nil {
		t.Fatal(err)
	}
	if doc["a"] != float64(9) {
		t.Fatalf("a not replaced: %s", got.GetValue())
	}
	if _, ok := doc["b"]; ok {
		t.Fatalf("omitted field b not removed by Put: %s", got.GetValue())
	}
}

// Patch removes only the named fields and keeps the rest; it may set and delete
// in one call.
func TestPatchSetsAndDeletes(t *testing.T) {
	h := Start(t, 4)
	ctx := context.Background()

	const key = "patch-me"
	client := h.Client(0)
	if _, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: []byte(`{"a": 1, "b": 2, "c": 3}`)}); err != nil {
		t.Fatal(err)
	}
	h.WaitOwnersConverged(key, time.Second)
	// Set d, drop b — a and c are untouched.
	if _, err := client.Patch(ctx, &pb.PatchRequest{
		Key: key, Value: []byte(`{"d": 4}`), DeleteFields: []string{"b"},
	}); err != nil {
		t.Fatal(err)
	}
	h.WaitOwnersConverged(key, time.Second)

	got, err := h.Client(1).Get(ctx, &pb.GetRequest{Key: key})
	if err != nil || !got.GetFound() {
		t.Fatalf("get: %v found=%v", err, got.GetFound())
	}
	var doc map[string]any
	if err := json.Unmarshal(got.GetValue(), &doc); err != nil {
		t.Fatal(err)
	}
	if doc["a"] != float64(1) || doc["c"] != float64(3) || doc["d"] != float64(4) {
		t.Fatalf("patch lost a/c or did not set d: %s", got.GetValue())
	}
	if _, ok := doc["b"]; ok {
		t.Fatalf("named delete b not removed by Patch: %s", got.GetValue())
	}
}

func TestConcurrentSameFieldConvergesToOneWinner(t *testing.T) {
	h := Start(t, 4)
	ctx := context.Background()

	const key = "lww-field"
	c1, c2 := h.Client(0), h.Client(1)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = c1.Put(ctx, &pb.PutRequest{Key: key, Value: []byte(`{"x": {"version": 1, "only": "c1"}}`)})
	}()
	go func() {
		defer wg.Done()
		_, _ = c2.Put(ctx, &pb.PutRequest{Key: key, Value: []byte(`{"x": {"version": 2, "only": "c2"}}`)})
	}()
	wg.Wait()
	// WaitOwnersConverged asserts byte-identical state (incl. context) on
	// all owners — that IS the LWW determinism check across replicas.
	h.WaitOwnersConverged(key, time.Second)

	got, err := h.Client(3).Get(ctx, &pb.GetRequest{Key: key})
	if err != nil || !got.GetFound() {
		t.Fatalf("get: %v", err)
	}
	var doc map[string]map[string]any
	if err := json.Unmarshal(got.GetValue(), &doc); err != nil {
		t.Fatal(err)
	}
	x := doc["x"]
	// Exactly one side's WHOLE nested object: version and owner must match.
	switch x["only"] {
	case "c1":
		if x["version"] != float64(1) {
			t.Fatalf("mixed nested object: %v", x)
		}
	case "c2":
		if x["version"] != float64(2) {
			t.Fatalf("mixed nested object: %v", x)
		}
	default:
		t.Fatalf("unexpected winner: %v", x)
	}
}

func TestDeleteVisibleEverywhere(t *testing.T) {
	h := Start(t, 4)
	ctx := context.Background()

	const key = "to-delete"
	client := h.Client(0)
	if _, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: []byte(`{"v": 1}`)}); err != nil {
		t.Fatal(err)
	}
	h.WaitOwnersConverged(key, time.Second)
	if _, err := client.Delete(ctx, &pb.DeleteRequest{Key: key}); err != nil {
		t.Fatal(err)
	}
	h.WaitOwnersConverged(key, time.Second)

	for i := range h.Nodes {
		got, err := h.Client(i).Get(ctx, &pb.GetRequest{Key: key})
		if err != nil {
			t.Fatal(err)
		}
		if got.GetFound() {
			t.Fatalf("node %d still finds deleted key", i)
		}
	}
}

func TestInvalidDocumentRejected(t *testing.T) {
	h := Start(t, 3)
	ctx := context.Background()
	for _, bad := range []string{`[1,2]`, `"scalar"`, `null`, `not json`} {
		if _, err := h.Client(0).Put(ctx, &pb.PutRequest{Key: "k", Value: []byte(bad)}); err == nil {
			t.Fatalf("document %q must be rejected", bad)
		}
	}
}
