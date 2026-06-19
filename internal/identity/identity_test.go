package identity

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

func TestLoadOrCreateStableAcrossRestarts(t *testing.T) {
	dir := t.TempDir()

	first, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatalf("first LoadOrCreate: %v", err)
	}
	if first == uuid.Nil {
		t.Fatal("got nil UUID")
	}

	second, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatalf("second LoadOrCreate: %v", err)
	}
	if first != second {
		t.Fatalf("identity changed across restarts: %s != %s", first, second)
	}
}

func TestLoadOrCreateDistinctPerDataDir(t *testing.T) {
	a, err := LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b, err := LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("two data dirs produced the same identity")
	}
}

func TestLoadOrCreateCorruptFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, fileName), []byte("not-a-uuid"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrCreate(dir); err == nil {
		t.Fatal("expected error for corrupt node id file")
	}
}
