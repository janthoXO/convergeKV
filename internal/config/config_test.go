package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Partitions != 256 {
		t.Fatalf("default partitions = %d, want 256", cfg.Partitions)
	}
}

func TestLoadFileAndEnvPrecedence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := "data_dir: /tmp/from-file\npartitions: 128\nseeds: [a:7946, b:7946]\nanti_entropy_interval: 30s\n"
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CONVERGEKV_DATA_DIR", "/tmp/from-env")
	t.Setenv("CONVERGEKV_SEEDS", "c:7946, d:7946,")

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DataDir != "/tmp/from-env" {
		t.Fatalf("env should override file: got %q", cfg.DataDir)
	}
	if cfg.Partitions != 128 {
		t.Fatalf("partitions from file = %d, want 128", cfg.Partitions)
	}
	if cfg.AntiEntropyInterval != 30*time.Second {
		t.Fatalf("anti_entropy_interval = %v, want 30s", cfg.AntiEntropyInterval)
	}
	if len(cfg.Seeds) != 2 || cfg.Seeds[0] != "c:7946" || cfg.Seeds[1] != "d:7946" {
		t.Fatalf("seeds from env = %v", cfg.Seeds)
	}
}

func TestValidateRejectsNonPowerOfTwoPartitions(t *testing.T) {
	t.Setenv("CONVERGEKV_PARTITIONS", "100")
	if _, err := Load(""); err == nil {
		t.Fatal("expected error for non-power-of-two partition count")
	}
}
