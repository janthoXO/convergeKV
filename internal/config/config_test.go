package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Partitions != 256 {
		t.Fatalf("default partitions = %d, want 256", cfg.Partitions)
	}
	if cfg.CrashGracePeriod != 10*time.Minute {
		t.Fatalf("default crash grace = %v", cfg.CrashGracePeriod)
	}
}

func TestLoadFromEnvironment(t *testing.T) {
	t.Setenv("CONVERGEKV_DATA_DIR", "/tmp/from-env")
	t.Setenv("CONVERGEKV_PARTITIONS", "128")
	t.Setenv("CONVERGEKV_SEEDS", "c:7946,d:7946")
	t.Setenv("CONVERGEKV_ANTI_ENTROPY_INTERVAL", "30s")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DataDir != "/tmp/from-env" {
		t.Fatalf("DataDir = %q", cfg.DataDir)
	}
	if cfg.Partitions != 128 {
		t.Fatalf("Partitions = %d, want 128", cfg.Partitions)
	}
	if cfg.AntiEntropyInterval != 30*time.Second {
		t.Fatalf("AntiEntropyInterval = %v, want 30s", cfg.AntiEntropyInterval)
	}
	if len(cfg.Seeds) != 2 || cfg.Seeds[0] != "c:7946" || cfg.Seeds[1] != "d:7946" {
		t.Fatalf("Seeds = %v", cfg.Seeds)
	}
	// Untouched fields keep their defaults.
	if cfg.ClientAddr != ":7000" {
		t.Fatalf("ClientAddr default lost: %q", cfg.ClientAddr)
	}
}

func TestValidateRejectsNonPowerOfTwoPartitions(t *testing.T) {
	t.Setenv("CONVERGEKV_PARTITIONS", "100")
	if _, err := Load(); err == nil {
		t.Fatal("expected error for non-power-of-two partition count")
	}
}

func TestValidateRejectsBadDuration(t *testing.T) {
	t.Setenv("CONVERGEKV_CRASH_GRACE_PERIOD", "not-a-duration")
	if _, err := Load(); err == nil {
		t.Fatal("expected error for malformed duration")
	}
}
