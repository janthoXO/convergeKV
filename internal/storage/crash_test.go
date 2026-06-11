package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/janthoXO/convergeKV/internal/crdt"
)

const crashDirEnv = "CONVERGEKV_CRASH_HELPER_DIR"

var crashActor = crdt.ActorID{0xCC}

// TestCrashRestart spawns a child process that hammers the store with synced
// writes, SIGKILLs it mid-storm, reopens the store, and verifies the two M2
// acceptance properties: no torn documents (every stored doc decodes — done
// implicitly by ScanPartition) and no dot reuse (the persisted dot-seq
// checkpoint covers every dot found in any stored document, because doc and
// checkpoint commit in one atomic batch).
func TestCrashRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("crash test spawns child processes")
	}
	dir := t.TempDir()

	for round := 0; round < 3; round++ {
		cmd := exec.Command(os.Args[0], "-test.run=TestCrashWriterHelper", "-test.v")
		cmd.Env = append(os.Environ(), crashDirEnv+"="+dir)
		out, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		// Wait until the helper reports it is writing, then let the storm
		// run briefly and kill -9.
		ready := make(chan struct{})
		go func() {
			buf := make([]byte, 1)
			for {
				n, err := out.Read(buf)
				if err != nil {
					return
				}
				if n == 1 && buf[0] == '!' {
					close(ready)
					return
				}
			}
		}()
		select {
		case <-ready:
		case <-time.After(30 * time.Second):
			_ = cmd.Process.Kill()
			t.Fatal("helper never started writing")
		}
		time.Sleep(time.Duration(50+round*30) * time.Millisecond)
		if err := cmd.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		_ = cmd.Wait()

		verifyAfterCrash(t, dir, round)
	}
}

func verifyAfterCrash(t *testing.T, dir string, round int) {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("round %d: reopen after kill -9: %v", round, err)
	}
	defer func() { _ = s.Close() }()

	checkpoint, err := s.DotSeq()
	if err != nil {
		t.Fatal(err)
	}
	var maxSeen uint64
	docs := 0
	for pid := uint16(0); pid < 8; pid++ {
		err := s.ScanPartition(pid, func(key []byte, doc *crdt.Document) error {
			docs++ // decoding already proved the doc is not torn
			if seq := doc.Context.VV[crashActor]; seq > maxSeen {
				maxSeen = seq
			}
			for d := range doc.Context.Cloud {
				if d.Actor == crashActor && d.Seq > maxSeen {
					maxSeen = d.Seq
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("round %d: torn or corrupt document: %v", round, err)
		}
	}
	if docs == 0 {
		t.Fatalf("round %d: helper wrote nothing before being killed", round)
	}
	if maxSeen > checkpoint {
		t.Fatalf("round %d: dot reuse possible: doc carries seq %d > checkpoint %d",
			round, maxSeen, checkpoint)
	}
	t.Logf("round %d: %d docs intact, checkpoint %d >= max dot %d", round, docs, checkpoint, maxSeen)
}

// TestCrashWriterHelper is the child process body; it only runs when the
// crash-dir env var is set, and never exits on its own.
func TestCrashWriterHelper(t *testing.T) {
	dir := os.Getenv(crashDirEnv)
	if dir == "" {
		t.Skip("helper process only")
	}
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	seq, err := s.DotSeq()
	if err != nil {
		t.Fatal(err)
	}
	minter := &crdt.Minter{Actor: crashActor, Seq: seq}

	fmt.Print("!") // signal: storm starting
	for i := 0; ; i++ {
		pid := uint16(i % 8)
		key := binary.BigEndian.AppendUint32(nil, uint32(i%64))
		doc, err := s.GetDocument(pid, key)
		if err != nil {
			t.Fatal(err)
		}
		if doc == nil {
			doc = crdt.NewDocument()
		}
		doc.Put("n", binary.BigEndian.AppendUint64(nil, uint64(i)), minter.Next(), uint64(i)<<16)
		b := s.NewBatch()
		b.SetDocument(pid, key, doc)
		b.SetDotSeq(minter.Seq)
		if err := s.Commit(b); err != nil {
			t.Fatal(err)
		}
	}
}
