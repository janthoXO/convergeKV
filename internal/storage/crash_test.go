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
// implicitly by ScanPartition) and no dot reuse. Dots are minted per document
// from its own persisted context (seq = Context.Next(self)), so the document
// itself is the checkpoint: after a crash, every surviving doc's context must
// be exactly the contiguous local-mint shape (no cloud dots for the writer,
// register seqs covered by the VV) — a torn doc/context pair is the only way
// reuse could happen, and the atomic batch forbids it.
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

	docs := 0
	for pid := uint16(0); pid < 8; pid++ {
		err := s.ScanPartition(pid, func(key []byte, doc *crdt.Document) error {
			docs++ // decoding already proved the doc is not torn
			// Local minting is contiguous per document: the writer's dots
			// must all have folded into the VV. A cloud dot for the writer
			// would mean the next mint (Context.Next) could collide.
			for d := range doc.Context.Cloud {
				if d.Actor == crashActor {
					return fmt.Errorf("doc %q: writer dot %d stuck in cloud", key, d.Seq)
				}
			}
			for _, regs := range doc.Fields {
				for _, r := range regs {
					if r.Dot.Actor == crashActor && r.Dot.Seq > doc.Context.VV[crashActor] {
						return fmt.Errorf("doc %q: register seq %d > VV %d",
							key, r.Dot.Seq, doc.Context.VV[crashActor])
					}
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
	t.Logf("round %d: %d docs intact with consistent contexts", round, docs)
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
		// Per-document minting, exactly like the coordinator's applier.
		dot := crdt.Dot{Actor: crashActor, Seq: doc.Context.Next(crashActor)}
		doc.Put("n", binary.BigEndian.AppendUint64(nil, uint64(i)), dot, uint64(i)<<16)
		b := s.NewBatch()
		b.SetDocument(pid, key, doc)
		if err := s.Commit(b); err != nil {
			t.Fatal(err)
		}
	}
}
