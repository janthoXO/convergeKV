// Package identity manages the node's permanent identity: a UUID generated
// once on first start and persisted to disk. It is never derived from the
// network address. The 16 raw bytes double as the CRDT ActorID.
package identity

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"
)

const fileName = "node_id"

type NodeID = uuid.UUID

// LoadOrCreate returns the node ID stored in dataDir, creating and persisting
// a fresh one (with an fsync'd atomic rename) on first start.
func LoadOrCreate(dataDir string) (NodeID, error) {
	path := filepath.Join(dataDir, fileName)

	b, err := os.ReadFile(path)
	switch {
	case err == nil:
		id, err := uuid.ParseBytes(b)
		if err != nil {
			return uuid.Nil, fmt.Errorf("corrupt node id file %s: %w", path, err)
		}
		return id, nil
	case !os.IsNotExist(err):
		return uuid.Nil, fmt.Errorf("read node id: %w", err)
	}

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return uuid.Nil, fmt.Errorf("create data dir: %w", err)
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return uuid.Nil, fmt.Errorf("generate node id: %w", err)
	}
	if err := writeAtomic(path, []byte(id.String())); err != nil {
		return uuid.Nil, fmt.Errorf("persist node id: %w", err)
	}
	return id, nil
}

func writeAtomic(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), fileName+".tmp*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
