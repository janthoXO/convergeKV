// Package keyspace maps string keys to virtual partition IDs and encodes the
// (partitionID, key, field) triple into the storage key format used by BadgerDB.
//
// Storage key encoding: partitionID(4B big-endian) + key + "\x00" + field.
// NUM_PARTITIONS must be identical on every cluster node.
package keyspace

import (
	"encoding/binary"
	"errors"
	"strings"

	"github.com/cespare/xxhash/v2"
)

// ErrNullByte is returned by RejectNullBytes when a key contains a null byte.
var ErrNullByte = errors.New("key must not contain null bytes")

// Of returns the partition ID for key in a cluster configured with numPartitions
// total partitions. Result is in [0, numPartitions). numPartitions must be > 0.
func Of(key string, numPartitions int) uint32 {
	return uint32(xxhash.Sum64String(key) % uint64(numPartitions))
}

// EncodeKey encodes (partitionID, key, field) into a BadgerDB storage key:
// 4B big-endian partition prefix + key + "\x00" + field.
func EncodeKey(partitionId uint32, key string, field string) []byte {
	kb := []byte(key)
	fb := []byte(field)
	buf := make([]byte, 4+len(kb)+1+len(fb))
	binary.BigEndian.PutUint32(buf, partitionId)
	copy(buf[4:], kb)
	buf[4+len(kb)] = 0x00
	copy(buf[4+len(kb)+1:], fb)
	return buf
}

// DecodeKey splits a storage key back into (partitionID, key, field).
// The first 4 bytes are the big-endian partition ID; the remainder is split on
// the first "\x00" to separate the key from the field.
func DecodeKey(b []byte) (partitionId uint32, key string, field string, err error) {
	if len(b) < 4 {
		return 0, string(b), "", errors.New("keyspace: storage key too short")
	}
	partitionId = binary.BigEndian.Uint32(b[:4])
	rest := string(b[4:])
	parts := strings.SplitN(rest, "\x00", 2)
	if len(parts) != 2 {
		return partitionId, rest, "", nil
	}
	return partitionId, parts[0], parts[1], nil
}

// PartitionPrefix returns the 4-byte prefix for scanning all entries in partitionId.
func PartitionPrefix(partitionId uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, partitionId)
	return b
}

// CheckpointKey returns the reserved storage key for the HLC checkpoint
// (see RecoverHLCFloor). It is the 4-byte partition prefix 0xFFFFFFFF, which
// can never collide with a real partition's range since NUM_PARTITIONS is
// always far smaller than 2^32.
func CheckpointKey() []byte {
	return []byte{0xFF, 0xFF, 0xFF, 0xFF}
}

// KeyPrefix returns the prefix for scanning all fields of a single key:
// 4B partition prefix + key + "\x00".
func KeyPrefix(partitionId uint32, key string) []byte {
	kb := []byte(key)
	buf := make([]byte, 4+len(kb)+1)
	binary.BigEndian.PutUint32(buf, partitionId)
	copy(buf[4:], kb)
	buf[4+len(kb)] = 0x00
	return buf
}

// RejectNullBytes returns ErrNullByte if key contains a null byte.
// Null bytes corrupt the key/field separator in the storage encoding.
func RejectNullBytes(key string) error {
	if strings.ContainsRune(key, 0) {
		return ErrNullByte
	}
	return nil
}
