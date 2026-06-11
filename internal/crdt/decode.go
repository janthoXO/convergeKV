package crdt

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// DecodeDocument parses the canonical encoding produced by
// Document.AppendCanonical. The canonical format is also the storage format,
// so decoding must reject any truncated or corrupt input.
func DecodeDocument(b []byte) (*Document, error) {
	r := reader{buf: b}
	doc := NewDocument()

	nFields, err := r.uint32()
	if err != nil {
		return nil, err
	}
	for i := uint32(0); i < nFields; i++ {
		name, err := r.bytes32()
		if err != nil {
			return nil, err
		}
		nRegs, err := r.uint32()
		if err != nil {
			return nil, err
		}
		if nRegs == 0 {
			return nil, errors.New("crdt: empty register set")
		}
		regs := make([]Register, 0, nRegs)
		for j := uint32(0); j < nRegs; j++ {
			var reg Register
			if reg.Dot, err = r.dot(); err != nil {
				return nil, err
			}
			if reg.HLC, err = r.uint64(); err != nil {
				return nil, err
			}
			val, err := r.bytes32()
			if err != nil {
				return nil, err
			}
			reg.Value = val
			regs = append(regs, reg)
		}
		doc.Fields[string(name)] = regs
	}

	nVV, err := r.uint32()
	if err != nil {
		return nil, err
	}
	for i := uint32(0); i < nVV; i++ {
		d, err := r.dot()
		if err != nil {
			return nil, err
		}
		doc.Context.VV[d.Actor] = d.Seq
	}
	nCloud, err := r.uint32()
	if err != nil {
		return nil, err
	}
	for i := uint32(0); i < nCloud; i++ {
		d, err := r.dot()
		if err != nil {
			return nil, err
		}
		doc.Context.Cloud[d] = struct{}{}
	}
	if len(r.buf) != 0 {
		return nil, fmt.Errorf("crdt: %d trailing bytes", len(r.buf))
	}
	return doc, nil
}

type reader struct{ buf []byte }

var errTruncated = errors.New("crdt: truncated document encoding")

func (r *reader) take(n int) ([]byte, error) {
	if n < 0 || len(r.buf) < n {
		return nil, errTruncated
	}
	out := r.buf[:n]
	r.buf = r.buf[n:]
	return out, nil
}

func (r *reader) uint32() (uint32, error) {
	b, err := r.take(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

func (r *reader) uint64() (uint64, error) {
	b, err := r.take(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b), nil
}

// bytes32 reads a uint32 length prefix and copies that many bytes.
func (r *reader) bytes32() ([]byte, error) {
	n, err := r.uint32()
	if err != nil {
		return nil, err
	}
	b, err := r.take(int(n))
	if err != nil {
		return nil, err
	}
	out := make([]byte, n)
	copy(out, b)
	return out, nil
}

func (r *reader) dot() (Dot, error) {
	b, err := r.take(16)
	if err != nil {
		return Dot{}, err
	}
	var d Dot
	copy(d.Actor[:], b)
	d.Seq, err = r.uint64()
	return d, err
}
