package api

import (
	"io"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc/encoding"
)

// Zstd is the compressor name registered with gRPC; streams (anti-entropy,
// transfer) opt in via grpc.UseCompressor(Zstd).
const Zstd = "zstd"

func init() {
	encoding.RegisterCompressor(&zstdCompressor{})
}

type zstdCompressor struct{}

func (*zstdCompressor) Name() string { return Zstd }

func (c *zstdCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	return zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedFastest))
}

func (c *zstdCompressor) Decompress(r io.Reader) (io.Reader, error) {
	d, err := zstd.NewReader(r)
	if err != nil {
		return nil, err
	}
	return d.IOReadCloser(), nil
}
