// Package checksum provides CRC32C integrity checks for persistent content.
package checksum

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

const Magic = "PXCHK\x00\x01\n"
const FooterBytes = 16

var table = crc32.MakeTable(crc32.Castagnoli)
var ErrCorrupt = errors.New("checksum: corrupt content")

func Sum(data []byte) uint32 { return crc32.Checksum(data, table) }

type countingWriter struct {
	writer io.Writer
	count  uint64
}

func (w *countingWriter) Write(data []byte) (int, error) {
	n, err := w.writer.Write(data)
	w.count += uint64(n)
	return n, err
}

// Write streams the envelope without buffering the payload. The checksum covers
// the magic and payload; a second checksum covers length and checksum metadata.
func Write(writer io.Writer, encode func(io.Writer) error) error {
	hash := crc32.New(table)
	body := &countingWriter{writer: io.MultiWriter(writer, hash)}
	if _, err := body.Write([]byte(Magic)); err != nil {
		return err
	}
	if err := encode(body); err != nil {
		return err
	}
	footer := make([]byte, FooterBytes)
	binary.LittleEndian.PutUint64(footer, body.count)
	binary.LittleEndian.PutUint32(footer[8:], hash.Sum32())
	binary.LittleEndian.PutUint32(footer[12:], Sum(footer[:12]))
	n, err := writer.Write(footer)
	if err != nil {
		return err
	}
	if n != len(footer) {
		return io.ErrShortWrite
	}
	return nil
}

// Decode verifies the entire envelope before exposing any payload to a decoder.
// Truncation, extra bytes and metadata corruption are all rejected.
func Decode(data []byte) ([]byte, error) {
	if len(data) < len(Magic)+FooterBytes {
		return nil, fmt.Errorf("%w: truncated envelope", ErrCorrupt)
	}
	if !bytes.HasPrefix(data, []byte(Magic)) {
		return nil, fmt.Errorf("%w: invalid envelope magic", ErrCorrupt)
	}
	body, footer := data[:len(data)-FooterBytes], data[len(data)-FooterBytes:]
	if Sum(footer[:12]) != binary.LittleEndian.Uint32(footer[12:]) {
		return nil, fmt.Errorf("%w: footer checksum mismatch", ErrCorrupt)
	}
	if binary.LittleEndian.Uint64(footer) != uint64(len(body)) {
		return nil, fmt.Errorf("%w: payload length mismatch", ErrCorrupt)
	}
	if Sum(body) != binary.LittleEndian.Uint32(footer[8:]) {
		return nil, fmt.Errorf("%w: payload checksum mismatch", ErrCorrupt)
	}
	return body[len(Magic):], nil
}
