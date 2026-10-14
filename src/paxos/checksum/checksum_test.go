package checksum

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func envelope(t *testing.T, payload []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	if err := Write(&out, func(writer io.Writer) error { _, err := writer.Write(payload); return err }); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}
func TestEnvelopeRejectsEverySingleBitChange(t *testing.T) {
	payload := []byte("state, dedup records, ballots and slot metadata")
	good := envelope(t, payload)
	got, err := Decode(good)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("round trip: %v", err)
	}
	for i := range good {
		for bit := uint(0); bit < 8; bit++ {
			damaged := bytes.Clone(good)
			damaged[i] ^= 1 << bit
			if _, err := Decode(damaged); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("byte %d bit %d corruption accepted: %v", i, bit, err)
			}
		}
	}
}
func TestEnvelopeRejectsTruncationAndTrailingBytes(t *testing.T) {
	good := envelope(t, []byte("durable state"))
	for cut := 0; cut < len(good); cut++ {
		if _, err := Decode(good[:cut]); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("truncation %d accepted", cut)
		}
	}
	for _, tail := range [][]byte{{0}, []byte("garbage"), good} {
		if _, err := Decode(append(bytes.Clone(good), tail...)); !errors.Is(err, ErrCorrupt) {
			t.Fatal("trailing bytes accepted")
		}
	}
}

type shortWriter struct{}

func (shortWriter) Write(data []byte) (int, error) { return len(data) - 1, nil }
func TestEnvelopeWritePropagatesFailures(t *testing.T) {
	failure := errors.New("encode failed")
	var out bytes.Buffer
	if err := Write(&out, func(writer io.Writer) error { writer.Write([]byte("partial")); return failure }); !errors.Is(err, failure) {
		t.Fatalf("encode failure: %v", err)
	}
	if _, err := Decode(out.Bytes()); err == nil {
		t.Fatal("incomplete write accepted")
	}
	if err := Write(shortWriter{}, func(io.Writer) error { return nil }); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write: %v", err)
	}
}
