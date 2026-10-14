package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func records(t *testing.T, path string) ([]string, error) {
	t.Helper()
	out := []string{}
	_, err := Replay(path, func(payload []byte) error { out = append(out, string(payload)); return nil })
	return out, err
}
func TestAppendReplayAndReset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	for _, s := range []string{"promise", "accept", "decide"} {
		if _, err := Append(path, []byte(s)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := records(t, path)
	if err != nil || !reflect.DeepEqual(got, []string{"promise", "accept", "decide"}) {
		t.Fatalf("replay %v %v", got, err)
	}
	if err := Reset(path); err != nil {
		t.Fatal(err)
	}
	if _, err := Append(path, []byte("next")); err != nil {
		t.Fatal(err)
	}
	got, err = records(t, path)
	if err != nil || !reflect.DeepEqual(got, []string{"next"}) {
		t.Fatalf("reset replay %v %v", got, err)
	}
}
func TestEveryIncompleteTailPreservesCompletePrefix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source.wal")
	Append(path, []byte("durable"))
	prefix, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	Append(path, []byte("unfinished"))
	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for cut := len(prefix) + 1; cut < len(full); cut++ {
		target := filepath.Join(t.TempDir(), "cut.wal")
		if err := os.WriteFile(target, full[:cut], 0600); err != nil {
			t.Fatal(err)
		}
		got, err := records(t, target)
		if err != nil || !reflect.DeepEqual(got, []string{"durable"}) {
			t.Fatalf("cut %d: %v %v", cut, got, err)
		}
		repaired, err := os.ReadFile(target)
		if err != nil || !bytes.Equal(repaired, prefix) {
			t.Fatalf("cut %d not repaired", cut)
		}
		if _, err := Append(target, []byte("after")); err != nil {
			t.Fatal(err)
		}
		got, err = records(t, target)
		if err != nil || !reflect.DeepEqual(got, []string{"durable", "after"}) {
			t.Fatalf("append after repair %v %v", got, err)
		}
	}
}
func TestCompleteCorruptionFailsWithoutTruncation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source.wal")
	Append(path, []byte("durable"))
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, index := range []int{0, len(magic), len(magic) + 4, len(magic) + 8, len(good) - 1} {
		damaged := bytes.Clone(good)
		damaged[index] ^= 1
		target := filepath.Join(t.TempDir(), "bad.wal")
		os.WriteFile(target, damaged, 0600)
		if _, err := records(t, target); err == nil {
			t.Fatalf("corruption at %d accepted", index)
		}
		after, _ := os.ReadFile(target)
		if !bytes.Equal(after, damaged) {
			t.Fatal("corrupt complete record was discarded")
		}
	}
}

func TestNewNestedDirectoryAndTornMagic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "new", "nested", "node.wal")
	if _, err := Append(path, []byte("first")); err != nil {
		t.Fatal(err)
	}
	got, err := records(t, path)
	if err != nil || !reflect.DeepEqual(got, []string{"first"}) {
		t.Fatalf("new directory WAL: %v", err)
	}
	for cut := 1; cut < len(magic); cut++ {
		target := filepath.Join(t.TempDir(), "partial.wal")
		if err := os.WriteFile(target, magic[:cut], 0600); err != nil {
			t.Fatal(err)
		}
		got, err := records(t, target)
		if err != nil || len(got) != 0 {
			t.Fatalf("torn magic %d: %v", cut, err)
		}
		if _, err := Append(target, []byte("after")); err != nil {
			t.Fatal(err)
		}
		got, err = records(t, target)
		if err != nil || !reflect.DeepEqual(got, []string{"after"}) {
			t.Fatalf("append after torn magic %d: %v", cut, err)
		}
	}
}
