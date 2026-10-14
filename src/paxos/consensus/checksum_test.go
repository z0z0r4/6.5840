package consensus

import (
	"bytes"
	"encoding/gob"
	"errors"
	"os"
	"strings"
	"testing"

	"6.5840/paxos/checksum"
	"6.5840/paxos/kv"
)

type restoreTracker struct {
	*kv.StateMachine
	restores int
}

func (m *restoreTracker) Restore(image string) error {
	m.restores++
	return m.StateMachine.Restore(image)
}

func TestSnapshotChecksumCheckedBeforeAnyRestore(t *testing.T) {
	s := newProtocolServer(t, []string{"p0"})
	decideOperation(t, s, 1, command(kv.Operation{ReqID: "write", Action: "set", Key: "key", Value: "checksum-marker"}))
	if err := s.writeSnapshotLocked(s.walSequence); err != nil {
		t.Fatal(err)
	}
	good, err := os.ReadFile(s.statePath)
	if err != nil {
		t.Fatal(err)
	}
	body, err := checksum.Decode(good)
	if err != nil {
		t.Fatal(err)
	}
	// A same-length value edit keeps the gob structurally valid, so decoding and
	// JSON validity alone could not detect this corruption.
	index := bytes.Index(good, []byte("checksum-marker"))
	if index < 0 {
		t.Fatal("test value missing from image")
	}
	for _, mode := range []string{"valid-gob-edit", "payload-crc", "footer-crc", "truncate", "append", "strip-envelope"} {
		t.Run(mode, func(t *testing.T) {
			damaged := bytes.Clone(good)
			switch mode {
			case "valid-gob-edit":
				damaged[index] ^= 1
				var snapshot Snapshot
				if err := gob.NewDecoder(bytes.NewReader(damaged[len(checksum.Magic) : len(damaged)-checksum.FooterBytes])).Decode(&snapshot); err != nil {
					t.Fatalf("edit unexpectedly damaged gob syntax: %v", err)
				}
			case "payload-crc":
				damaged[len(damaged)-8] ^= 1
			case "footer-crc":
				damaged[len(damaged)-1] ^= 1
			case "truncate":
				damaged = damaged[:len(damaged)-1]
			case "append":
				damaged = append(damaged, 0)
			case "strip-envelope":
				damaged = bytes.Clone(body)
			}
			if err := os.WriteFile(s.statePath, damaged, 0600); err != nil {
				t.Fatal(err)
			}
			tracker := &restoreTracker{StateMachine: kv.NewStateMachine(kv.New())}
			_, err := New(Config{ID: 0, Peers: s.peers, DataDir: tDir(s), Machine: tracker})
			if err == nil || tracker.restores != 0 {
				t.Fatalf("corrupt state reached restoration: err=%v restores=%d", err, tracker.restores)
			}
			if mode != "strip-envelope" && !errors.Is(err, checksum.ErrCorrupt) {
				t.Fatalf("checksum error not preserved: %v", err)
			}
			if mode == "strip-envelope" && !strings.Contains(err.Error(), "requires a checksum") {
				t.Fatalf("unchecked current format accepted: %v", err)
			}
			after, _ := os.ReadFile(s.statePath)
			if !bytes.Equal(after, damaged) {
				t.Fatal("failed recovery rewrote damaged image")
			}
		})
	}
}

func TestUncheckedV6SnapshotAndV1WALUpgradeTogether(t *testing.T) {
	s := newProtocolServer(t, []string{"p0"})
	base, err := os.ReadFile(s.statePath)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := checksum.Decode(base)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot Snapshot
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot.Version = 6
	var old bytes.Buffer
	if err := gob.NewEncoder(&old).Encode(snapshot); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.statePath, old.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	decideOperation(t, s, 1, command(kv.Operation{ReqID: "kept", Action: "set", Key: "key", Value: "value"}))
	journal, err := os.ReadFile(s.walPath())
	if err != nil {
		t.Fatal(err)
	}
	legacy := append([]byte{'P', 'X', 'W', 'A', 'L', 0, 1, '\n'}, journal[12:]...)
	if err := os.WriteFile(s.walPath(), legacy, 0600); err != nil {
		t.Fatal(err)
	}
	restored, err := newServer(0, s.peers, tDir(s))
	if err != nil {
		t.Fatal(err)
	}
	if restored.applied != 1 || values(restored)["key"] != "value" || restored.requests["kept"].Seq != 1 {
		t.Fatal("joint upgrade lost WAL state or dedup")
	}
	checked, err := os.ReadFile(s.statePath)
	if err != nil {
		t.Fatal(err)
	}
	payload, err = checksum.Decode(checked)
	if err != nil {
		t.Fatal(err)
	}
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&snapshot); err != nil || snapshot.Version != SnapshotVersion {
		t.Fatalf("snapshot upgrade: %v", err)
	}
	upgraded, _ := os.ReadFile(s.walPath())
	if len(upgraded) != len(legacy)+4 || upgraded[6] != 2 {
		t.Fatal("legacy WAL file header was not upgraded")
	}
	again, err := newServer(0, s.peers, tDir(s))
	if err != nil || again.applied != 1 {
		t.Fatalf("upgraded files did not restart: %v", err)
	}
}
