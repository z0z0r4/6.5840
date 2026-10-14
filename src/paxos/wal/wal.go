// Package wal provides durable append-only frames independently of Paxos or KV.
// Each path has a single writer. Callers serialize append, replay and reset.
package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"6.5840/paxos/checksum"
)

var magic = []byte{'P', 'X', 'W', 'A', 'L', 0, 2, '\n'}
var legacyMagic = []byte{'P', 'X', 'W', 'A', 'L', 0, 1, '\n'}

const headerSize = 12
const MaxRecordBytes = 256 << 20
const FileHeaderBytes = 12

// Append syncs a complete frame before returning. A failure poisons the writer:
// the caller must stop and recover the valid prefix before writing again.
func Append(path string, payload []byte) (int64, error) {
	if len(payload) == 0 || len(payload) > MaxRecordBytes {
		return 0, fmt.Errorf("WAL record size %d out of bounds", len(payload))
	}
	if err := EnsureDir(filepath.Dir(path)); err != nil {
		return 0, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}
	var frame bytes.Buffer
	if stat.Size() == 0 {
		frame.Write(fileHeader())
	}
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header, uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:], checksum.Sum(payload))
	binary.LittleEndian.PutUint32(header[8:], checksum.Sum(header[:8]))
	frame.Write(header)
	frame.Write(payload)
	n, err := file.Write(frame.Bytes())
	if err != nil {
		return 0, err
	}
	if n != frame.Len() {
		return 0, io.ErrShortWrite
	}
	if err := file.Sync(); err != nil {
		return 0, err
	}
	if err := file.Close(); err != nil {
		return 0, err
	}
	if stat.Size() == 0 {
		if err := syncDir(filepath.Dir(path)); err != nil {
			return 0, err
		}
	}
	return int64(n), nil
}

// Replay visits only complete, verified frames. An incomplete trailing write is
// truncated and synced; checksum errors in complete frames fail recovery.
func Replay(path string, visit func([]byte) error) (int64, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0600)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer file.Close()
	prefix := make([]byte, len(magic))
	n, err := io.ReadFull(file, prefix)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		if !bytes.Equal(prefix[:n], magic[:n]) && !bytes.Equal(prefix[:n], legacyMagic[:n]) {
			return 0, fmt.Errorf("%w: invalid WAL magic", checksum.ErrCorrupt)
		}
		return repairTail(file, 0)
	}
	if err != nil {
		return 0, err
	}
	legacy := bytes.Equal(prefix, legacyMagic)
	offset := int64(len(magic))
	if bytes.Equal(prefix, magic) {
		digest := make([]byte, 4)
		if _, err := io.ReadFull(file, digest); err == io.EOF || err == io.ErrUnexpectedEOF {
			return repairTail(file, 0)
		} else if err != nil {
			return 0, err
		}
		if binary.LittleEndian.Uint32(digest) != checksum.Sum(prefix) {
			return 0, fmt.Errorf("%w: WAL file header checksum mismatch", checksum.ErrCorrupt)
		}
		offset = FileHeaderBytes
	} else if !legacy {
		return 0, fmt.Errorf("%w: invalid WAL magic", checksum.ErrCorrupt)
	}
	finish := func(offset int64, repair bool) (int64, error) {
		if repair {
			if _, err := repairTail(file, offset); err != nil {
				return 0, err
			}
		}
		if legacy {
			// Upgrade only after every complete frame and its application record have
			// been validated. The body remains byte-for-byte unchanged.
			if err := upgradeHeader(path, file, offset); err != nil {
				return 0, err
			}
			return offset + 4, nil
		}
		return offset, nil
	}
	header := make([]byte, headerSize)
	for {
		_, err := io.ReadFull(file, header)
		if err == io.EOF {
			return finish(offset, false)
		}
		if err == io.ErrUnexpectedEOF {
			return finish(offset, true)
		}
		if err != nil {
			return 0, err
		}
		if checksum.Sum(header[:8]) != binary.LittleEndian.Uint32(header[8:]) {
			return 0, fmt.Errorf("%w: WAL header checksum mismatch at %d", checksum.ErrCorrupt, offset)
		}
		size := binary.LittleEndian.Uint32(header)
		if size == 0 || size > MaxRecordBytes {
			return 0, fmt.Errorf("%w: invalid WAL record length %d at %d", checksum.ErrCorrupt, size, offset)
		}
		payload := make([]byte, int(size))
		if _, err := io.ReadFull(file, payload); err == io.EOF || err == io.ErrUnexpectedEOF {
			return finish(offset, true)
		} else if err != nil {
			return 0, err
		}
		if checksum.Sum(payload) != binary.LittleEndian.Uint32(header[4:]) {
			return 0, fmt.Errorf("%w: WAL payload checksum mismatch at %d", checksum.ErrCorrupt, offset)
		}
		if err := visit(payload); err != nil {
			return 0, fmt.Errorf("WAL record at %d: %w", offset, err)
		}
		offset += int64(headerSize) + int64(size)
	}
}

func fileHeader() []byte {
	header := make([]byte, FileHeaderBytes)
	copy(header, magic)
	binary.LittleEndian.PutUint32(header[len(magic):], checksum.Sum(magic))
	return header
}
func upgradeHeader(path string, source *os.File, size int64) error {
	target, err := os.CreateTemp(filepath.Dir(path), ".wal-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(target.Name())
	defer target.Close()
	if _, err := target.Write(fileHeader()); err != nil {
		return err
	}
	if _, err := source.Seek(int64(len(legacyMagic)), io.SeekStart); err != nil {
		return err
	}
	if _, err := io.CopyN(target, source, size-int64(len(legacyMagic))); err != nil {
		return err
	}
	if err := target.Sync(); err != nil {
		return err
	}
	if err := target.Close(); err != nil {
		return err
	}
	if err := os.Rename(target.Name(), path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func repairTail(file *os.File, offset int64) (int64, error) {
	if err := file.Truncate(offset); err != nil {
		return 0, err
	}
	if err := file.Sync(); err != nil {
		return 0, err
	}
	return offset, nil
}

// Reset atomically replaces the journal with an empty one. The checkpoint
// covering all prior records must already be durable before this is called.
func Reset(path string) error {
	dir := filepath.Dir(path)
	if err := EnsureDir(dir); err != nil {
		return err
	}
	file, err := os.CreateTemp(dir, ".wal-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(fileHeader()); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return err
	}
	return syncDir(dir)
}
func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// EnsureDir syncs each newly created directory entry in its parent. A synced
// WAL file alone would not make a new data-directory path durable.
func EnsureDir(path string) error {
	var missing []string
	for candidate := filepath.Clean(path); ; candidate = filepath.Dir(candidate) {
		info, err := os.Stat(candidate)
		if err == nil {
			if !info.IsDir() {
				return fmt.Errorf("%s is not a directory", candidate)
			}
			break
		}
		if !os.IsNotExist(err) {
			return err
		}
		missing = append(missing, candidate)
		if filepath.Dir(candidate) == candidate {
			return fmt.Errorf("missing filesystem root")
		}
	}
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	for i := len(missing) - 1; i >= 0; i-- {
		if err := syncDir(filepath.Dir(missing[i])); err != nil {
			return err
		}
	}
	return nil
}
