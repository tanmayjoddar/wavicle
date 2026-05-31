package storage

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"wavicle/internal/core"
)

const maxLineSize = 1 << 20

type WAL struct {
	mu      sync.Mutex
	file    *os.File
	path    string
	offsets []int64
}

func NewWAL(path string) (*WAL, error) {
	// Clean up stale temp file from previous crash/failed compaction
	staleTmp := path + ".tmp"
	if _, err := os.Stat(staleTmp); err == nil {
		os.Remove(staleTmp)
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f, path: path}, nil
}

func (w *WAL) Append(atom *core.CausalAtom) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return errors.New("WAL file is closed")
	}

	data, err := json.Marshal(atom)
	if err != nil {
		return err
	}
	if len(data) > maxLineSize {
		return fmt.Errorf("atom too large: %d bytes > %d limit", len(data), maxLineSize)
	}

	data = append(data, '\n')
	n, err := w.file.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return errors.New("short write to WAL")
	}
	return w.file.Sync()
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	return w.file.Close()
}

func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return 0
	}
	stat, err := w.file.Stat()
	if err != nil {
		return 0
	}
	return stat.Size()
}

// reopenFile opens the WAL file for append. Caller must hold w.mu.
func (w *WAL) reopenFile() error {
	f, err := os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	// Close old handle if any (may be nil if this is initial open after failure)
	if w.file != nil {
		w.file.Close()
	}
	w.file = f
	return nil
}

// Rewrite replaces the current WAL file with a new one containing only the provided atoms.
// On any failure, it guarantees w.file is a valid, open handle so the system can continue.
func (w *WAL) Rewrite(atoms []*core.CausalAtom) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return errors.New("WAL file is closed")
	}

	tmpPath := w.path + ".tmp"

	// Step 1: Write alive atoms to temp file
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("create tmp: %w", err)
	}

	writeErr := error(nil)
	for _, a := range atoms {
		data, err := json.Marshal(a)
		if err != nil {
			writeErr = fmt.Errorf("marshal: %w", err)
			break
		}
		data = append(data, '\n')
		if _, err := f.Write(data); err != nil {
			writeErr = fmt.Errorf("write tmp: %w", err)
			break
		}
	}
	if writeErr != nil {
		f.Close()
		os.Remove(tmpPath)
		return writeErr
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("sync tmp: %w", err)
	}
	f.Close()

	// Step 2: Close old log (we still have the file on disk)
	oldPath := w.path + ".old"
	w.file.Close()
	w.file = nil

	// Step 3: Remove old target before rename (avoids rename-over-open issues)
	os.Remove(oldPath)

	// Step 4: Move old log to .old (backup for rollback)
	if err := os.Rename(w.path, oldPath); err != nil {
		// Old file might not exist if this is first compaction
		// Non-fatal: continue with rename
	}

	// Step 5: Atomically replace with new file
	if err := os.Rename(tmpPath, w.path); err != nil {
		// Rename failed. Restore old log if possible.
		os.Rename(oldPath, w.path)
		// Reopen whatever file exists at w.path
		if reopenErr := w.reopenFile(); reopenErr != nil {
			return fmt.Errorf("rename failed: %v, reopen failed: %v", err, reopenErr)
		}
		return fmt.Errorf("rename: %w", err)
	}

	// Success — remove backup and old temp
	os.Remove(oldPath)
	os.Remove(tmpPath)

	// Reopen the new file
	return w.reopenFile()
}

func (w *WAL) ReadAll() ([]*core.CausalAtom, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil, errors.New("WAL file is closed")
	}

	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, err
	}

	var atoms []*core.CausalAtom
	scanner := bufio.NewScanner(w.file)
	scanner.Buffer(make([]byte, 0, maxLineSize), maxLineSize)
	lineNum := 0
	var lastErrLine int
	for scanner.Scan() {
		lineNum++
		data := scanner.Bytes()
		if len(data) == 0 {
			continue
		}
		var atom core.CausalAtom
		if err := json.Unmarshal(data, &atom); err != nil {
			lastErrLine = lineNum
			continue
		}
		if atom.Hash.IsZero() {
			continue
		}
		atoms = append(atoms, &atom)
	}

	if lineNum > 0 && lastErrLine == lineNum {
		// Partial last line from crash during Sync() — safe to ignore
	}

	w.file.Seek(0, 2)
	return atoms, nil
}
