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
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f, path: path}, nil
}

func (w *WAL) Append(atom *core.CausalAtom) error {
	w.mu.Lock()
	defer w.mu.Unlock()

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
	return w.file.Close()
}

func (w *WAL) ReadAll() ([]*core.CausalAtom, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

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
