package storage

import (
	"sync"
	"wavicle/internal/core"
)

// FrontierIndex stores the current atom hash for each path.
type FrontierIndex struct {
	mu    sync.RWMutex
	paths map[string]core.Hash
}

func NewFrontierIndex() *FrontierIndex {
	return &FrontierIndex{
		paths: make(map[string]core.Hash),
	}
}

func (f *FrontierIndex) Get(path string) (core.Hash, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	h, ok := f.paths[path]
	return h, ok
}

func (f *FrontierIndex) Set(path string, hash core.Hash) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.paths[path] = hash
}

func (f *FrontierIndex) Delete(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.paths, path)
}

func (f *FrontierIndex) Paths() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	paths := make([]string, 0, len(f.paths))
	for p := range f.paths {
		paths = append(paths, p)
	}
	return paths
}
