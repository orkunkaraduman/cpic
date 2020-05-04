package main

import (
	"sync"
)

type fileHash struct {
	size int64
	sum  [32]byte
}

var (
	fileHashMu sync.Mutex
	fileHashMap map[fileHash]struct{}
)

func init() {
	fileHashMap = make(map[fileHash]struct{}, 128)
}

func fileHashSet(fh fileHash) bool {
	fileHashMu.Lock()
	if _, ok := fileHashMap[fh]; ok {
		fileHashMu.Unlock()
		return false
	}
	fileHashMap[fh] = struct{}{}
	fileHashMu.Unlock()
	return true
}

func fileHashDel(fh fileHash) bool {
	fileHashMu.Lock()
	if _, ok := fileHashMap[fh]; !ok {
		fileHashMu.Unlock()
		return false
	}
	delete(fileHashMap, fh)
	fileHashMu.Unlock()
	return true
}
