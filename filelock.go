package main

import (
	"sync"
)

var (
	fileLockMu sync.Mutex
	fileLockMap map[string]chan struct{}
)

func init() {
	fileLockMap = make(map[string]chan struct{}, 128)
}

func fileLock(path string) {
	for {
		fileLockMu.Lock()
		if _, ok := fileLockMap[path]; !ok {
			fileLockMap[path] = make(chan struct{})
			fileLockMu.Unlock()
			return
		}
		ch := fileLockMap[path]
		fileLockMu.Unlock()
		<-ch
	}
}

func fileUnlock(path string) {
	fileLockMu.Lock()
	if ch, ok := fileLockMap[path]; ok {
		delete(fileLockMap, path)
		close(ch)
	}
	fileLockMu.Unlock()
}
