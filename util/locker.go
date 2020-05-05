package util

import (
	"sync"
)

type Locker struct {
	mu  sync.Mutex
	chs map[string]chan struct{}
}

func NewLocker() *Locker {
	return &Locker{
		chs: make(map[string]chan struct{}, 128),
	}
}

func (l *Locker) Lock(name string) {
	for {
		l.mu.Lock()
		if _, ok := l.chs[name]; !ok {
			l.chs[name] = make(chan struct{})
			l.mu.Unlock()
			return
		}
		ch := l.chs[name]
		l.mu.Unlock()
		<-ch
	}
}

func (l *Locker) Unlock(name string) {
	l.mu.Lock()
	if ch, ok := l.chs[name]; ok {
		delete(l.chs, name)
		close(ch)
	}
	l.mu.Unlock()
}
