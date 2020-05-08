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
		ch, ok := l.chs[name]
		if !ok {
			l.chs[name] = make(chan struct{})
			l.mu.Unlock()
			return
		}
		l.mu.Unlock()
		<-ch
	}
}

func (l *Locker) Unlock(name string) {
	l.mu.Lock()
	ch, ok := l.chs[name]
	if ok {
		delete(l.chs, name)
		close(ch)
	}
	l.mu.Unlock()
}
