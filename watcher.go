package zksync

import (
	"fmt"
	"sync"

	"github.com/samuel/go-zookeeper/zk"
)

type WatchType int

const (
	WatchData WatchType = iota
	WatchExist
	WatchChildren

	// how many messages to store before skipping a subscriber
	watchBuffer = 10
)

// Watcher implements locked watch loops on ZooKeeper nodes. It will
// establish its watches under a read lock at a specified path and
// announce changes to a set of subscribers.
//
// Watcher tries to re-establish watches as quickly as it can after
// firing, but it is possible with many writers for it to lose a few
// state changes. When subscribers receive an event, they should read
// with a read lock to get the current value of whatever they are
// watching.
//
// The watcher might encounter an error in its listen loop (for
// example, if it becomes disconnected from ZooKeeper). In this case,
// it will emit a special Event announcing that it is no longer
// watching, it will close all subscriber channels, and it will
// exit. It will not attempt to reconnect.
//
// This is the message it sends in case of error:
//
//  	zk.Event{
//			Type:  zk.EventNotWatching,
//			State: zk.StateDisconnected,
//			Path:  w.watchPath,
//			Err:   err,
//		}
//
// where err is the actual error that caused it to shut down.
type Watcher struct {
	watchPath string
	lockPath  string
	zk        *zk.Conn

	subscribers map[int]chan zk.Event
	subsLock    sync.Mutex
}

// NewWatcher establishes a watch of watchPath, using locks at
// lockPath. The watch is of the specified type and uses the specified
// connection. It immediately connects to ZooKeeper and starts listening.
func NewWatcher(watchPath, lockPath string, t WatchType, conn *zk.Conn) *Watcher {
	w := &Watcher{
		watchPath:   watchPath,
		lockPath:    lockPath,
		zk:          conn,
		subscribers: make(map[int]chan zk.Event),
	}
	go w.watch(t)
	return w
}

func (w *Watcher) watch(t WatchType) {
	lock := NewRWMutex(w.zk, w.lockPath, publicACL)
	for {
		var (
			recv <-chan zk.Event
			err  error
		)
		err = lock.RLock()
		if err != nil {
			w.announceErr(fmt.Errorf("err establishing lock, err=%q", err))
			return
		}

		switch t {
		case WatchChildren:
			_, _, recv, err = w.zk.ChildrenW(w.watchPath)
		case WatchData:
			_, _, recv, err = w.zk.GetW(w.watchPath)
		case WatchExist:
			_, _, recv, err = w.zk.ExistsW(w.watchPath)
		}
		if err != nil {
			w.announceErr(err)
			lock.Unlock()
			return
		}

		err = lock.Unlock()
		if err != nil {
			w.announceErr(err)
			return
		}
		ev := <-recv
		// announce in a goroutine so we can immediately re-establish the watch
		go w.announceEvent(ev)
	}
}

// Susbscribe adds a new subscriber to events from the
// watcher. returns an ID that can be used to unsubscibe later and a
// channel of events
func (w *Watcher) Subscribe() (int, chan zk.Event) {
	w.subsLock.Lock()
	defer w.subsLock.Unlock()

	id := len(w.subscribers)
	ch := make(chan zk.Event, watchBuffer)

	w.subscribers[id] = ch
	return id, ch
}

// Unsubscribe from the watcher - no more messages will be sent to
// that channel (it won't be closed)
func (w *Watcher) Unsubscribe(id int) {
	w.subsLock.Lock()
	defer w.subsLock.Unlock()
	delete(w.subscribers, id)
}

// announce an error to subscribers.
func (w *Watcher) announceErr(err error) {
	w.subsLock.Lock()
	defer w.subsLock.Unlock()

	for _, sub := range w.subscribers {
		ev := zk.Event{
			Type:  zk.EventNotWatching,
			State: zk.StateDisconnected,
			Path:  w.watchPath,
			Err:   err,
		}
		select {
		case sub <- ev:
		default:
		}
		close(sub)
	}
	w.subscribers = make(map[int]chan zk.Event)
}

func (w *Watcher) announceEvent(ev zk.Event) {
	w.subsLock.Lock()
	defer w.subsLock.Unlock()

	for _, sub := range w.subscribers {
		select {
		case sub <- ev:
		default:
		}
	}
}
