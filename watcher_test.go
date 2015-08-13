package zksync

import (
	"sync"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

func TestWatcherExist(t *testing.T) {
	var (
		timeout = time.Millisecond * 50
		watch   = testPath("/watch")
		lock    = testPath("/lock")
	)

	defer cleanup(t)
	conn := connectAllZk(t)
	defer conn.Close()

	w := NewWatcher(watch, lock, WatchExist, conn)
	id1, ch1 := w.Subscribe()
	var ev zk.Event

	// nothing is happening, so waiting for a message should block
	select {
	case ev = <-ch1:
		t.Errorf("received event before timeout: %+v", ev)
	case <-time.After(timeout):
	}

	// make the watched node
	if _, err := LockedCreate(conn, lock, watch, []byte{}, 0, publicACL); err != nil {
		t.Fatalf("failed to create watched node, err=%q", err)
	}

	// now we should get a message
	select {
	case ev = <-ch1:
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for watch")
	}

	// verify the message
	if ev.Type != zk.EventNodeCreated {
		t.Errorf("unexpected event type from watcher, have=%q want=%q", ev.Type, zk.EventNodeCreated)
	}
	if ev.Path != watch {
		t.Errorf("unexpected event path from watcher, have=%q want=%q", ev.Path, watch)
	}

	// deleting the node should trigger another message
	if err := LockedDelete(conn, lock, watch, -1); err != nil {
		t.Fatalf("failed to delete watched node, err=%q", err)
	}

	select {
	case ev = <-ch1:
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for watch of delete")
	}

	// verify the message
	if ev.Type != zk.EventNodeDeleted {
		t.Errorf("unexpected event type from watcher, have=%q want=%q", ev.Type, zk.EventNodeDeleted)
	}
	if ev.Path != watch {
		t.Errorf("unexpected event path from watcher, have=%q want=%q", ev.Path, watch)
	}

	// after unsubscribing, we should stop receiving messages
	w.Unsubscribe(id1)
	if _, err := LockedCreate(conn, lock, watch, []byte{}, 0, publicACL); err != nil {
		t.Fatalf("failed to create watched node, err=%q", err)
	}
	select {
	case ev = <-ch1:
		t.Errorf("received event after unsubscribing: %+v", ev)
	case <-time.After(timeout):
	}

}

func TestWatcherData(t *testing.T) {
	var (
		timeout = time.Millisecond * 50
		watch   = testPath("/watch")
		lock    = testPath("/watch-lock")
	)

	defer cleanup(t)
	conn := connectAllZk(t)
	defer conn.Close()

	data := []byte("value1")
	// make the watched node
	if _, err := LockedCreate(conn, lock, watch, data, 0, publicACL); err != nil {
		t.Fatalf("failed to create watched node, err=%q", err)
	}

	w := NewWatcher(watch, lock, WatchData, conn)
	id1, ch1 := w.Subscribe()
	var ev zk.Event

	// nothing is happening, so waiting for a message should block
	select {
	case ev = <-ch1:
		t.Errorf("received event before timeout: %+v", ev)
	case <-time.After(timeout):
	}

	// update the value
	data = []byte("value2")
	if _, err := LockedSet(conn, lock, watch, data, -1); err != nil {
		t.Fatalf("failed to update watched node, err=%q", err)
	}

	// now we should get a message
	select {
	case ev = <-ch1:
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for watch")
	}

	// verify the message
	if ev.Type != zk.EventNodeDataChanged {
		t.Errorf("unexpected event type from watcher, have=%q want=%q", ev.Type, zk.EventNodeDataChanged)
	}
	if ev.Path != watch {
		t.Errorf("unexpected event path from watcher, have=%q want=%q", ev.Path, watch)
	}

	// deleting the node should trigger another message
	if err := LockedDelete(conn, lock, watch, -1); err != nil {
		t.Fatalf("failed to delete watched node, err=%q", err)
	}

	select {
	case ev = <-ch1:
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for watch of delete")
	}

	// verify the message
	if ev.Type != zk.EventNodeDeleted {
		t.Errorf("unexpected event type from watcher, have=%q want=%q", ev.Type, zk.EventNodeDeleted)
	}
	if ev.Path != watch {
		t.Errorf("unexpected event path from watcher, have=%q want=%q", ev.Path, watch)
	}

	// after unsubscribing, we should stop receiving messages
	w.Unsubscribe(id1)
	if _, err := LockedCreate(conn, lock, watch, []byte{}, 0, publicACL); err != nil {
		t.Fatalf("failed to create watched node, err=%q", err)
	}
	select {
	case ev = <-ch1:
		t.Errorf("received event after unsubscribing: %+v", ev)
	case <-time.After(timeout):
	}

}

func TestWatcherMultipleSubscribers(t *testing.T) {
	var (
		timeout = time.Millisecond * 50
		watch   = testPath("/watch")
		lock    = testPath("/watch-lock")
	)

	defer cleanup(t)
	conn := connectAllZk(t)
	defer conn.Close()

	w := NewWatcher(watch, lock, WatchExist, conn)
	nSubs := 3
	var (
		ids = make([]int, nSubs)
		chs = make([]chan zk.Event, nSubs)
	)
	for i := 0; i < nSubs; i++ {
		ids[i], chs[i] = w.Subscribe()
	}
	var wg sync.WaitGroup
	// nothing is happening, so waiting for a message should block
	for i := 0; i < nSubs; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			select {
			case ev := <-chs[id]:
				t.Errorf("received event before timeout: %+v", ev)
			case <-time.After(timeout):
			}
		}(i)
	}
	wg.Wait()

	// add the watched node
	if _, err := LockedCreate(conn, lock, watch, []byte{}, 0, publicACL); err != nil {
		t.Fatalf("failed to create watched node, err=%q", err)
	}

	// now we should get a message
	evs := make([]zk.Event, nSubs)
	for i := 0; i < nSubs; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			select {
			case evs[id] = <-chs[id]:
			case <-time.After(timeout):
				t.Errorf("timed out waiting for event")
			}
		}(i)
	}
	wg.Wait()

	for _, ev := range evs {
		// verify the message
		if ev.Type != zk.EventNodeCreated {
			t.Errorf("unexpected event type from watcher, have=%q want=%q", ev.Type, zk.EventNodeCreated)
		}
		if ev.Path != watch {
			t.Errorf("unexpected event path from watcher, have=%q want=%q", ev.Path, watch)
		}
	}

	// deleting the node should trigger another message
	if err := LockedDelete(conn, lock, watch, -1); err != nil {
		t.Fatalf("failed to delete watched node, err=%q", err)
	}

	evs = make([]zk.Event, nSubs)
	for i := 0; i < nSubs; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			select {
			case evs[id] = <-chs[id]:
			case <-time.After(timeout):
				t.Errorf("timed out waiting for event")
			}
		}(i)
	}
	wg.Wait()

	for _, ev := range evs {
		// verify the message
		if ev.Type != zk.EventNodeDeleted {
			t.Errorf("unexpected event type from watcher, have=%q want=%q", ev.Type, zk.EventNodeDeleted)
		}
		if ev.Path != watch {
			t.Errorf("unexpected event path from watcher, have=%q want=%q", ev.Path, watch)
		}
	}

	// after unsubscribing, one should stop getting messages, but others should be fine
	w.Unsubscribe(ids[0])
	if _, err := LockedCreate(conn, lock, watch, []byte{}, 0, publicACL); err != nil {
		t.Fatalf("failed to create watched node, err=%q", err)
	}
	select {
	case ev := <-chs[0]:
		t.Errorf("received event after unsubscribing: %+v", ev)
	case <-time.After(timeout):
	}

	evs = make([]zk.Event, nSubs-1)
	for i := 1; i < nSubs; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			select {
			case evs[id-1] = <-chs[id]:
			case <-time.After(timeout):
				t.Errorf("timed out waiting for event")
			}
		}(i)
	}
	wg.Wait()

	for _, ev := range evs {
		// verify the message
		if ev.Type != zk.EventNodeCreated {
			t.Errorf("unexpected event type from watcher, have=%q want=%q, ev=%+v", ev.Type, zk.EventNodeCreated, ev)
		}
		if ev.Path != watch {
			t.Errorf("unexpected event path from watcher, have=%q want=%q", ev.Path, watch)
		}
	}

}
