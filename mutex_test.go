package zksync

import (
	"sync"
	"testing"
	"time"
)

// how long to wait before assuming a reader is blocked
const mutexTimeout = time.Second * 3

func TestSequenceNumber(t *testing.T) {
	type testcase struct {
		path string
		want int
	}

	testcases := []testcase{
		{"/locks/read-00001", 1},
		{"/testlock/_c_4397756eb59edeab74acdc231f75b8d9-read-0000000004", 4},
		{"/testlock/_c_4397756eb59edeab74acdc231f75b8d9-write-0000000004", 4},
		{"/testlock/_c_4397756eb59edeab74acdc231f75b8d9-write-1000000000", 1000000000},
		{"/testlock/_c_4397756eb59edeab74acdc231f75b8d9-read-0000000099", 99},
		{"/locks/in/a/really/long/path/_blah_read-00001", 1},
	}

	for _, tc := range testcases {
		have, _ := parseSequenceNumber(tc.path)
		if have != tc.want {
			t.Errorf("parseSequenceNumber fail  path=%q have=%d want=%d", tc.path, have, tc.want)
		}
	}

}

func TestReadLockSimpleCreation(t *testing.T) {
	defer cleanup(t)

	conn := connectAllZk(t)
	defer conn.Close()

	l := NewRWMutex(conn, testPath("TestReadLockSimpleCreation"), publicACL)
	err := l.RLock()
	if err != nil {
		t.Errorf("rlock err=%q", err)
	}
}

func TestWriteLockSimpleCreation(t *testing.T) {
	defer cleanup(t)

	conn := connectAllZk(t)
	defer conn.Close()

	l := NewRWMutex(conn, testPath("TestWriteLockSimpleCreation"), publicACL)
	err := l.WLock()
	if err != nil {
		t.Errorf("rlock err=%q", err)
	}

}

func TestMultipleReadLocksDontBlock(t *testing.T) {
	defer cleanup(t)

	var (
		// number of concurrent read lock holders
		n  int = 5
		wg sync.WaitGroup
	)
	for i := 0; i < n; i += 1 {
		wg.Add(1)
		go func() {
			conn := connectAllZk(t)
			defer conn.Close()
			defer wg.Done()

			l := NewRWMutex(conn, testPath("TestMultipleReadLocksDontBlock"), publicACL)
			err := l.RLock()
			if err != nil {
				t.Errorf("rlock err=%q", err)
			}
		}()
	}

	ch := make(chan struct{}, 1)
	go func() {
		wg.Wait()
		ch <- struct{}{}
	}()

	select {
	case <-ch:
	case <-time.After(mutexTimeout):
		t.Error("timeout waiting for read locks to establish")
	}
}

func TestWriteLockBlocksReadLocks(t *testing.T) {
	defer cleanup(t)

	path := testPath("TestWriteLockBlocksReadLocks")

	writeConn := connectAllZk(t)
	defer writeConn.Close()

	writeLock := NewRWMutex(writeConn, path, publicACL)
	err := writeLock.WLock()
	if err != nil {
		t.Fatalf("wlock err=%q", err)
	}

	readConn := connectAllZk(t)
	defer readConn.Close()
	readLock := NewRWMutex(readConn, path, publicACL)

	ch := make(chan struct{}, 1)
	go func() {
		readLock.RLock()
		ch <- struct{}{}
	}()

	select {
	case <-ch:
		t.Error("read wasnt blocked by write lock's presence")
	case <-time.After(mutexTimeout):
	}

}

func TestReleasingWriteLockUnblocksReaders(t *testing.T) {
	defer cleanup(t)

	path := testPath("TestReleasingWriteLockUnblocksReaders")

	writeConn := connectAllZk(t)
	defer writeConn.Close()

	writeLock := NewRWMutex(writeConn, path, publicACL)
	err := writeLock.WLock()
	if err != nil {
		t.Fatalf("wlock err=%q", err)
	}

	readConn := connectAllZk(t)
	defer readConn.Close()
	readLock := NewRWMutex(readConn, path, publicACL)

	ch := make(chan struct{}, 1)
	go func() {
		readLock.RLock()
		ch <- struct{}{}
	}()

	err = writeLock.Unlock()
	if err != nil {
		t.Fatalf("wlock unlock err=%q", err)
	}

	select {
	case <-ch:
	case <-time.After(mutexTimeout):
		t.Error("read wasnt unblocked when write lock was released")
	}

}

func TestReleasingWriteLockUnblocksWriters(t *testing.T) {
	defer cleanup(t)

	path := testPath("TestReleasingWriteLockUnblocksWriters")

	writeConn1 := connectAllZk(t)
	defer writeConn1.Close()

	writeLock1 := NewRWMutex(writeConn1, path, publicACL)
	err := writeLock1.WLock()
	if err != nil {
		t.Fatalf("wlock1 err=%q", err)
	}

	writeConn2 := connectAllZk(t)
	defer writeConn2.Close()
	writeLock2 := NewRWMutex(writeConn2, path, publicACL)

	ch := make(chan struct{}, 1)
	go func() {
		writeLock2.WLock()
		ch <- struct{}{}
	}()

	err = writeLock1.Unlock()
	if err != nil {
		t.Fatalf("wlock1 unlock err=%q", err)
	}

	select {
	case <-ch:
	case <-time.After(mutexTimeout):
		t.Error("write2 wasnt unblocked when write1 was released")
	}
}

func TestWriteLocksGoInOrder(t *testing.T) {
	defer cleanup(t)

	path := testPath("TestWriteLocksGoInOrder")
	var n = 5

	// syncronously grab a lock
	writeConn1 := connectAllZk(t)
	defer writeConn1.Close()

	writeLock1 := NewRWMutex(writeConn1, path, publicACL)
	err := writeLock1.WLock()
	if err != nil {
		t.Fatalf("wlock 1 err=%q", err)
	}

	// asynchronously stack up several more requests
	createOrder := make(chan int, n)
	executeOrder := make(chan int, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i += 1 {
		writeConn := connectAllZk(t)
		defer writeConn.Close()

		writeLock := NewRWMutex(writeConn, path, publicACL)
		wg.Add(1)
		go func(id int) {
			createOrder <- id
			writeLock.WLock()
			executeOrder <- id
			writeLock.Unlock()
			wg.Done()
		}(i)
		time.Sleep(20 * time.Millisecond)
	}

	go func() {
		wg.Wait()
		close(createOrder)
		close(executeOrder)
	}()

	// trigger the n writers by unlocking the first one
	writeLock1.Unlock()
	for id := range createOrder {
		executeID := <-executeOrder
		if id != executeID {
			t.Errorf("write lock out of order  have=%d want=%d", executeID, id)
		}
	}
}

func TestRWMutexCleanExitReleasesLock(t *testing.T) {
	defer cleanup(t)

	path := testPath("TestRWMutexCleanExitReleasesLock")

	// grab a lock
	writeConn1 := connectAllZk(t)
	defer quietClose(writeConn1) // we plan on closing writeConn1 ourselves

	writeLock1 := NewRWMutex(writeConn1, path, publicACL)
	err := writeLock1.WLock()
	if err != nil {
		t.Fatalf("wlock 1 err=%q", err)
	}

	// queue up another who wants the lock
	writeConn2 := connectAllZk(t)
	defer writeConn2.Close()
	writeLock2 := NewRWMutex(writeConn2, path, publicACL)

	// try to acquire the lock, send signal when we have done so
	ch := make(chan struct{}, 1)
	go func() {
		err := writeLock2.WLock()
		if err != nil {
			t.Fatalf("wlock 2 err=%q", err)
		}
		close(ch)
	}()

	select {
	case <-ch:
		t.Fatal("wlock 2 acquired lock while it was still active")
	case <-time.After(mutexTimeout):
	}

	// disconnect writeConn1
	writeConn1.Close()
	// ZooKeeper should time out the session
	select {
	case <-ch:
	case <-time.After(zkTimeout + mutexTimeout):
		t.Fatal("wlock 2 failed to acquire lock after wlock1's clean exit")
	}

}

func TestRWMutexRandomDisconnect(t *testing.T) {
	if testing.Short() {
		t.Skipf("skipping, takes at least %s", zkTimeout)
	}

	defer cleanup(t)

	var (
		path = testPath("TestRWMutexRandomDisconnect")
	)

	// make a connection that we can fiddle with
	badConn := connectZk(t, 0)
	defer toxiproxyClient.ResetState()
	defer quietClose(badConn)

	// grab a lock on the bad connection
	writeLock1 := NewRWMutex(badConn, path, publicACL)
	err := writeLock1.WLock()
	if err != nil {
		t.Fatalf("wlock 1 err=%q", err)
	}

	// Now make a second, good connection
	goodConn := connectZk(t, 1)
	defer goodConn.Close()

	// queue up for a lock on the good connection
	writeLock2 := NewRWMutex(goodConn, path, publicACL)
	// try to acquire the lock, send signal when we have done so
	errs := make(chan error, 1)
	go func() {
		errs <- writeLock2.WLock()
	}()

	// suddenly destroy the bad connection by killing the proxy
	zookeeperProxies[0].Enabled = false
	if err := zookeeperProxies[0].Save(); err != nil {
		t.Fatalf("unable to save change to proxy, err=%q", err)
	}

	// ZooKeeper should time out the session
	select {
	case err := <-errs:
		if err != nil {
			t.Fatal("wlock 2 err=%q", err)
		}
	case <-time.After(zkTimeout * 3):
		t.Fatal("wlock 2 failed to acquire lock after wlock1's dirty exit")
	}

}
