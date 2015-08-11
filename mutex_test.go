package zksync

import (
	"sync"
	"testing"
	"time"
)

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

	conn := setupZk(t)
	defer conn.Close()

	l := NewRWMutex(conn, testPath("TestReadLockSimpleCreation"))
	err := l.RLock(time.Second * 1)
	if err != nil {
		t.Errorf("rlock err=%q", err)
	}
}

func TestWriteLockSimpleCreation(t *testing.T) {
	defer cleanup(t)

	conn := setupZk(t)
	defer conn.Close()

	l := NewRWMutex(conn, testPath("TestWriteLockSimpleCreation"))
	err := l.WLock(time.Second * 1)
	if err != nil {
		t.Errorf("rlock err=%q", err)
	}

}

func TestMultipleReadLocksDontBlock(t *testing.T) {
	defer cleanup(t)

	var (
		// number of concurrent read lock holders
		n int = 5

		// how long to wait before assuming a reader is blocked
		timeout time.Duration = 500 * time.Millisecond
	)
	var wg sync.WaitGroup
	for i := 0; i < n; i += 1 {
		wg.Add(1)
		go func() {
			conn := setupZk(t)
			defer conn.Close()
			defer wg.Done()

			l := NewRWMutex(conn, testPath("TestMultipleReadLocksDontBlock"))
			err := l.RLock(time.Second * 1)
			if err != nil {
				t.Errorf("rlock err=%q", err)
			}
		}()
	}

	ch := make(chan struct{})
	go func() {
		wg.Wait()
		ch <- struct{}{}
	}()

	select {
	case <-ch:
	case <-time.After(timeout):
		t.Error("timeout waiting for read locks to establish")
	}
}

func TestWriteLockBlocksReadLocks(t *testing.T) {
	defer cleanup(t)

	var timeout time.Duration = 500 * time.Millisecond // how long to wait to declare readers blocked

	path := testPath("TestWriteLockBlocksReadLocks")

	writeConn := setupZk(t)
	defer writeConn.Close()

	writeLock := NewRWMutex(writeConn, path)
	err := writeLock.WLock(time.Second * 1)
	if err != nil {
		t.Fatalf("wlock err=%q", err)
	}

	readConn := setupZk(t)
	defer readConn.Close()
	readLock := NewRWMutex(readConn, path)

	ch := make(chan struct{})
	go func() {
		readLock.RLock(timeout * 5)
		ch <- struct{}{}
	}()

	select {
	case <-ch:
		t.Error("read wasnt blocked by write lock's presence")
	case <-time.After(timeout):
	}

}

func TestReleasingWriteLockUnblocksReaders(t *testing.T) {
	defer cleanup(t)

	var timeout time.Duration = 500 * time.Millisecond // how long to wait to declare readers blocked

	path := testPath("TestReleasingWriteLockUnblocksReaders")

	writeConn := setupZk(t)
	defer writeConn.Close()

	writeLock := NewRWMutex(writeConn, path)
	err := writeLock.WLock(time.Second * 1)
	if err != nil {
		t.Fatalf("wlock err=%q", err)
	}

	readConn := setupZk(t)
	defer readConn.Close()
	readLock := NewRWMutex(readConn, path)

	ch := make(chan struct{})
	go func() {
		readLock.RLock(timeout * 5)
		ch <- struct{}{}
	}()

	err = writeLock.Unlock()
	if err != nil {
		t.Fatalf("wlock unlock err=%q", err)
	}

	select {
	case <-ch:
	case <-time.After(timeout):
		t.Error("read wasnt unblocked when write lock was released")
	}

}

func TestWriteLocksGoInOrder(t *testing.T) {
	defer cleanup(t)

	path := testPath("TestWriteLocksGoInOrder")
	var n = 5

	// syncronously grab a lock
	writeConn1 := setupZk(t)
	defer writeConn1.Close()

	writeLock1 := NewRWMutex(writeConn1, path)
	err := writeLock1.WLock(time.Second * 1)
	if err != nil {
		t.Fatalf("wlock 1 err=%q", err)
	}

	// asynchronously stack up several more requests
	createOrder := make(chan int, n)
	executeOrder := make(chan int, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i += 1 {
		writeConn := setupZk(t)
		defer writeConn.Close()

		writeLock := NewRWMutex(writeConn, path)
		wg.Add(1)
		go func(id int) {
			createOrder <- id
			writeLock.WLock(time.Second * 1)
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
