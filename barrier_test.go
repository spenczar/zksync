package zksync

import (
	"sync"
	"testing"
	"time"
)

// how long for a goroutine to spin before considering it blocked by a barrier
const barrierTimeout = time.Millisecond * 50

func TestBarrierSet(t *testing.T) {
	defer cleanup(t)

	conn := connectAllZk(t)
	defer conn.Close()

	barrier := NewBarrier(conn, testPath("/TestBarrierSet"), publicACL)
	err := barrier.Set()
	if err != nil {
		t.Fatalf("unable to set barrier: err=%q", err)
	}

}

func TestBarrierUnset(t *testing.T) {
	defer cleanup(t)

	conn := connectAllZk(t)
	defer conn.Close()

	barrier := NewBarrier(conn, testPath("/TestBarrierUnset"), publicACL)
	err := barrier.Set()
	if err != nil {
		t.Fatalf("unable to set barrier: err=%q", err)
	}
	err = barrier.Unset()
	if err != nil {
		t.Fatalf("unable to unset barrier: err=%q", err)
	}
}

func TestBarrierIsBlocking(t *testing.T) {
	defer cleanup(t)

	conn := connectAllZk(t)
	defer conn.Close()

	barrier := NewBarrier(conn, testPath("/TestBarrierIsBlocking"), publicACL)
	err := barrier.Set()
	if err != nil {
		t.Fatalf("unable to set barrier: err=%q", err)
	}

	ch := make(chan struct{}, 1)
	go func() {
		barrier.Wait()
		ch <- struct{}{}
	}()

	select {
	case <-ch:
		t.Errorf("barrier did not block for at least %s", barrierTimeout)
	case <-time.After(barrierTimeout):
	}
}

func TestBarrierUnsetUnblocks(t *testing.T) {
	defer cleanup(t)

	conn := connectAllZk(t)
	defer conn.Close()

	barrier := NewBarrier(conn, testPath("/TestBarrierUnsetUnblocks"), publicACL)
	err := barrier.Set()
	if err != nil {
		t.Fatalf("unable to set barrier: err=%q", err)
	}

	ch := make(chan struct{}, 1)
	go func() {
		barrier.Wait()
		ch <- struct{}{}
	}()

	select {
	case <-ch:
		t.Errorf("barrier did not block for at least %s", barrierTimeout)
	case <-time.After(barrierTimeout):
	}

	barrier.Unset()

	ch2 := make(chan struct{}, 1)
	go func() {
		barrier.Wait()
		ch2 <- struct{}{}
	}()

	select {
	case <-ch2:
	case <-time.After(barrierTimeout):
		t.Errorf("barrier was unset, but still blocked for at least %s", barrierTimeout)
	}

}

func TestMultipleConnsSeeSameBarrier(t *testing.T) {
	defer cleanup(t)

	conn1 := connectAllZk(t)
	defer conn1.Close()
	barrier1 := NewBarrier(conn1, testPath("/TestMultipleConnsSeeSameBarrier"), publicACL)

	conn2 := connectAllZk(t)
	defer conn2.Close()
	barrier2 := NewBarrier(conn2, testPath("/TestMultipleConnsSeeSameBarrier"), publicACL)

	err := barrier1.Set()
	if err != nil {
		t.Fatalf("unable to set barrier1: err=%q", err)
	}

	ch := make(chan struct{}, 2)
	go func() {
		barrier1.Wait()
		ch <- struct{}{}
	}()
	go func() {
		barrier2.Wait()
		ch <- struct{}{}
	}()

	select {
	case <-ch:
		t.Errorf("barrier did not block both clients for at least %s", barrierTimeout)
	case <-time.After(barrierTimeout):
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		barrier1.Wait()
		wg.Done()
	}()
	go func() {
		barrier2.Wait()
		wg.Done()
	}()

	barrier1.Unset()

	ch2 := make(chan struct{}, 1)
	go func() {
		wg.Wait()
		ch2 <- struct{}{}
	}()

	select {
	case <-ch2:
	case <-time.After(barrierTimeout):
		t.Errorf("barrier was unset, but still blocked a client for at least %s", barrierTimeout)
	}

}
