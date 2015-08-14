package zksync

import (
	"strconv"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

func TestDoubleBarrier(t *testing.T) {
	defer cleanup(t)

	var (
		n       = 5
		timeout = time.Millisecond * 50 * time.Duration(n) // 50ms per client
		path    = testPath("/TestDoubleBarrier")
	)

	barriers := make([]*DoubleBarrier, n)
	for i := 0; i < n; i++ {
		conn := connectAllZk(t)
		defer conn.Close()
		barriers[i] = NewDoubleBarrier(conn, path, strconv.Itoa(i), n, publicACL)
	}

	ch := make(chan int)

	// call Enter() and do some work in all barriers except barrier 0
	for i := 1; i < n; i++ {
		go func(id int) {
			if err := barriers[id].Enter(); err != nil {
				t.Fatalf("barrier enter err=%q", err)
			}
			ch <- id
		}(i)
	}

	// since barrier 0 hasn't entered, no work should be done yet
	select {
	case id := <-ch:
		t.Fatalf("barrier %d did work before all barriers were ready", id)
	case <-time.After(timeout):
	}

	// add barrier 0
	go func() {
		barriers[0].Enter()
		ch <- 0
	}()

	// Now we should have all 5 values, as all barriers do work
	for i := 0; i < n; i++ {
		select {
		case <-ch:
		case <-time.After(timeout):
			t.Fatalf("timed out waiting for barriers to do work")
		}
	}

	// tell barriers 1 through n-1 to finish
	for i := 1; i < n; i++ {
		go func(id int) {
			if err := barriers[id].Exit(); err != nil {
				t.Fatalf("barrier exit err=%q", err)
			}
			// once unblocked, send a message to ch
			ch <- id
		}(i)
	}

	// again, since barrier 0 hasn't exited, no work should be done
	select {
	case id := <-ch:
		t.Fatalf("barrier %d was unblocked before all barriers exited", id)
	case <-time.After(timeout):
	}

	// exit barrier 0
	go func() {
		barriers[0].Exit()
		ch <- 0
	}()

	// Now we should have all 5 values, as all barriers do work
	for i := 0; i < n; i++ {
		select {
		case <-ch:
		case <-time.After(timeout):
			t.Fatalf("timed out waiting for barriers to do work")
		}
	}
}

func TestDoubleBarrierRemovedWhenDone(t *testing.T) {
	defer cleanup(t)
	conn := connectAllZk(t)
	defer conn.Close()

	path := testPath("/TestDoubleBarrierRemovedWhenDone")
	barrier := NewDoubleBarrier(conn, path, "1", 1, publicACL)

	enter := make(chan error)
	go func() {
		enter <- barrier.Enter()
	}()
	select {
	case err := <-enter:
		if err != nil {
			t.Fatalf("enter err=%q", err)
		}
	case <-time.After(time.Millisecond * 250):
		t.Fatalf("timed out on barrier entry")
	}

	exit := make(chan error)
	go func() {
		exit <- barrier.Exit()
	}()
	select {
	case err := <-exit:
		if err != nil {
			t.Fatalf("enter err=%q", err)
		}
	case <-time.After(time.Millisecond * 250):
		t.Fatalf("timed out on barrier exit")
	}

	exists, _, err := conn.Exists(path)
	if err != nil {
		t.Fatalf("exists check err=%q", err)
	}
	if exists {
		t.Errorf("failed to delete barrier node %s", path)
	}
}

func TestDoubleBarrierDirtyExit(t *testing.T) {
	defer cleanup(t)

	var (
		n    = 3
		path = testPath("/TestDoubleBarrierDirtyExit")
	)

	barriers := make([]*DoubleBarrier, n)
	conns := make([]*zk.Conn, n)
	for i := 0; i < n; i++ {
		conns[i] = connectAllZk(t)

		if i == 0 { // we will manually close conn 0
			defer quietClose(conns[i])
		} else {
			defer conns[i].Close()
		}

		barriers[i] = NewDoubleBarrier(conns[i], path, strconv.Itoa(i), n, publicACL)
	}

	chans := make([]chan int, n)
	// enter all barriers
	for i := 0; i < n; i++ {
		chans[i] = make(chan int)
		go func(id int) {
			if err := barriers[id].Enter(); err != nil {
				t.Fatalf("barrier enter id=%d err=%q", id, err)
			}
			chans[id] <- id
		}(i)
	}

	ch := make(chan int)
	// tell barriers 1 through n-1 to finish cleanly
	for i := 1; i < n; i++ {
		go func(id int) {
			<-chans[id] // verify that Enter() worked
			if err := barriers[id].Exit(); err != nil {
				t.Fatalf("barrier exit id=%d err=%q", id, err)
			}
			// once unblocked, send a message to ch
			ch <- id
		}(i)
	}

	// kill barrier 0's connection after it has successfully entered
	go func() {
		<-chans[0]
		conns[0].Close()
	}()

	// Now we should have n-1 values, as barriers do work since barrier 0 dropped out
	for i := 0; i < n-1; i++ {
		select {
		case <-ch:
		case <-time.After(zkTimeout * 2):
			t.Fatalf("timed out waiting for barriers to do work")
		}
	}
}
