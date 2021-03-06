package zksync

import (
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/samuel/go-zookeeper/zk"
)

type lockType int

const (
	invalidLock lockType = iota
	readLock
	writeLock
	anyLock
)

func (t lockType) String() string {
	switch t {
	case invalidLock:
		return "invalid-lock"
	case readLock:
		return "read-lock"
	case writeLock:
		return "write-lock"
	case anyLock:
		return "lock"
	default:
		return "unknown-lock"
	}
}

var (
	ErrMalformedLock = errors.New("not a valid lock")
	ErrNoLockPresent = errors.New("not currently holding a lock")

	publicACL = zk.WorldACL(zk.PermAll)
)

// RWMutex provides a read-write lock backed by ZooKeeper. Multiple
// clients can use the lock as long as they use the same path on the
// same ZooKeeper ensemble.
//
// Access is provided on a first-come, first-serve basis. Readers are
// granted the lock if there are no current writers (so reads can
// happen concurrently). Writers are only granted the lock
// exclusively.
//
// It is important to release the lock with `Unlock`, of course.
//
// In case of an unexpected disconnection from ZooKeeper, any locks
// will be released because the Session Timeout will expire, but its
// the caller's responsibilty to halt any computation in this
// case. This can be done by listening to the Event channel provided
// by zk.Connect (https://godoc.org/github.com/samuel/go-zookeeper/zk#Connect).
//
// RWMutexes are not safe for shared local use across goroutines.
type RWMutex struct {
	conn *zk.Conn
	path string
	acl  []zk.ACL

	curLock     string
	curLockType lockType
}

// NewRWMutex creates a new RWMutex object. It doesn't actually
// perform any locking or communicate with ZooKeeper in any way. If
// the path does not exist, it will be created when RLock or WLock are
// called, as will any of its parents, using the provided ACL.
func NewRWMutex(conn *zk.Conn, path string, acl []zk.ACL) *RWMutex {
	return &RWMutex{conn, path, acl, "", 0}
}

// RLock acquires a read lock on a znode. This will block if there are
// any write locks already on that znode until the write locks are
// released.
func (m *RWMutex) RLock() error {
	return m.lock(readLock)
}

// WLock acquires a write lock on a znode. This will block if there
// are any read or write locks already on that znode until those locks
// are released.
func (m *RWMutex) WLock() error {
	return m.lock(writeLock)
}

// Unlock releases the lock. Returns an error if not currently holding
// the lock.
func (m *RWMutex) Unlock() error {
	if m.curLock == "" {
		return ErrNoLockPresent
	}
	err := m.conn.Delete(m.curLock, -1)
	if err != nil {
		return err
	}
	m.curLock = ""
	m.curLockType = 0
	return nil
}

func (m *RWMutex) lock(t lockType) error {
	// register our lock
	created, err := m.createLock(t)
	if err != nil {
		logError("failed to create lock, err=%q", err)
		return err
	}

	// figure out what number we got from zk
	seq, err := parseSequenceNumber(created)
	if err != nil {
		return err
	}

	// see if there are any other locks with lower sequence numbers
	// than us - we have to wait for them
	var blockedBy lockType

	// writes are blocked by reads and writes. reads are only blocked
	// by writes
	if t == writeLock {
		blockedBy = anyLock
	} else {
		blockedBy = writeLock
	}

	for {
		lowest, lowestSeq, err := m.lowestSeq(blockedBy)
		if err != nil {
			return err
		}
		if seq <= lowestSeq {
			// nothing to wait for - we're free!
			logTrace("%s %s is unblocked - proceeding", t, created)
			break
		}

		// listen for changes to the lowest write's path
		logTrace("%s %s blocked by %s", t, created, lowest)
		err = m.wait(lowest)
		if err != nil {
			return err
		}
	}
	m.curLock = created
	m.curLockType = t
	logTrace("successfully created %s %s", t, m.curLock)
	return nil
}

func (m *RWMutex) createLock(t lockType) (string, error) {
	var path string
	if t == writeLock {
		path = m.path + "/write-"
	} else if t == readLock {
		path = m.path + "/read-"
	}

	created, err := m.conn.CreateProtectedEphemeralSequential(path, []byte{}, m.acl)
	if err == zk.ErrNoNode {
		err := createParentPath(path, m.conn, m.acl)
		if err != nil {
			return "", err
		}
		return m.conn.CreateProtectedEphemeralSequential(path, []byte{}, m.acl)
	} else {
		return created, err
	}
}

// find the lowest lock of specified type at m.path, returning its
// path and its sequence number
func (m *RWMutex) lowestSeq(t lockType) (string, int, error) {
	children, _, err := m.conn.Children(m.path)
	if err != nil {
		return "", 0, err
	}

	// find the write lock with the lowest sequence number
	var (
		lowestPath string
		lowestSeq  int = math.MaxInt32
	)
	for _, path := range children {
		if t == anyLock || parseLockType(path) == t {
			thisSeq, err := parseSequenceNumber(path)
			if err != nil {
				return "", 0, err
			}
			if thisSeq < lowestSeq {
				lowestPath = path
				lowestSeq = thisSeq
			}
		}
	}
	return m.path + "/" + lowestPath, lowestSeq, nil
}

// watch the given path and block. does not error if the path doesn't
// exist.
func (m *RWMutex) wait(path string) error {
	exists, _, ch, err := m.conn.ExistsW(path)
	if err != nil {
		return err
	}

	if !exists {
		return nil
	}

	// we're sure lock exists - wait ZK to tell us it has changed
	ev := <-ch
	if ev.Err != nil {
		return ev.Err
	}
	return nil
}

func parseSequenceNumber(path string) (int, error) {
	parts := strings.Split(path, "/")
	end := parts[len(parts)-1]

	if idx := strings.LastIndex(end, "read-"); idx != -1 {
		idx += len("read-")
		return strconv.Atoi(end[idx:])

	} else if idx := strings.LastIndex(end, "write-"); idx != -1 {
		idx += len("write-")
		return strconv.Atoi(end[idx:])

	} else {
		return -1, ErrMalformedLock
	}
}

func parseLockType(path string) lockType {
	if strings.Index(path, "read-") != -1 {
		return readLock
	}
	if strings.Index(path, "write-") != -1 {
		return writeLock
	}
	return invalidLock
}
