package zksync

import (
	"fmt"
	"sort"

	"github.com/samuel/go-zookeeper/zk"
)

// Double barriers enable clients to synchronize the beginning and the
// end of a computation. When enough processes have joined the
// barrier, processes start their computation and leave the barrier
// once they have finished.
//
// Double barrier clients register with an ID string which is used to
// know which clients have not started or finished a computation.
//
// Double barrier clients need to know how many client processes are
// participating in order to know when all clients are ready.
type DoubleBarrier struct {
	conn *zk.Conn
	path string
	id   string
	n    int
}

// NewDoubleBarrier creates a DoubleBarrier using the provided
// connection to ZooKeeper. The barrier is registered under the given
// path, and the client will identify itself with the given ID. When
// Enter or Exit are called, it will block until n clients have
// similarly entered or exited.
func NewDoubleBarrier(conn *zk.Conn, path string, id string, n int) *DoubleBarrier {
	return &DoubleBarrier{conn, path, id, n}
}

func (db *DoubleBarrier) Enter() error {
	acl := zk.WorldACL(zk.PermAll)

	if err := createParentPath(db.pathWithID(), db.conn, acl); err != nil {
		return fmt.Errorf("createParentPath err=%q", err)
	}

	_, err := db.conn.Create(db.pathWithID(), []byte{}, zk.FlagEphemeral, acl)
	if err != nil {
		return fmt.Errorf("failed to register err=%q", err)
	}

	others, _, err := db.conn.Children(db.path)
	if err != nil {
		return fmt.Errorf("failed to find children err=%q", err)
	}

	if len(others) >= db.n {
		// mark barrier as complete
		_, err := db.conn.Create(db.path+"/ready", []byte{}, 0, acl)
		if err != nil && err != zk.ErrNodeExists {
			return fmt.Errorf("err creating ready node err=%q", err)
		}
	} else {
		// wait for someone else to mark the /ready node
		for {
			ready, _, ch, err := db.conn.ExistsW(db.path + "/ready")
			if err != nil {
				return fmt.Errorf("err checking existence of ready node err=%q", err)
			}
			if ready {
				break
			}
			<-ch
		}
	}
	return nil
}

func (db *DoubleBarrier) Exit() error {
	for {
		// list remaining processes
		remaining, _, err := db.conn.Children(db.path)
		if err != nil {
			return fmt.Errorf("err finding path=%s  err=%q", db.path, err)
		}

		// filter out the 'ready' node
		processNodes := make([]string, 0)
		for _, znode := range remaining {
			if znode != "ready" {
				processNodes = append(processNodes, znode)
			}
		}

		if len(processNodes) == 0 {
			// shouldn't be possible, but exit anyway
			return nil
		}
		if len(processNodes) == 1 && processNodes[0] == db.id {
			// We're the only process still computing. Delete
			// self and exit.
			_, stat, err := db.conn.Get(db.pathWithID())
			if err != nil && err != zk.ErrNoNode {
				return fmt.Errorf("err finding self err=%q", err)
			}
			if err := db.conn.Delete(db.pathWithID(), stat.Version); err != nil {
				return fmt.Errorf("delete self err=%q", err)
			}
		}
		// There are multiple outstanding processes. Sort them by ID.
		sort.Strings(processNodes)

		// If this is the alphabetically first process, wait for the
		// alphabetically highest one. Else, remove self from db.path,
		// and then wait for a change to the alphabetically first
		// process.
		var waitFor string
		if processNodes[0] == db.id {
			waitFor = processNodes[len(processNodes)-1]
		} else {
			waitFor = processNodes[0]
			if err := deleteIfExists(db.pathWithID(), db.conn); err != nil {
				return fmt.Errorf("delete err=%q", err)
			}
		}

		// If waitFor still exists, lsiten for changes to it. If not,
		// go back to the top of the loop.
		stillExists, _, ch, err := db.conn.ExistsW(db.path + "/" + waitFor)
		if err != nil {
			return fmt.Errorf("existence check err=%q", err)
		}
		if !stillExists {
			continue
		}
		<-ch
	}
}

func (db *DoubleBarrier) pathWithID() string {
	return db.path + "/" + db.id
}
