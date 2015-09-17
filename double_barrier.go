package zksync

import (
	"fmt"
	"sort"
	"sync"

	"github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"
)

const readyNode = "ready"

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
	acl  []zk.ACL

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewDoubleBarrier creates a DoubleBarrier using the provided
// connection to ZooKeeper. The barrier is registered under the given
// path, and the client will identify itself with the given ID. When
// Enter or Exit are called, it will block until n clients have
// similarly entered or exited. The acl is used when creating any
// znodes.
func NewDoubleBarrier(conn *zk.Conn, path string, id string, n int, acl []zk.ACL) *DoubleBarrier {
	ctx, cancel := context.WithCancel(context.Background())

	return &DoubleBarrier{
		conn:   conn,
		path:   path,
		id:     id,
		n:      n,
		acl:    acl,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Enter joins the computation. It registers this client at the znode,
// and then blocks until all n clients have registered. If the path
// does not exist, then it is created, along with any of its parents
// if they don't exist.
func (db *DoubleBarrier) Enter() (err error) {
	db.wg.Add(1)
	defer db.wg.Done()
	if err := createParentPath(db.pathWithID(), db.conn, db.acl); err != nil {
		return fmt.Errorf("createParentPath path=%q err=%q", db.pathWithID(), err)
	}

	_, err = db.conn.Create(db.pathWithID(), []byte{}, zk.FlagEphemeral, db.acl)
	if err != nil {
		return fmt.Errorf("failed to register path=%q err=%q", db.pathWithID(), err)
	}
	defer func() {
		// clean up if we hit any errors
		if err != nil {
			logWarning("cleaning up dirty enter of barrier - deleting %s", db.pathWithID())
			db.conn.Delete(db.pathWithID(), -1)
		}
	}()

	siblings, _, err := db.conn.Children(db.path)
	if err != nil {
		return fmt.Errorf("failed to find children path=%q err=%q", err, db.path)
	}

	// filter out the ready node
	siblingProcesses := make([]string, 0)
	for _, s := range siblings {
		if s != readyNode {
			siblingProcesses = append(siblingProcesses, s)
		}
	}

	if len(siblingProcesses) > db.n {
		return fmt.Errorf("too many sibling processes: found %d, expected %d", len(siblingProcesses), db.n)
	}

	if len(siblingProcesses) == db.n {
		// mark barrier as complete
		_, err := db.conn.Create(db.path+"/ready", []byte{}, 0, db.acl)
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
			select {
			case <-ch:
			case <-db.ctx.Done():
				return db.conn.Delete(db.pathWithID(), -1)
			}
		}
	}
	return nil
}

// CancelEnter aborts an Enter call and cleans up as it aborts. This
// can be used in conjunction with a timeout to exit early from a
// Double Barrier.
func (db *DoubleBarrier) CancelEnter() {
	db.cancel()
	db.wg.Wait()
}

// Exit reports this client as done with the computation. It
// deregisters this node from ZooKeeper, then blocks until all nodes
// have deregistered.
func (db *DoubleBarrier) Exit() error {
	db.wg.Add(1)
	defer db.wg.Done()
	for {
		// list remaining processes
		remaining, _, err := db.conn.Children(db.path)
		if err == zk.ErrNoNode {
			// barrier is destroyed - this means we are ready to exit
			break
		}
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
			// we're in the middle of teardown - time to exit
			break
		}
		if len(processNodes) == 1 && processNodes[0] == db.id {
			// only one process is left, and its us.  We're the only
			// process still computing. Delete self, delete the
			// barrier, and exit.

			// delete self
			if err := db.conn.Delete(db.pathWithID(), -1); err != nil {
				return fmt.Errorf("delete self err=%q", err)
			}

			// delete 'ready' marker
			if err := db.conn.Delete(db.path+"/ready", -1); err != nil {
				return fmt.Errorf("delete ready err=%q", err)
			}

			// delete barrier
			if err := db.conn.Delete(db.path, -1); err != nil {
				return fmt.Errorf("delete barrier err=%q", err)
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
	return nil
}

func (db *DoubleBarrier) pathWithID() string {
	return db.path + "/" + db.id
}
