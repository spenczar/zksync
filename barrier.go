package zksync

import "github.com/samuel/go-zookeeper/zk"

// Bariers block processing on until a condition is met, at which time
// all processes blocked on the barrier are allowed to proceed.
//
// Barriers are implemented by setting a znode in ZooKeeper. If the
// znode exists, the barrier is in place.
type Barrier struct {
	conn *zk.Conn
	path string
}

func NewBarrier(conn *zk.Conn, path string) *Barrier { return &Barrier{conn, path} }

// Set places the barrier, blocking any callers of b.Wait(). Returns
// an error if the barrier exists; callers can handle the
// zk.ErrNodeExists themselves.
func (b *Barrier) Set() error {
	acl := zk.WorldACL(zk.PermAll)

	_, err := b.conn.Create(b.path, []byte{}, 0, acl)
	if err == zk.ErrNoNode {
		err = createParentPath(b.path, b.conn, acl)
		if err != nil {
			return err
		}
		_, err = b.conn.Create(b.path, []byte{}, 0, acl)
	}
	return err
}

// Unset removes the barrier. Returns an error if the barrier does not
// exist; callers can handle the zk.ErrNoNode themselves.
func (b *Barrier) Unset() error {
	_, stat, err := b.conn.Get(b.path)
	if err != nil {
		return err
	}
	return b.conn.Delete(b.path, stat.Version)
}

// Wait blocks until the barrier is removed.
func (b *Barrier) Wait() error {
	for {
		exists, _, ch, err := b.conn.ExistsW(b.path)
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}
		<-ch
	}
}
