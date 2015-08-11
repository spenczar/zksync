package zksync

import (
	"strings"

	"github.com/samuel/go-zookeeper/zk"
)

// create the parent znodes up to path. does not error if any of the
// parent znodes exist.
func createParentPath(path string, conn *zk.Conn, acl []zk.ACL) error {
	parts := strings.Split(path, "/")
	prePath := ""
	for _, p := range parts[1 : len(parts)-1] {
		prePath += "/" + p
		_, err := conn.Create(prePath, []byte{}, 0, acl)
		if err != nil && err != zk.ErrNodeExists {
			return err
		}
	}
	return nil
}

func deleteIfExists(path string, conn *zk.Conn) error {
	_, stat, err := conn.Get(path)
	if err != nil && err != zk.ErrNoNode {
		return err
	}
	if err != zk.ErrNoNode {
		if err := conn.Delete(path, stat.Version); err != nil {
			return err
		}
	}
	return nil
}
