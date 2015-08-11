package zksync

import (
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

// utilities for integration tests

var (
	// By default, assume we're using the packaged vagrant cluster when running tests
	zookeeperPeers = []string{"192.168.100.67:2181", "192.168.100.67:2182", "192.168.100.67:2183", "192.168.100.67:2184", "192.168.100.67:2185"}
	zkTimeout      = time.Second * 3
	zkPrefix       = "/zksync-test"
)

func init() {
	if zookeeperPeersEnv := os.Getenv("ZOOKEEPER_PEERS"); zookeeperPeersEnv != "" {
		zookeeperPeers = strings.Split(zookeeperPeersEnv, ",")
	}
}

func setupZk(t *testing.T) *zk.Conn {
	conn, _, err := zk.Connect(zookeeperPeers, zkTimeout)
	if err != nil {
		t.Fatalf("zk connect err=%q", err)
	}
	return conn
}

// recursively delete the testdata. intended to be called in defer,
// and expects all other connections to have been closed already
func cleanup(t *testing.T) {
	conn := setupZk(t)
	err := recursiveDelete(conn, zkPrefix)
	if err != nil {
		t.Fatalf("cleanup err=%q", err)
	}
}

func recursiveDelete(c *zk.Conn, path string) error {
	children, _, err := c.Children(path)
	if err != nil && err != zk.ErrNoNode {
		log.Printf("err finding children of %s", path)
		return err
	}
	for _, child := range children {
		err := recursiveDelete(c, path+"/"+child)
		if err != nil && err != zk.ErrNoNode {
			log.Printf("err deleting %s", child)
			return err
		}
	}

	// get version
	_, stat, err := c.Get(path)
	if err != nil && err != zk.ErrNoNode {
		log.Printf("err getting version of %s", path)
		return err
	}

	if err := c.Delete(path, stat.Version); err != nil && err != zk.ErrNoNode {
		return err
	}
	return nil
}

func testPath(testname string) string {
	if testname == "" {
		return zkPrefix
	}
	return zkPrefix + "/" + testname
}

// return true if f takes longer than timeout to complete, false
// otherwise
func fnTimesOut(f func(), timeout time.Duration) bool {
	ch := make(chan struct{})
	go func() {
		f()
		ch <- struct{}{}
	}()
	select {
	case <-ch:
		return false
	case <-time.After(timeout):
		return true
	}
}

// Safely closes a zookeeper connection - probably won't panic if
// we're not currently connected (races are still possible,
// though). Should be used sparingly.
func quietClose(conn *zk.Conn) {
	if conn.State() != zk.StateDisconnected {
		conn.Close()
	}
}
