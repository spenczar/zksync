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
	if err != nil {
		log.Printf("err finding children of %s", path)
		return err
	}
	for _, child := range children {
		err := recursiveDelete(c, path+"/"+child)
		if err != nil {
			log.Printf("err deleting %s", child)
			return err
		}
	}

	// get version
	_, stat, err := c.Get(path)
	if err != nil {
		log.Printf("err getting version of %s", path)
		return err
	}

	return c.Delete(path, stat.Version)
}

func testPath(testname string) string {
	return zkPrefix + "/" + testname
}
