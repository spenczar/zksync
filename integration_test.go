package zksync

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/client"
	"github.com/samuel/go-zookeeper/zk"
)

// by default, expect everything to be in the vagrant cluster
const (
	defaultProxyHost = "192.168.100.67"
	defaultProxyPort = "8474"
)

var (
	proxyHost       string
	proxyPort       string
	toxiproxyClient *toxiproxy.Client

	zkPrefix         = "/casey-test"
	zkTimeout        = time.Second * 4
	zookeeperAddrs   = make([]string, 0)
	zookeeperProxies = make([]*toxiproxy.Proxy, 0)
)

func init() {
	SetLogger(log.New(os.Stderr, "[zksync] ", log.LstdFlags|log.Lmicroseconds))

	flag.StringVar(&proxyHost, "proxy-host", defaultProxyHost, "host that proxies connections to Kafka and ZooKeeper for tests")
	flag.StringVar(&proxyPort, "proxy-port", defaultProxyPort, "port that proxies connections to Kafka and ZooKeeper for tests")
	flag.Parse()

	success := false
	retries := 3
	for i := 0; i < retries; i++ {
		if err := initToxiproxy(); err != nil {
			logError("toxiproxy init err=%q", err)
			time.Sleep(1 * time.Second)
		} else {
			success = true
			logInfo("toxiproxy init success")
			break
		}
	}
	if !success {
		logFatal("unable to connect to services. is vagrant up?")
	}
}

func initToxiproxy() error {
	toxiproxyClient = toxiproxy.NewClient("http://" + proxyHost + ":" + proxyPort)
	proxies, err := toxiproxyClient.Proxies()
	if err != nil {
		return err
	}
	for name, proxy := range proxies {
		// toxiproxy thinks its listening on [::] since its inside
		// vagrant - adjust the host to point to the vagrant cluster
		// instead
		port := strings.TrimPrefix(proxy.Listen, "[::]:")
		hostport := fmt.Sprintf("%s:%s", proxyHost, port)
		if strings.HasPrefix(name, "zk") {
			zookeeperAddrs = append(zookeeperAddrs, hostport)
			zookeeperProxies = append(zookeeperProxies, proxy)
		}
	}
	return nil
}

// connect to one zookeeper
func connectZk(t *testing.T, idx int) *zk.Conn {
	conn, _, err := zk.Connect([]string{zookeeperAddrs[idx]}, zkTimeout)
	if err != nil {
		t.Fatalf("zk connect err=%q", err)
	}
	conn.SetLogger(discardLogger{})
	return conn
}

// connect to all 5 zookeepers
func connectAllZk(t *testing.T) *zk.Conn {
	conn, _, err := zk.Connect(zookeeperAddrs, zkTimeout)
	if err != nil {
		t.Fatalf("zk connect err=%q", err)
	}
	conn.SetLogger(discardLogger{})
	return conn
}

// recursively delete the testdata. intended to be called in defer,
// and expects all other connections to have been closed already
func cleanup(t *testing.T) {
	conn := connectAllZk(t)
	err := recursiveDelete(conn, zkPrefix)
	if err != nil {
		t.Fatalf("cleanup err=%q", err)
	}
	conn.Close()
}

func recursiveDelete(c *zk.Conn, path string) error {
	children, _, err := c.Children(path)
	if err != nil && err != zk.ErrNoNode {
		logError("err finding children of %s", path)
		return err
	}
	for _, child := range children {
		err := recursiveDelete(c, path+"/"+child)
		if err != nil && err != zk.ErrNoNode {
			logError("err deleting %s", child)
			return err
		}
	}

	// get version
	_, stat, err := c.Get(path)
	if err != nil && err != zk.ErrNoNode {
		logError("err getting version of %s", path)
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
	return zkPrefix + testname
}

// Safely closes a zookeeper connection - probably won't panic if
// we're not currently connected (races are still possible,
// though). Should be used sparingly.
func quietClose(conn *zk.Conn) {
	if conn.State() != zk.StateDisconnected {
		conn.Close()
	}
}

// implements zk.Logger, discarding log lines
type discardLogger struct{}

func (l discardLogger) Printf(fmt string, args ...interface{}) {}
