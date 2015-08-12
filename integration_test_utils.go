package zksync

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/client"
	"github.com/samuel/go-zookeeper/zk"
)

// utilities for integration tests

var (
	// By default, assume we're using the packaged vagrant cluster when running tests
	zookeeperPeers = []string{"192.168.100.67:2181", "192.168.100.67:2182", "192.168.100.67:2183", "192.168.100.67:2184", "192.168.100.67:2185"}
	zkTimeout      = time.Second * 4
	zkPrefix       = "/zksync-test"
	publicACL      = zk.WorldACL(zk.PermAll)

	toxiproxyEnabled = true
	toxiproxyHost    = "192.168.100.67"
	toxiproxyPort    = "8474"
	toxiproxyClient  *toxiproxy.Client
	toxiproxies      []*toxiproxy.Proxy
)

func init() {
	if zookeeperPeersEnv := os.Getenv("ZOOKEEPER_PEERS"); zookeeperPeersEnv != "" {
		zookeeperPeers = strings.Split(zookeeperPeersEnv, ",")
	}

	if toxiproxyAddrEnv := os.Getenv("TOXIPROXY"); toxiproxyAddrEnv != "" {
		hostport := strings.Split(toxiproxyAddrEnv, ":")
		toxiproxyHost = hostport[0]
		toxiproxyPort = hostport[1]
	}

	if err := initToxiproxy(); err != nil {
		toxiproxyEnabled = false
		log.Printf("toxiproxy disabled, err=%q", err)
	}
}

func initToxiproxy() error {
	toxiproxyClient = toxiproxy.NewClient("http://" + toxiproxyHost + ":" + toxiproxyPort)
	n := len(zookeeperPeers)
	toxiproxies = make([]*toxiproxy.Proxy, n)

	currentProxies, err := toxiproxyClient.Proxies()
	if err != nil {
		return err
	}
	toBeCreated := make(map[int]struct{})
	for i := 0; i < n; i++ {
		toBeCreated[i] = struct{}{}
	}
	for name, proxy := range currentProxies {
		for id := range toBeCreated {
			if name == fmt.Sprintf("zk-toxiproxy-%d", id) {
				toxiproxies[id] = proxy
				delete(toBeCreated, id)
			}
		}
	}

	if len(toBeCreated) == 0 {
		// all proxies already exist
		return nil
	}

	// else, go create missing ones
	for id := range toBeCreated {
		zk := zookeeperPeers[id]
		split := strings.Split(zk, ":")
		upstreamPort, _ := strconv.Atoi(split[1])
		upstreamPort = 21800 + upstreamPort%10
		upstream := fmt.Sprintf("localhost:%d", upstreamPort)

		port := rand.Intn(20000) + 20000
		listen := fmt.Sprintf("%s:%d", split[0], port)

		toxiproxies[id] = toxiproxyClient.NewProxy(&toxiproxy.Proxy{
			Name:     fmt.Sprintf("zk-toxiproxy-%d", id),
			Listen:   listen,
			Upstream: upstream,
			Enabled:  true,
		})
		err := toxiproxies[id].Create()
		if err != nil {
			return err
		}
		err = toxiproxies[id].Save()
		if err != nil {
			return err
		}
	}
	// make sure all are enabled
	for _, proxy := range toxiproxies {
		proxy.Enabled = true
		err = proxy.Save()
		if err != nil {
			return err
		}

	}
	return nil
}

func setupZk(t *testing.T) *zk.Conn {
	conn, _, err := zk.Connect(zookeeperPeers, zkTimeout)
	if err != nil {
		t.Fatalf("zk connect err=%q", err)
	}
	conn.SetLogger(testlogger{t})
	return conn
}

// create a zookeeper connection through a toxiproxy. Caller should
// call `defer toxiproxyClient.ResetState()` to clear proxy
// state. calls t.Fatal if any error occurs.
func setupToxicZk(t *testing.T) *zk.Conn {
	proxyAddrs := make([]string, len(toxiproxies))
	for i, tp := range toxiproxies {
		proxyAddrs[i] = tp.Listen
	}

	conn, _, err := zk.Connect(proxyAddrs, zkTimeout)
	if err != nil {
		t.Fatalf("zk toxiproxy connect err=%q", err)
	}
	time.Sleep(1 * time.Second)
	conn.SetLogger(testlogger{t})
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

// implements zk.Logger
type testlogger struct {
	t *testing.T
}

func (tl testlogger) Printf(fmt string, args ...interface{}) {
	tl.t.Logf(fmt, args...)
}
