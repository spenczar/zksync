# zksync #

[![GoDoc](http://godoc.internal.justin.tv/code.justin.tv/spencer/zksync?status.svg)](http://godoc.internal.justin.tv/code.justin.tv/spencer/zksync)

zksync provides a set of synchronization primitives like what you'd
find in the official [sync](http://golang.org/pkg/sync/) package.

## RWMutex ##

`RWMutex` provides a read-write lock. Readers can share concurrent
access as long as a writer hasn't claimed the lock. Writers must be
exclusive, and will block on any other lock holder.

Lock acquisition is handled on a first-come, first-serve basis, with
ordering determined by the ZooKeeper server.

Locks are stored as ephemeral znodes, so if a client disconnects from
ZooKeeper unexpectedly, the lock will be released within the session
timeout that the client established when they connected.

## Barrier ##

`Barrier` provides a shared barrier for multiple clients. When the
barrier is set, any call to `Barrier.Wait` will block any client of
the barrier. A call to `Barrier.Unset` will unblock all clients.

Setting the barrier is an inherently racy process; clients can
optimistically call `Barrier.Set` and handle `zk.ErrNodeExists` if the
barrier is already in place.

Barriers are not ephemeral, since a disconnect from the barrier-setter
should not obviously cause all other clients to proceed. This means
that any client has permission to remove the barrier.


# Development #

Run tests with `GOMAXPROCS=4 godep go test -timeout 10s ./...` (or
some other value of GOMAXPROCS, of course - the point is, you want
parallelism).

Tests require a working ZooKeeper cluster. You can set this two ways:
 1. Use Vagrant. `vagrant up` will launch a VM with a 5-node ZooKeeper
    ensemble listening on 192.168.100.67, ports 2181
    through 2185. This is the assumed default in tests.
 2. Set the environment variable `ZOOKEEPER_PEERS` to a
    comma-separated list of ZooKeeper hostports, like so:
    ```
    $ ZOOKEEPER_PEERS=localhost:2181,localhost:2182 GOMAXPROCS=4 godep go test -timeout 10s ./...
    ```

The tests will all be run under a namespace, `/zksync-test`, and will
delete everything under that node after each test run.
