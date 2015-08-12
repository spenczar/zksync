# zksync #

[![GoDoc](http://godoc.internal.justin.tv/code.justin.tv/spencer/zksync?status.svg)](http://godoc.internal.justin.tv/code.justin.tv/spencer/zksync)

zksync provides a go implementation the synchronization primitives
that you'll find in the
[ZooKeeper documentation](http://zookeeper.apache.org/doc/r3.1.2/recipes.html):
locks and barriers. These can be used to coordinate computation across
multiple processes.


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


## Double Barriers ##

`Double Barriers` allow clients to synchronize start and end times of
a computation. All participants in the double barrier should agree on
the _number_ of participants ahead of time. Then, they each call
`DoubleBarrier.Enter()` and block until all participants have called
that function and written their data in ZooKeeper.

When a client finishes its computation, it calls
`DoubleBarrier.Exit()`, which will block until all clients have either
called `DoubleBarrier.Exit()` or disconnected from ZooKeeper.

_All clients must agree on the number of participants throughout the
entry process_. If the group is expecting 5 participants, and then
only 4 successfully call `Enter`, then those 4 will all block forever.

# Development #

Run tests with `GOMAXPROCS=4 godep go test -timeout 10s ./...` (or
some other value of GOMAXPROCS, of course - the point is, you want
parallelism).

Tests require a working ZooKeeper cluster. You can set this in either
of two ways:
 1. Use Vagrant. `vagrant up` will launch a VM with a 5-node ZooKeeper
    ensemble listening on 192.168.100.67, ports 2181
    through 2185. This is the assumed default in tests.
 2. Set the environment variable `ZOOKEEPER_PEERS` to a
    comma-separated list of ZooKeeper hostports, like so:
    ```
    $ ZOOKEEPER_PEERS=localhost:2181,localhost:2182 GOMAXPROCS=4 godep go test -timeout 10s ./...
    ```
    
The vagrant ZooKeeper cluster uses
[Toxiproxy](https://github.com/Shopify/toxiproxy) to simulate network
failures. By default, tests will attempt to connect to
192.168.100.67:8474 to talk to Toxiproxy. You can override this by
setting a `TOXIPROXY` environment variable to your preferred
hostport. If unable to connect to Toxiproxy, then tests which rely
upon it will be skipped.

Either way, the tests will all be run under a namespace,
`/zksync-test`, and will delete everything under that node after each
test run.
