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
