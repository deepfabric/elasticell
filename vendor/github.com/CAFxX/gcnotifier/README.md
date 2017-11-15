# gcnotifier

gcnotifier provides a way to receive notifications after every run of the
garbage collector (GC). Knowing when GC runs is useful to instruct your code to
free additional memory resources that it may be using.

[![GoDoc](https://godoc.org/github.com/CAFxX/gcnotifier?status.svg)](http://godoc.org/github.com/CAFxX/gcnotifier)
[![GoCover](http://gocover.io/_badge/github.com/CAFxX/gcnotifier)](http://gocover.io/github.com/CAFxX/gcnotifier)

## Why?
Package gcnotifier provides a way to receive notifications after every
garbage collection (GC) cycle. This can be useful, in long-running programs,
to instruct your code to free additional memory resources that you may be
using.

A common use case for this is when you have custom data structures (e.g.
buffers, rings, trees, pools, ...): instead of setting a maximum size to your
data structure you can leave it unbounded and then drop all (or some) of the
allocated-but-unused slots after every GC run (e.g. sync.Pool drops all
allocated-but-unused objects in the pool during GC).

To minimize the load on the GC the code that runs after receiving the
notification should try to avoid allocations as much as possible, or at the
very least make sure that the amount of new memory allocated is significantly
smaller than the amount of memory that has been "freed" by your code.

GCNotifier guarantees to send a notification after every GC cycle completes.
Note that the Go runtime does not guarantee that the GC will run:
specifically there is no guarantee that a GC will run before the program
terminates.

## How to use it
For a simple example of how to use it have a look at `Example()` in
[gcnotifier_test.go](gcnotifier_test.go). For details have a look at the
[documentation](https://godoc.org/github.com/CAFxX/gcnotifier).

## How it works
gcnotifier uses [finalizers][SetFinalizer] to know when a GC run has completed.

Finalizers are run [when the garbage collector finds an unreachable block with
an associated finalizer][SetFinalizer].

The `SetFinalizer` documentation notes that [there is no guarantee that
finalizers will run before a program exits][SetFinalizer]. This doesn't mean, as
sometimes incorrectly understood, that finalizers are not guaranteed to run *at
all*, it just means that they are not guaranteed to run because GC itself is not
guaranteed to run in certain situations: e.g. when the runtime is shutting down.
Finalizers can also not run for other reasons (e.g. zero-sized or
package-level objects) but they don't apply to `gcnotifier` because care was
taken in the implementation to avoid them.

The only other case in which a notification will not be sent by gcnotifier is if
your code hasn't consumed a previously-sent notification. In all other cases if
a GC cycle completes your code will receive a notification.

The test in [gcnotifier_test.go](gcnotifier_test.go) generates garbage in a loop
and makes sure that we receive exactly one notification for each of the first
500 GC runs. In my testing I haven't found a way yet to make gcnotifier fail to
notify of a GC run short of shutting down the process or failing to receive the
notification. If you manage to make it fail in any other way please file a
[GitHub issue](https://github.com/CAFxX/gcnotifier/issues/new).

# License
[MIT](LICENSE)

# Author
Carlo Alberto Ferraris ([@cafxx](https://twitter.com/cafxx))


[SetFinalizer]: https://golang.org/pkg/runtime/#SetFinalizer
