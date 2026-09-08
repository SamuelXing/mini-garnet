# Deriving Garnet's design from first principles

Notes I took while reimplementing Garnet and Tsavorite. The interesting question
is not what Garnet does but why it ends up looking like this. Almost every piece
follows from a small number of forced moves.

Everything here is my reading of the VLDB 2026 paper
([PDF](https://www.vldb.org/pvldb/vol19/p224-chandramouli.pdf)) plus what I learned
building [the model in this repo](../README.md).

## Start with where the time goes

A cache-store's job is to answer point lookups over a network. The hash lookup
costs about 100ns. The network round trip costs tens of microseconds. So the
server has exactly two levers worth pulling: amortize the round trip over many
commands (batching), and use all the cores you are paying for.

Redis pulls the first lever and deliberately declines the second. Single-threaded
execution buys an enormous amount of simplicity — no locks, no memory ordering, no
concurrent data structures, and every command is trivially atomic. On a four-core
box in 2010 this was close to free. On a 72-core cloud VM it means the data path
uses one core and idles seventy-one.

The usual workarounds each reintroduce a cost. Put a global lock around the hash
table (KeyDB) and you get contention. Shard the keyspace across worker threads
(Dragonfly, Redis Enterprise) and every batch has to be split by owner, routed,
and its replies collated back into FIFO order, which also collapses under skew.
Run 64 processes and let the client route (intra-node cluster) and you get a
quadratic number of connections and fewer commands per batch. The paper measures
all four against the same hardware (§8.5): 47 Mops/s for Garnet, 17 for 64
processes, 9 for 64 worker threads, 4.4 for one worker queue, 1.3 for a global
lock.

**First forced move: if you want the whole machine, the data structure has to be
shared and concurrent, not partitioned.** Partitioning moves the request to the
data. Sharing lets cache coherence move the data to the request, and on a modern
machine that is fast and needs no routing decision.

## Sharing costs you memory safety, so buy it back cheaply

Once every thread can touch every record, the hard problem is not mutual
exclusion. It is reclamation. A thread holding a pointer into a page can be
racing another thread that wants to evict that page, resize the index, or free a
record. Reference counting or hazard pointers on every access would cost more
than the lookup itself.

The answer is epoch protection. A thread publishes "I am working in epoch E" into
its own cache line on entry and clears it on exit. Anything you want to retire is
tagged with the current epoch and freed only once every thread has moved past it.
Fast-path cost: one store in, one store out.

This is not an implementation detail, it is the enabling primitive. Once you have
it, every expensive-but-rare operation gets the same shape: change a boundary,
then do the dangerous part only after the epoch drains. Garnet leans on this so
hard that it names the pattern — the epoch-protected state machine — and reuses
it for page flushing, page eviction, index resize, checkpoints, and shard
migration. Four different subsystems, one mechanism.

## Point lookups that spill to disk want a log, not a tree

Requirement: larger than memory, but optimized for the case where the working set
fits in memory.

A B-tree or LSM (RocksDB, LeanStore) supports range queries and pays for them
with reordering, compaction, node consolidation, and a traversal per read. A RESP
cache-store never scans a key range. You would be buying something you never use.

The right shape is a hash index over a log. The index maps a key hash to a 48-bit
logical address; the log is one append-only address space whose low end lives on
disk and whose high end lives in a circular buffer of pages in memory.

That single address space is the whole trick. Evicting a page changes nothing in
the index — the addresses still point at the same records, they just happen to be
on disk now, and a lookup that walks below the head address issues a read instead
of a dereference. No pointer rewriting, no eviction bookkeeping.

## A pure log has space amplification, so make its tail mutable

Append-only means a counter incremented a million times writes a million records.
LSMs answer this with compaction, which costs write amplification and latency
spikes.

Tsavorite answers it by splitting the in-memory portion in two. Near the tail is a
**mutable region** where records are updated in place, no append and no garbage.
Below it is a **read-only region** where records are frozen, so an update becomes
read-copy-update: allocate at the tail, write the new version, repoint the hash
chain.

This is the most important decision in the engine. The hot working set behaves
like an in-memory hash table with in-place updates, while the cold tail behaves
like a log-structured store that tiers to disk, and it is one data structure.

## The fuzzy region is the epoch handshake made visible

Here is the detail I found most illuminating, because it looks arbitrary until you
see where it comes from.

The log has more boundaries than you would expect:

```
Begin ≤ ClosedUntil ≤ SafeHead ≤ Head ≤ FlushedUntil ≤ SafeReadOnly ≤ ReadOnly ≤ Tail
```

Why two read-only markers? Because moving a boundary is not instantaneous. When
you advance `ReadOnly` to freeze more of the log, threads that already read the
*old* value may still be mid in-place-update below the new line. You cannot flush
those pages yet — you would write a torn record.

So you publish `ReadOnly` optimistically, bump the epoch, and only when the epoch
drains do you set `SafeReadOnly` and start flushing. Every such pair is
"value I published" and "value everyone has now observed", and the gap between
them is the handshake in flight. `Head`/`SafeHead` is the same pair for eviction.

The gap `[SafeReadOnly, ReadOnly)` is the **fuzzy region**, and it explains an
otherwise strange asymmetry: reads there are fine, but an update must back off and
retry. An in-place update racing a copy-to-tail would be silently lost, and the
engine cannot tell which is happening, so it declines to guess.

## Lost updates need one bit, not a lock

Structure changes are latch-free. To add a record: reserve space at the tail with
a fetch-and-add, fill it while it is marked invalid and sealed, then one CAS on the
hash entry publishes it and prepends it to the chain. A failed CAS means someone
beat you; retry.

But there is still a race the CAS does not cover. Thread A copies a record to the
tail while thread B updates the old copy in place. Both succeed. B's write is lost.

The fix is one bit: after A's CAS succeeds, it **seals** the old record. Any thread
that finds a sealed record retries instead of trusting it. That is the entire
lost-update story, and it costs a bit in a header word you already loaded.

Reading and writing record *contents* does need mutual exclusion, but the
placement is clever. The latch lives in the hash bucket, specifically in the
eighth entry, which also holds the overflow-bucket pointer. You already pulled
that cache line in to do the lookup, so the lock is free in cache-miss terms.

And the latch is try-only with bounded spinning. On failure the operation returns
"retry later", which makes the caller refresh its epoch. This rule matters more
than it looks: a thread spinning on a lock while holding an epoch would stall
reclamation for the whole process.

## 250 commands cannot each know about all of this

RESP has more than 250 commands and the list grows. If every command author has to
understand sealing, the fuzzy region, and page eviction, nobody contributes and
nothing is maintainable. But a fat generic interface loses the ability to update in
place, which is the point of the mutable region.

The resolution is a narrow waist: five operations — Read, Upsert, Modify, Delete,
Scan — plus callbacks. The store decides **where** an operation happens. The
command decides **what the bytes become**.

`INCRBY` never learns whether it is incrementing a record in the mutable region or
one that just came back from disk. It implements `in_place_updater` ("add the
delta; return false if the answer needs more digits than fit") and `copy_updater`
("write old + delta into this fresh allocation"), and the engine picks.

The callback set is more subtle than it first appears. `NeedInitialUpdate` and
`NeedCopyUpdate` exist so a command can veto *before* space is allocated, and they
are the entire implementation of some commands: `SETNX` is `need_copy_update →
false`, `SETXX` is `need_initial_update → false`. That is the whole semantics, with
no storage code involved.

## Durability without stopping the world

A consistent checkpoint traditionally requires quiescing. Redis forks, which in the
worst case doubles the memory footprint and stalls badly.

Garnet adapts Concurrent Prefix Recovery: use the epoch state machine to move the
database from version `v` to `v+1`; once every thread is in `v+1` and a `v+1`
thread is forbidden from updating a `v` record in place, everything of version `v`
below the captured tail is a consistent prefix. Flush that. Nobody blocks. The
paper's number is 1.06s versus 273s for Valkey on 256M keys, saturating an 8-SSD
RAID.

For the write log there is a second good idea. Log at the narrow waist rather than
at the wire, and capture all non-determinism — timestamps, random numbers — into
the logged input. Replay then re-executes deterministically, which means the same
records expire on replay as expired originally. That log doubles as the
replication stream.

## The network layer falls out of the concurrency model

Because the store is shared and thread-safe, there is no owner thread to route to.
So the buffer the kernel filled can be parsed, executed, and answered on the very
thread that received it. No queue, no handoff, no collation, and the session's
working set stays in L1/L2.

Since a RESP session is FIFO and clients pipeline, one receive buffer holds many
commands and one send buffer holds all their replies: one read syscall in, one
write syscall out, for potentially hundreds of operations.

## Compared to Redis

The honest summary is that Redis optimizes for the simplicity of one thread, and
Garnet pays for concurrency once, inside a storage engine, then spends the proceeds.

What that buys, with the paper's measurements:

- **Throughput.** Up to 100x at high session counts, because all cores serve the
  same store instead of one core serving it.
- **Tail latency.** Around 4x lower at p99.9 under load, and lower still with
  kernel bypass, because a request is never queued or handed between threads.
- **Larger than memory.** The working set stays in RAM and the rest tiers to SSD
  or cloud storage through the same address space. Redis has no answer here.
- **Checkpoints.** 1.06s versus 273s on 256M keys, and no fork, so no memory
  spike and no stall.
- **Extensibility.** New commands are callbacks against five operations, in a
  memory-safe language, with no exposure to the storage internals.

What Redis still does better, and the paper does not pretend otherwise: it is
simpler, its per-key memory overhead is lower (a log record carries a header, and
log structure costs space until compaction reclaims it), its command surface is
far broader and battle-tested, and single-threaded execution means the data
structures never need concurrency reasoning at all.

The tradeoff is legible: Garnet is what you build when the machine is big, the
dataset does not fit, and you want durability without a stall. It is not what you
build when you want 3,000 lines of C that anyone can read in an afternoon.

