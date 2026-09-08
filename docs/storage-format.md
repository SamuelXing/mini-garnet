# The storage format

How a key becomes an address, what a record looks like on the log, and why the
log has more watermarks than you would expect. The reasoning behind these
choices is in [design-derivation.md](design-derivation.md).


The log is where the interesting decisions live, so it is worth going slowly.

## Finding a key

```mermaid
flowchart LR
    K["key bytes"] --> H["hash64"]
    H -->|"low bits select"| BSEL["bucket index"]
    H -->|"top 15 bits become"| TSEL["tag"]

    BSEL --> BK

    subgraph BK["one bucket = one 64-byte cache line"]
        direction TB
        E0["entry 0 — tag + 48-bit address"]
        EDOT["entries 1 to 6 — same"]
        E7["entry 7 — overflow pointer + reader/writer latch"]
    end

    E0 -->|"chain head"| R1

    subgraph CH["hash chain, addresses strictly descending"]
        direction LR
        R1["newest record<br/>mutable region"]
        R2["older record<br/>frozen region"]
        R3["older record<br/>on disk"]
        R1 -->|"prev address"| R2 -->|"prev address"| R3
    end
```

Two things fall out of this picture.

The latch shares a cache line with the entries, so taking it costs no extra cache
miss once you have done the lookup. That is why it lives in entry 7 next to the
overflow pointer instead of in the record.

The chain crosses the memory/disk boundary without noticing. A previous-address
field is just a number in one 48-bit space. Whether following it is a pointer
dereference or a disk read is decided by comparing it against `Head`, and nothing
is rewritten when a page is evicted.

## The address space and the region decision

```mermaid
flowchart LR
    D["ON DISK<br/>Begin to Head<br/><br/>read: device IO<br/>write: blind append"]
    I["FROZEN IN MEMORY<br/>Head to SafeReadOnly<br/><br/>read: in place<br/>write: copy to tail, seal old"]
    F["FUZZY<br/>SafeReadOnly to ReadOnly<br/><br/>read: fine<br/>write: retry later"]
    M["MUTABLE<br/>ReadOnly to Tail<br/><br/>read: in place<br/>write: in place, no garbage"]
    D --> I --> F --> M
```

Every operation does the same four things: hash the key, take the bucket latch
(shared to read, exclusive to write), walk the chain to the newest record for that
key, then branch entirely on **where that record's address falls**.

| record lives in | Read | Upsert | Modify (RMW) | Delete |
|---|---|---|---|---|
| mutable | read in place | overwrite in place | update in place | tombstone in place |
| fuzzy | read | blind insert at tail | **retry later** | copy tombstone to tail |
| frozen | read | new record at tail | copy to tail, seal old | copy tombstone to tail |
| on disk | read from device | blind insert | read it, then copy | read it, then tombstone |
| no record | not found | initial write | initial update | not found |

Upsert never reads from disk, because it is blind and does not need the old value.
That single row is why `SET` on a cold key costs no IO while `APPEND` on the same
key does.

The fuzzy column is the one that looks arbitrary. It is not: it is the width of
the epoch handshake, and [the derivation](design-derivation.md#the-fuzzy-region-is-the-epoch-handshake-made-visible)
walks through why it has to exist.

## What a record looks like

```mermaid
flowchart LR
    RI["RecordInfo<br/>8 bytes<br/>one atomic word"]
    DH["data header<br/>16 bytes<br/>key len, value len,<br/>value capacity, flags"]
    EX["expiration<br/>8 bytes<br/>optional"]
    KY["key bytes"]
    VL["value bytes"]
    SP["spare capacity<br/>room to grow in place"]
    RI --> DH --> EX --> KY --> VL --> SP
```

`RecordInfo` is a single 64-bit word so that all of a record's structural state
changes with one atomic store or CAS:

| bits | field | why it exists |
|---|---|---|
| 0-47 | previous address | the hash chain; one 48-bit space covering memory and disk |
| 48 | tombstone | logical delete |
| 49 | valid | cleared while a new record is being filled, and forever if its CAS loses |
| 50 | in new version | written after a checkpoint version bump (CPR) |
| 51 | modified | dirty since the last checkpoint |
| 52 | sealed | this record was copied to the tail; anyone who finds it must retry |

The spare capacity at the end is what makes in-place update possible for growing
values. `INCR` on `99` needs three digits where two were allocated; if the
capacity covers it the update stays in place, and if not the callback returns
false and the engine falls back to copy-to-tail. The command never learns which
happened.

Two independent write paths meet on this record and neither takes a lock on it.
Publishing a new record is one CAS on the hash entry, with the record itself
filled beforehand while marked invalid. Preventing the lost update that this
enables is the `sealed` bit, set on the old record after that CAS succeeds. That
is the whole concurrency story for record contents, in one word.
