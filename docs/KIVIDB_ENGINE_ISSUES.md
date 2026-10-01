# kividb engine issues found while testing the operator

Found while running kividb-operator 0.4.0 against **kividb v1.0.4**
(`quay.io/kividbio/kividb@sha256:6bab772968654ecd357975a4f36f9e7d262ac7765be386d86170191eef1d8959`)
on a single-node minikube cluster, in plain Docker containers, and on a
three-node, three-zone EKS cluster, on 2026-10-01 and 2026-10-02. Each entry says what was observed, how to reproduce it without
the operator, what Redis does in the same situation, and what the operator
does about it today.

| # | Issue | Severity | Operator workaround |
|---|---|---|---|
| [1](#1-a-password-on-the-default-user-in-an-acl-file-is-not-enforced) | Password on `default` in an ACL file is not enforced | Critical | None possible (see 2) |
| [2](#2-a-replica-cannot-authenticate-to-its-master) | A replica cannot authenticate to its master | High | None |
| [3](#3-a-snapshot-that-cannot-be-read-is-skipped-and-the-server-starts-empty) | Unreadable snapshot is skipped; server starts empty | High | Restore Job fixed; engine behaviour unchanged |
| [4](#4-a-replica-reports-itself-in-sync-while-it-is-still-doing-its-first-full-sync) | Replica reports "connected" during its first full sync | High | Offset heuristic |
| [5](#5-acl-load-merges-into-existing-users-instead-of-replacing-them) | `ACL LOAD` merges instead of replacing | High | `reset` in every line; agent deletes stale users |
| [6](#6-only-the-last-key-pattern-of-a-user-is-kept) | Only the last key pattern of a user is kept | Medium | None |
| [7](#7-acl-setuser-and-acl-deluser-report-an-error-after-applying-the-change) | `ACL SETUSER` / `DELUSER` return an error after applying the change | Medium | Agent re-reads the user list |
| [8](#8-the-config-file-silently-ignores-what-it-does-not-understand) | Config file silently ignores unknown directives and bad values | Medium | None |
| [9](#9-a-full-resync-needs-about-twice-the-datasets-memory-and-happens-on-every-replica-restart) | Full resync needs ~2x memory and runs on every replica restart | Medium | None |
| [10](#10-a-full-resync-that-fails-part-way-leaves-the-replica-empty) | A full resync that fails part-way leaves the replica empty | Critical | Failover refuses to promote an emptied replica |
| [11](#11-the-final-snapshot-on-sigterm-is-sometimes-not-written) | The final snapshot on SIGTERM is sometimes not written | High | Master hands over before it is restarted |

Issues 1 and 10 are the ones to look at first. Issue 1 means **every
cluster that relies on a `default`-user password for access control is
open to anyone who can reach its port** (and issue 2 is what stops the
operator from closing it). Issue 10 **lost an entire dataset** in testing:
every replica dropped its copy at the same moment.

---

## 1. A password on the `default` user in an ACL file is not enforced

**Severity: critical.**

When kividb is started with `--aclfile` and no `--requirepass`, a
connection that never sends `AUTH` is treated as the `default` user and
can run every command that user is allowed, even though the ACL file gives
`default` a password. A connection whose `AUTH` *failed* behaves the same
way: the error is returned and the next command runs as `default`.

The operator's ROADMAP described this as "unauthenticated `PING` returns
`PONG`". It is not limited to `PING`.

### Reproduce

```bash
mkdir acl
printf 'user default on #%s ~* &* +@all\n' "$(printf secret | sha256sum | cut -d' ' -f1)" > acl/users.acl
docker run -d --name kv -v "$PWD/acl:/acl" quay.io/kividbio/kividb:v1.0.4 --aclfile /acl/users.acl

docker exec kv redis-cli -p 6380 set k v          # OK
docker exec kv redis-cli -p 6380 acl whoami       # default
docker exec kv redis-cli -p 6380 acl list         # prints every user and password hash
docker exec kv redis-cli -p 6380 -a wrong get k   # "AUTH failed: WRONGPASS ...", then: v
```

### Expected

`NOAUTH Authentication required.` for everything except `AUTH`/`HELLO`
until the connection authenticates, exactly as kividb already does when
`--requirepass secret` is given. In Redis, `user default on #<hash> ...`
in the ACL file is equivalent to `requirepass`.

### Impact on the operator

A `KividbAclConfig` whose `default` user has a password (or that sets
`requirePassSecretRef`) renders exactly this ACL file. Confirmed on a
live operator-managed cluster: an unauthenticated client wrote through the
`<cluster>-master` Service. Passwords on **non-default** users are
enforced when a client authenticates as them, but nothing forces a client
to authenticate at all.

The operator cannot work around it by also passing `--requirepass`,
because of issue 2.

---

## 2. A replica cannot authenticate to its master

**Severity: high** (blocks the only mitigation for issue 1).

There is no `masterauth` / `masteruser` option (flag, config directive or
`CONFIG SET`). If the master enforces authentication with `--requirepass`,
a replica's handshake is rejected and it retries forever.

### Reproduce

```bash
docker network create kvnet
docker run -d --name kvm --network kvnet quay.io/kividbio/kividb:v1.0.4 --requirepass secret
docker run -d --name kvr --network kvnet quay.io/kividbio/kividb:v1.0.4 --requirepass secret
docker exec kvr redis-cli -p 6380 -a secret replicaof kvm 6380   # OK
docker logs kvr | grep REPL
# [REPL] Replication error: Expected PONG, got: -NOAUTH Authentication required. — reconnecting in 5s
docker exec kvr redis-cli -p 6380 -a secret role                 # master 0
```

Two further things are visible in the last line: `REPLICAOF` answered `OK`
although replication never started, and `ROLE` on the would-be replica
reports `master` while it is retrying.

### Expected

`masterauth <password>` and `masteruser <name>` as in Redis, settable at
startup and with `CONFIG SET`, so the replica sends `AUTH` before `PING`
in the handshake. While the link is down the node should still report
`slave` with state `connect`/`connecting`.

---

## 3. A snapshot that cannot be read is skipped and the server starts empty

**Severity: high** (silent data loss).

If `dump.kdb` exists in the working directory but cannot be opened, kividb
logs a warning and continues with an empty dataset. The same happens for
`appendonly.aof`. The server then accepts writes, replicas full-sync the
empty dataset, and the next `SAVE`/`BGSAVE` replaces the file it could not
read.

### Observed

A pod whose `/data/dump.kdb` was mode `0600` and owned by a different UID
(written by the operator's restore Job, since fixed):

```
[INFO] Found dump.kdb — loading native snapshot
[WARN] Could not load snapshot: Permission denied (os error 13)
Failed to open AOF: Os { code: 13, kind: PermissionDenied, message: "Permission denied" }
[INFO] Saving snapshot → /data/repl_fullsync.kdb.tmp
```

The pod became Ready with `DBSIZE` 0 and served as the cluster's master.
This was observed in Kubernetes; a reduced Docker-only reproduction was
not obtained, so the exact conditions (permission error versus a corrupt
file) should be checked on the engine side.

### Expected

Refuse to start (non-zero exit) when a persistence file is present but
cannot be read or parsed, as Redis does. An empty database is only the
right outcome when no file exists.

---

## 4. A replica reports itself in sync while it is still doing its first full sync

**Severity: high** for anything that orchestrates restarts or failover.

From the moment `REPLICAOF` is accepted, and for as long as the snapshot
is being transferred and loaded, both sides report a healthy, caught-up
link:

| Where | Reports during the full sync |
|---|---|
| Replica `ROLE` | `slave <host> <port> connected 0` |
| Replica `INFO replication` | `master_link_status:up`, `master_sync_in_progress:0` |
| Master `INFO replication` | `slave0:...,state=online,offset=0,lag=<master offset>` |
| Replica `DBSIZE` | `0` (or its stale pre-sync count) until the load completes |

The only thing that changes when the sync finishes is the replica's
offset, which jumps from 0 to the master's.

### Reproduce

Load about a million keys into a master, start an empty second node,
send it `REPLICAOF`, and poll both nodes a few times per second. With a
213 MB snapshot the window was several seconds.

### Expected

As in Redis: replica state `sync` (not `connected`) and
`master_link_status:down` / `master_sync_in_progress:1` until the dataset
is loaded; master-side `state=wait_bgsave` then `send_bulk` then `online`.
A `loading:1` flag in `INFO persistence` while a snapshot is being loaded
would also help.

### Impact on the operator

The readiness probe passes during the sync, so a replica joins the
read Service and counts as "restarted" before it holds the data. The
operator now infers sync completion from the offsets (replica offset
greater than 0 and close to the master's), and falls back to comparing key
counts when the master's own offset is 0. Both are heuristics that a real
state field would replace.

---

## 5. `ACL LOAD` merges into existing users instead of replacing them

**Severity: high** (revoked credentials keep working).

`ACL LOAD` applies each line of the file on top of the user as it already
exists in memory:

- a changed password is **added**; the previous password still works;
- a removed key pattern or command rule is not taken away;
- a user that is no longer in the file is not deleted.

### Reproduce

```bash
# acl/users.acl: default nopass, app with password pw1, gone with password pw1
docker run -d --name kv -v "$PWD/acl:/acl" quay.io/kividbio/kividb:v1.0.4 --aclfile /acl/users.acl
# rewrite the file: app now has password pw2, user "gone" removed
docker exec kv redis-cli -p 6380 acl load                              # OK
docker exec kv redis-cli -p 6380 acl getuser app                       # two password hashes
docker exec kv redis-cli -p 6380 --user app --pass pw1 acl whoami      # app   (old password)
docker exec kv redis-cli -p 6380 acl users                             # still lists "gone"
```

### Expected

Redis semantics: `ACL LOAD` replaces the whole in-memory user table with
the file's contents, all-or-nothing (if any line is invalid, nothing
changes).

### Operator workaround

Every rendered line starts with `reset` (`user app reset on #... ~... +...`),
which kividb honours both at startup and on `ACL LOAD`, and after a reload
the agent runs `ACL DELUSER` for users the file no longer defines.

---

## 6. Only the last key pattern of a user is kept

**Severity: medium.** Already noted in the operator's ROADMAP; included
here for completeness.

```bash
# user app reset on #<hash> ~app:* ~session:* &* +@all
redis-cli --user app --pass apppw set app:x 1       # NOPERM ... key 'app:x'
redis-cli --user app --pass apppw set session:x 1   # OK
redis-cli acl getuser app                           # keys: ~session:*
```

Expected: a user holds a list of key patterns and a key is allowed if it
matches any of them. The same should be checked for channel patterns.

---

## 7. `ACL SETUSER` and `ACL DELUSER` report an error after applying the change

**Severity: medium.**

When the ACL file is not writable (it is a read-only Secret mount in every
operator-managed pod), these commands change the in-memory users and then
answer with an error:

```
> ACL SETUSER probe on >a ~* +@all
ERR ACL file save failed: Read-only file system (os error 30)
> ACL USERS
app default probe          <- the user was created anyway
```

Two separate points:

1. A command that returns an error should not have taken effect.
2. Redis never writes the ACL file implicitly; that is what `ACL SAVE` is
   for. Rewriting the file on every `SETUSER`/`DELUSER` is surprising when
   the file is managed by something else.

The operator's agent relies on `ACL DELUSER` (see issue 5) and so ignores
its reply and re-reads `ACL USERS` to see whether it worked.

---

## 8. The config file silently ignores what it does not understand

**Severity: medium.**

```
port 6380
appendonly yes
this-directive-does-not-exist 42
maxmemory notanumber
```

kividb starts with this file without a warning. AOF stays off
(`aof_enabled:0`): the directive is `aof yes`, and the Redis spelling
`appendonly yes` is not recognised. The unknown directive and the invalid
`maxmemory` value are dropped.

Expected: fail startup on an unknown directive or an unparseable value,
naming the line (Redis: `Bad directive or wrong number of arguments`), and
accept `appendonly` as an alias given the Redis-compatible config format.

A user who writes `appendonly yes` in a `KividbConfig` today believes they
have durability and does not. The operator cannot validate directives
without duplicating the engine's list.

Related, from the operator's source and **not re-tested on v1.0.4**: the
`tls-port` / `tls-cert-file` / `tls-key-file` / `tls-ca-cert-file`
directives were accepted but not applied from the config file on v1.0.2,
which is why the operator passes them as command-line flags.

---

## 9. A full resync needs about twice the dataset's memory, and happens on every replica restart

**Severity: medium.**

A restarted replica that has its own `dump.kdb` / `appendonly.aof` loads
them, then does a full resync from the master anyway and loads the
received snapshot into a second, staging copy before swapping:

```
[INFO] Loaded 818756 keys from snapshot
[INFO] AOF replay: 483565 commands, 888039 keys, 20.75 MB in 1.87s
[REPL] Full resync from 00000000071240e5... offset=0
[REPL] Receiving RDB dump (24865112 bytes)...
[REPL] RDB received — loading into staging swap...
```

With roughly 900k small keys and a 256Mi container limit the replica was
OOM-killed at this point on every start and crash-looped; the same data
ran comfortably as a master within that limit. `--maxmemory` is accepted
but, per `--help`, not enforced, so there is no softer failure than the
OOM kill.

Worth considering: partial resync (`PSYNC` with the replication ID and
offset the replica already has) so that a quick restart does not transfer
the whole dataset; discarding the local dataset before loading the
received one when a full resync is unavoidable; and enforcing `maxmemory`.

---

## 10. A full resync that fails part-way leaves the replica empty

**Severity: critical** (lost a whole dataset on EKS).

When the master goes away in the middle of sending a full-sync snapshot,
the replica logs a load error and is left holding **zero keys**. The
dataset it held before the resync started is gone.

A master that restarts in a loop does this to every replica at the same
time: each restart gives the master a new replication ID, each replica
starts a full resync, and the next crash cuts the transfer off. That is
what an OOM-killed master looks like, and OOM during a full sync is likely
given issue 9.

### Observed

Three-pod cluster, about 261,000 keys on every pod. The master's
container was being killed and restarted every few seconds (its node's
kubelet could not reach it, so liveness probes failed) while pod-to-pod
traffic still worked. Both replicas, within the same second:

```
21:02:12 [REPL] Restored 261704 keys from full-sync KDB snapshot
21:04:10 [REPL] Connecting to master 192.168.17.9:6380...
21:04:10 [REPL] Full resync from 0000000031c3875d000000000000000131c3875c offset=0
21:04:11 [REPL] Receiving RDB dump (57046214 bytes)...
21:04:11 [REPL] Full-sync KDB load error: failed to fill whole buffer
```

Nine seconds later the operator promoted one of them; the other then
synced from it:

```
21:04:19 [REPL] Receiving RDB dump (76 bytes)...
21:04:19 [REPL] Restored 0 keys from full-sync KDB snapshot
```

A 76-byte snapshot with 0 keys: the promoted replica was empty. The old
master still had the data on disk, but came back as a replica of the
empty one and lost it too.

### Reproduce

Seen once, with the log evidence above, and **not reproduced on demand**:
two later attempts on EKS (a `SIGKILL` loop on the master) and three in
local Docker did not hit it, because the master either died before the
replicas reconnected or finished sending the snapshot before the kill
landed. The condition to aim for is the master dying while a replica is
in `Receiving RDB dump`, i.e. after the size header and before the last
byte; throttling the link between the two would widen that window.

### Expected

Keep the existing dataset until the new one has been received **and**
loaded successfully; on any error, discard the partial one and carry on
serving the old data. kividb's own log line for the successful case
(`RDB received — loading into staging swap`) suggests this is the
intended design, and that the error path does not honour it. Redis with
disk-based replication only flushes the old dataset after the transfer
is complete.

### Operator workaround

`status.pods[].keys` records each pod's key count. When the master was
last seen holding data, failover only considers replicas that still hold
some; if none does, it does not fail over and waits for the master to
return (event `FailoverBlocked`). That trades availability for the data
still on the master's disk, and can be overridden with the
`kividb.io/allow-empty-failover: "true"` annotation.

---

## 11. The final snapshot on SIGTERM is sometimes not written

**Severity: high.**

On `SIGTERM` kividb announces a final snapshot. Sometimes it writes it;
sometimes the process exits with status 0 a moment later without having
written anything, and the next start loads the previous `dump.kdb`.

### Observed

Same engine version, same cluster, minutes apart. An idle three-pod
cluster with 861k keys:

```
[INFO] SIGTERM received — shutting down gracefully
[INFO] Writing final snapshot...
[INFO] Saving snapshot → /data/dump.kdb
[INFO] Snapshot saved  → /data/dump.kdb (16 databases)
[INFO] Goodbye.
```

A master with 1.18M keys, two attached replicas and a client writing
about five keys a second (previous-container log, `exitCode: 0`):

```
22:10:38.457 [INFO] SIGTERM received — shutting down gracefully
22:10:38.457 [INFO] Writing final snapshot...
```

and nothing after it. The container had finished within the same second;
the successful case above took about six. An earlier occurrence, with AOF
off, cost data: the master was restarted, came back from a `dump.kdb`
written 55 seconds before, and its replicas, which held the newer writes,
resynced from it. 261 acknowledged writes were gone from every pod.

What separates the two cases is not established. The failing pods had
client connections open and replication traffic flowing at the time;
the succeeding one was idle. That, and the fact that the process exits
cleanly, points at the shutdown path returning before the snapshot task
has run rather than at the snapshot failing.

### Expected

Do not exit until the final snapshot is on disk (or has failed, in which
case exit non-zero and say so).

### Operator workaround

The operator no longer restarts a master in place. Rolling updates and
`KividbDbOps` restarts first move the master role to an in-sync replica
(event `Switchover`) and only then replace the old master, so nothing
depends on what it saved on the way down. A master that restarts on its
own (crash, OOM kill, node reboot) and comes back before the failover
threshold is still exposed; enabling AOF (`aof yes`) closes that.

---

## Fixed between v1.0.3 and v1.0.4

For reference, seen on v1.0.3 during the upgrade test and no longer
present on v1.0.4: `ROLE` on a replica returned an empty master host and
port 0 (`slave "" 0 connected <offset>`).

---

## Questions for the engine team

These are not bugs as far as is known, but the operator's backup code
depends on the answers:

1. **`dump.kdb.gen`.** kividb writes this 8-byte file next to `dump.kdb`.
   The operator's backups do not include it. Is it needed for a correct
   restore, or to pair `dump.kdb` with `appendonly.aof`?
2. **What is a consistent backup when AOF is on?** The operator runs
   `BGSAVE`, waits for `LASTSAVE` to advance, then copies `dump.kdb` and
   `appendonly.aof` one after the other while kividb keeps appending to
   the AOF. On restore kividb loads the snapshot and then replays the AOF
   "on top". Is that safe when the AOF copy contains commands already in
   the snapshot, or ends in the middle of a command?
3. **`LASTSAVE` has one-second resolution.** A `BGSAVE` that completes in
   the same second as the previous save cannot be detected by comparing
   `LASTSAVE` values. Is there a save counter or generation the operator
   should use instead (possibly what `dump.kdb.gen` holds)?
