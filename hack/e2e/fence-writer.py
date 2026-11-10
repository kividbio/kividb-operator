# fence-writer.py: counts the acknowledged writes a failover loses (#53).
#
# Runs in a pod. CONNS connections to the master Service each SET a fresh
# key, one at a time, and record every +OK: an acknowledged write. On an
# error the connection is reopened through the Service and writing goes on
# with the next key. After DURATION seconds, and SETTLE more for the
# cluster to settle, every acknowledged key is checked with EXISTS on the
# master. Prints one JSON line:
#   {"acked": N, "lost": M, "acked_after_error": K,
#    "first_lost": t|null, "last_lost": t|null}
# acked_after_error counts the writes acknowledged after the first error
# on any connection -- the master going away: writes that went through the
# new master.
# Key names start with PREFIX: they must be fresh on every run, or a key an
# earlier run wrote hides the loss of this run's.
import json
import os
import socket
import threading
import time

HOST = os.environ["HOST"]
PORT = int(os.environ.get("PORT", "6380"))
CONNS = int(os.environ.get("CONNS", "4"))
DURATION = float(os.environ.get("DURATION", "30"))
SETTLE = float(os.environ.get("SETTLE", "20"))
PREFIX = os.environ["PREFIX"]


def encode(*args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = str(a).encode()
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out


class Conn:
    def __init__(self, timeout):
        self.sock = socket.create_connection((HOST, PORT), timeout=timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.file = self.sock.makefile("rb")

    def send(self, *args):
        self.sock.sendall(encode(*args))

    def read(self):
        line = self.file.readline()
        if not line:
            raise ConnectionError("closed")
        kind, rest = line[:1], line[1:-2]
        if kind == b"-":
            raise RuntimeError(rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            return None if n < 0 else self.file.read(n + 2)[:-2]
        return rest.decode()

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


acked = [[] for _ in range(CONNS)]  # (key, time acknowledged)
first_error = []  # time of the first error on any connection
end = time.time() + DURATION


def writer(i):
    n, conn = 0, None
    while time.time() < end:
        if conn is None:
            try:
                conn = Conn(timeout=2)
            except OSError:
                time.sleep(0.05)
                continue
        n += 1
        key = "%s:w%d:%d" % (PREFIX, i, n)
        try:
            conn.send("SET", key, n)
            if conn.read() == "OK":
                acked[i].append((key, time.time()))
        except (OSError, RuntimeError, ConnectionError):
            if not first_error:
                first_error.append(time.time())
            conn.close()
            conn = None


threads = [threading.Thread(target=writer, args=(i,)) for i in range(CONNS)]
for t in threads:
    t.start()
for t in threads:
    t.join()
time.sleep(SETTLE)

check = Conn(timeout=10)
total, lost = 0, []
for keys in acked:
    for start in range(0, len(keys), 500):
        batch = keys[start:start + 500]
        for key, _ in batch:
            check.send("EXISTS", key)
        for key, at in batch:
            total += 1
            if check.read() == 0:
                lost.append(at)
lost.sort()
after = sum(1 for keys in acked for _, at in keys if first_error and at > first_error[0])
print(json.dumps({
    "acked": total,
    "lost": len(lost),
    "acked_after_error": after,
    "first_lost": lost[0] if lost else None,
    "last_lost": lost[-1] if lost else None,
}))
