"""Resumable whole-response collector for one portal export at a time.

The durable checkpoint is the tuple (strong ETag, Content-Length, full-file
SHA-256, confirmed prefix length).  Bytes are fsync'ed and the checkpoint is
committed before they are advertised; a restart therefore never claims bytes
that were only displayed.  A conditional range request continues the same
representation.  A 200 response to a range request, or a HEAD with a different
strong ETag, discards the old incomplete payload before any new byte is kept.

The handoff after the raw file is verified is replay-safe: the raw file is
reused, runtime.submit is idempotent, the ledger commit is atomic, and the
download row becomes completed only after the business receipt is readable.
"""
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import sqlite3
import time
from urllib.parse import urlsplit

import runtime

CHUNK = 32768
STRONG_ETAG = re.compile(r'"[^"]+"')
HEX64 = re.compile(r"[0-9a-f]{64}")
CONTENT_RANGE = re.compile(r"bytes (\d+)-(\d+)/(\d+)")
CONTENT_RANGE_STAR = re.compile(r"bytes \*/(\d+)")
# Business-kernel refusals that describe contention, not bad input, and must
# be retried without making the download terminal.
TRANSIENT = {"ACTIVE_BATCH", "NOT_ACCOUNTED", "RANGE_RESET"}

SCHEMA = """
CREATE TABLE downloads(
 id TEXT PRIMARY KEY, url TEXT NOT NULL, state TEXT NOT NULL,
 etag TEXT, length INTEGER, sha256 TEXT, received INTEGER NOT NULL DEFAULT 0,
 error TEXT, receipt TEXT
);
"""


def init(root):
    (Path(root) / "downloads").mkdir(exist_ok=True)
    with runtime.connect(root) as db:
        db.executescript(SCHEMA)


def enqueue(root, task_id, url):
    if not isinstance(task_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", task_id):
        raise ValueError("TASK_ID")
    parts = urlsplit(url)
    if (parts.scheme != "http" or not parts.hostname or parts.username
            or parts.password or parts.fragment):
        raise ValueError("URL")
    root = Path(root)
    reopened = False
    with runtime.connect(root) as db:
        db.execute("BEGIN IMMEDIATE")
        old = db.execute("SELECT url,state FROM downloads WHERE id=?", (task_id,)).fetchone()
        if old:
            if old[0] != url:
                raise ValueError("TASK_CONFLICT")
            if old[1] == "failed":
                # An explicit re-enqueue is the only way back from a terminal
                # failure; it still respects the single-active-download rule.
                if db.execute("SELECT 1 FROM downloads WHERE id != ? AND "
                              "state NOT IN ('completed','failed')",
                              (task_id,)).fetchone():
                    raise ValueError("ACTIVE_DOWNLOAD")
                db.execute("UPDATE downloads SET state='queued',etag=NULL,length=NULL,"
                           "sha256=NULL,received=0,error=NULL,receipt=NULL WHERE id=?",
                           (task_id,))
                reopened = True
        else:
            if db.execute("SELECT 1 FROM downloads WHERE state NOT IN ('completed','failed')").fetchone():
                raise ValueError("ACTIVE_DOWNLOAD")
            db.execute("INSERT INTO downloads(id,url,state) VALUES(?,?,'queued')", (task_id, url))
    if reopened:
        # Stale payload from the failed attempt must not be adopted; the row
        # already has no representation-bound checkpoint.
        for suffix in (".part", ".raw"):
            (root / "downloads" / (task_id + suffix)).unlink(missing_ok=True)
    return {"task_id": task_id, "url": url}


# ---------------------------------------------------------------------------
# HTTP primitives.  No redirects, no compression, no credentials.

def _request(url, method, headers, connection_factory=None):
    parts = urlsplit(url)
    factory = connection_factory or http.client.HTTPConnection
    connection = factory(parts.hostname, parts.port or 80, timeout=10)
    request_headers = {"Accept-Encoding": "identity"}
    request_headers.update(headers)
    connection.request(method, parts.path + ("?" + parts.query if parts.query else ""),
                       headers=request_headers)
    return connection, connection.getresponse()


def _metadata(response):
    etag = response.getheader("ETag")
    digest = response.getheader("X-Content-SHA256")
    length_text = response.getheader("Content-Length", "")
    if not etag or not STRONG_ETAG.fullmatch(etag):
        raise ValueError("ETAG")
    if not digest or not HEX64.fullmatch(digest) or not length_text.isdecimal():
        raise ValueError("METADATA")
    return {"etag": etag, "length": int(length_text), "sha256": digest}


def download(url, target, progress=None, connection_factory=None):
    """Whole-response primitive kept for the protocol regression and selftest."""
    connection, response = _request(url, "GET", {}, connection_factory)
    try:
        if response.status != 200:
            raise ValueError("HTTP_STATUS")
        metadata = _metadata(response)
        size, actual = 0, hashlib.sha256()
        while True:
            block = response.read(16384)
            if not block:
                break
            target.write(block)
            size += len(block)
            actual.update(block)
            if progress:
                progress(size, metadata)
        if size != metadata["length"]:
            raise OSError("SHORT_RESPONSE")
        if actual.hexdigest() != metadata["sha256"]:
            raise ValueError("DIGEST")
        return metadata
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# Durable helpers.  Each one is a short transaction; none is held across I/O.

def _checkpoint(root, name, etag, length, sha256, received):
    with runtime.connect(root) as db:
        db.execute("UPDATE downloads SET etag=?,length=?,sha256=?,received=?,error=NULL WHERE id=?",
                   (etag, length, sha256, received, name))


def _set_state(root, name, state, error, receipt=None):
    with runtime.connect(root) as db:
        if receipt is None:
            db.execute("UPDATE downloads SET state=?,error=? WHERE id=?", (state, error, name))
        else:
            db.execute("UPDATE downloads SET state=?,error=?,receipt=? WHERE id=?",
                       (state, error, receipt, name))


def _reset_checkpoint(root, name):
    with runtime.connect(root) as db:
        db.execute("UPDATE downloads SET etag=NULL,length=NULL,sha256=NULL,received=0 WHERE id=?",
                   (name,))


def _truncate(path, size):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as target:
        target.truncate(size)
        target.flush()
        os.fsync(target.fileno())
    runtime.sync_directory(path.parent)


def _file_size(path):
    try:
        return path.stat().st_size
    except FileNotFoundError:
        return 0


def _sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while True:
            block = source.read(65536)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _verify_file(path, length, sha256):
    if length is None or sha256 is None or not path.exists():
        raise OSError("SHORT_RESPONSE")
    if _file_size(path) != length:
        raise OSError("SHORT_RESPONSE")
    if _sha256_file(path) != sha256:
        raise ValueError("DIGEST")


# ---------------------------------------------------------------------------
# Acquisition.

def _head(url, connection_factory=None):
    connection, response = _request(url, "HEAD", {}, connection_factory)
    try:
        if response.status != 200:
            raise ValueError("HTTP_STATUS")
        return _metadata(response)
    finally:
        connection.close()


def _stream(root, name, response, part, offset, etag, length, sha256):
    mode = "r+b" if offset else "wb"
    read = getattr(response, "read1", response.read)
    with part.open(mode) as target:
        if offset:
            target.seek(offset)
            target.truncate(offset)
        position = offset
        while True:
            block = read(CHUNK)
            if not block:
                break
            target.write(block)
            position += len(block)
            # Durable bytes first, then the checkpoint that advertises them.
            target.flush()
            os.fsync(target.fileno())
            _checkpoint(root, name, etag, length, sha256, position)
            runtime.barrier(root, "chunk")
    return position


def _fetch(root, name, url, part, etag, length, sha256, received,
           connection_factory=None):
    """Fetch the missing suffix and verify the whole on-disk file.

    A conditional range request is used whenever a same-version prefix is
    confirmed.  Any complete 200 representation is treated as the current
    version: the old prefix is discarded before its body is written.
    """
    for attempt in (1, 2):
        headers = {}
        if received > 0 and etag is not None:
            headers["Range"] = "bytes=%d-" % received
            headers["If-Range"] = etag
        connection, response = _request(url, "GET", headers, connection_factory)
        try:
            status = response.status
            if status == 206:
                if not (received > 0 and etag is not None):
                    raise ValueError("CONTENT_RANGE")
                match = CONTENT_RANGE.fullmatch(response.getheader("Content-Range") or "")
                metadata = _metadata(response)
                if (match is None or metadata["etag"] != etag
                        or int(match[1]) != received or int(match[2]) != int(match[3]) - 1
                        or metadata["length"] != int(match[3]) - received):
                    raise ValueError("CONTENT_RANGE")
                length = int(match[3])
                sha256 = metadata["sha256"]
                _checkpoint(root, name, etag, length, sha256, received)
                position = _stream(root, name, response, part, received,
                                   etag, length, sha256)
            elif status == 200:
                metadata = _metadata(response)
                etag, length, sha256 = metadata["etag"], metadata["length"], metadata["sha256"]
                # Version changed (or first delivery): old payload is not a
                # prefix of this representation and must not be appended to.
                _truncate(part, 0)
                received = 0
                _checkpoint(root, name, etag, length, sha256, 0)
                position = _stream(root, name, response, part, 0, etag, length, sha256)
            elif status == 416:
                match = CONTENT_RANGE_STAR.fullmatch(response.getheader("Content-Range") or "")
                total = int(match[1]) if match else None
                if received <= 0 or etag is None:
                    raise ValueError("HTTP_STATUS")
                if (response.getheader("ETag") == etag and total is not None
                        and received >= total):
                    if received > total:
                        _truncate(part, total)
                        _checkpoint(root, name, etag, total, sha256, total)
                        received = total
                    length = total
                    position = received
                else:
                    # Our checkpoint no longer belongs to this representation.
                    _truncate(part, 0)
                    etag = length = sha256 = None
                    received = 0
                    _checkpoint(root, name, None, None, None, 0)
                    if attempt == 1:
                        continue
                    raise OSError("RANGE_RESET")
            else:
                raise ValueError("HTTP_STATUS")
        finally:
            connection.close()
        if position != length:
            raise OSError("SHORT_RESPONSE")
        _verify_file(part, length, sha256)
        return etag, length, sha256, position
    raise OSError("RANGE_RESET")


def _acquire(root, name, url, etag, length, sha256, received, part,
             connection_factory=None):
    size = _file_size(part)
    if etag is None:
        # No representation-bound checkpoint: nothing on disk is confirmed.
        if size:
            _truncate(part, 0)
        received = 0
        _checkpoint(root, name, None, None, None, 0)
    else:
        if size < received:
            received = size
            _checkpoint(root, name, etag, length, sha256, received)
        elif size > received:
            # Crash after append but before confirmation: the tail is not
            # advertised and is allowed to be requested again.
            _truncate(part, received)

    if received > 0 and etag is not None:
        remote = _head(url, connection_factory)
        if (remote["etag"] != etag or (length is not None and remote["length"] != length)
                or (sha256 is not None and remote["sha256"] != sha256)):
            # New version before any suffix request: drop the old payload.
            _truncate(part, 0)
            etag, length, sha256, received = (remote["etag"], remote["length"],
                                              remote["sha256"], 0)
            _checkpoint(root, name, etag, length, sha256, 0)
        else:
            length, sha256 = remote["length"], remote["sha256"]
            _checkpoint(root, name, etag, length, sha256, received)

    if length is not None and received >= length:
        if received > length:
            _truncate(part, length)
            received = length
            _checkpoint(root, name, etag, length, sha256, received)
        try:
            _verify_file(part, length, sha256)
        except (OSError, ValueError):
            # A full-length prefix that does not hash to its bound digest is
            # not a confirmed representation: discard it and fetch again.
            _truncate(part, 0)
            _reset_checkpoint(root, name)
            etag, length, sha256, received = None, None, None, 0
        else:
            return etag, length, sha256, received

    return _fetch(root, name, url, part, etag, length, sha256, received,
                  connection_factory)


# ---------------------------------------------------------------------------
# Verified-file handoff and the one public unit of work.

def _handoff(root, name, raw):
    runtime.barrier(root, "verified")
    batch = json.loads(raw.read_bytes())
    runtime.submit(root, batch)
    runtime.barrier(root, "accepted")
    runtime.run_one(root)
    runtime.barrier(root, "accounted")
    result = runtime.read(root, batch["batch_id"])
    if not result.get("available"):
        raise ValueError("NOT_ACCOUNTED")
    receipt = json.dumps(result["receipt"], sort_keys=True)
    with runtime.connect(root) as db:
        # completed, the stable receipt and received==length land together.
        db.execute("UPDATE downloads SET state='completed',error=NULL,receipt=?,"
                   "received=length WHERE id=? AND length IS NOT NULL", (receipt, name))
        db.execute("UPDATE downloads SET state='completed',error=NULL,receipt=? WHERE id=? "
                   "AND length IS NULL", (receipt, name))
    return True


def run_one(root, connection_factory=None):
    root = Path(root)
    with runtime.connect(root) as db:
        row = db.execute("SELECT id,url,state,etag,length,sha256,received FROM downloads "
                         "WHERE state IN ('queued','downloading') ORDER BY id LIMIT 1").fetchone()
    if row is None:
        return False
    name, url, _, etag, length, sha256, received = row
    part = root / "downloads" / (name + ".part")
    raw = root / "downloads" / (name + ".raw")

    try:
        if raw.exists():
            try:
                _verify_file(raw, length, sha256)
            except (OSError, ValueError):
                # A verified-file promotion is only trusted if its bound
                # metadata still matches; otherwise collect it again.
                raw.unlink(missing_ok=True)
                _truncate(part, 0)
                _reset_checkpoint(root, name)
                etag, length, sha256, received = None, None, None, 0
            else:
                _checkpoint(root, name, etag, length, sha256, length)
                return _handoff(root, name, raw)

        _set_state(root, name, "downloading", None)
        etag, length, sha256, received = _acquire(
            root, name, url, etag, length, sha256, received, part, connection_factory)
        _checkpoint(root, name, etag, length, sha256, received)
        os.replace(part, raw)
        runtime.sync_directory(raw.parent)
        return _handoff(root, name, raw)
    except (OSError, http.client.HTTPException) as error:
        # Transport failures, timeouts and short bodies keep the durable
        # checkpoint and are retried without claiming completion.
        _set_state(root, name, "queued", str(error) or type(error).__name__)
        return True
    except sqlite3.OperationalError as error:
        _set_state(root, name, "queued", str(error) or type(error).__name__)
        return True
    except ValueError as error:
        if str(error) in TRANSIENT:
            _set_state(root, name, "queued", str(error))
            return True
        # Invalid input, invalid metadata or a whole-file digest mismatch is
        # never handed to the business kernel.  A file that fails its digest
        # is not a confirmed representation, so its prefix is not retained.
        for suffix in (".part", ".raw"):
            (root / "downloads" / (name + suffix)).unlink(missing_ok=True)
        with runtime.connect(root) as db:
            db.execute("UPDATE downloads SET state='failed',error=?,received=0 WHERE id=?",
                       (str(error) or type(error).__name__, name))
        return True


def status(root):
    with runtime.connect(root) as db:
        rows = db.execute("SELECT id,url,state,etag,length,sha256,received,error,receipt "
                          "FROM downloads ORDER BY id").fetchall()
    keys = ("task_id", "url", "state", "etag", "length", "sha256", "received", "error", "receipt")
    result = [dict(zip(keys, row)) for row in rows]
    for row in result:
        row["receipt"] = json.loads(row["receipt"]) if row["receipt"] else None
    return result


def _retry_pending(root):
    with runtime.connect(root) as db:
        return db.execute("SELECT 1 FROM downloads WHERE state='queued' AND error IS NOT NULL "
                          "LIMIT 1").fetchone() is not None


def worker(root):
    import fcntl
    with (Path(root) / "worker.lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        while True:
            processed = run_one(root)
            if runtime.run_one(root):
                processed = True
            if _retry_pending(root):
                time.sleep(1)      # Back off while the source is unreachable.
            elif processed:
                time.sleep(0.05)
            else:
                time.sleep(0.2)
