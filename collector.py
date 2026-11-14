"""Working whole-response collector. Restart deliberately starts at byte zero."""
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import time
from urllib.parse import urlsplit

import runtime

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
    if parts.scheme != "http" or not parts.hostname or parts.username or parts.password or parts.fragment:
        raise ValueError("URL")
    with runtime.connect(root) as db:
        db.execute("BEGIN IMMEDIATE")
        old = db.execute("SELECT url FROM downloads WHERE id=?", (task_id,)).fetchone()
        if old:
            if old[0] != url:
                raise ValueError("TASK_CONFLICT")
        else:
            if db.execute("SELECT 1 FROM downloads WHERE state NOT IN ('completed','failed')").fetchone():
                raise ValueError("ACTIVE_DOWNLOAD")
            db.execute("INSERT INTO downloads(id,url,state) VALUES(?,?,'queued')", (task_id, url))
    return {"task_id": task_id, "url": url}


def download(url, target, progress=None, connection_factory=None):
    parts = urlsplit(url)
    factory = connection_factory or http.client.HTTPConnection
    connection = factory(parts.hostname, parts.port or 80, timeout=5)
    try:
        connection.request("GET", parts.path + ("?" + parts.query if parts.query else ""),
                           headers={"Accept-Encoding": "identity"})
        response = connection.getresponse()
        if response.status != 200:
            raise ValueError("HTTP_STATUS")
        etag = response.getheader("ETag")
        digest = response.getheader("X-Content-SHA256")
        length_text = response.getheader("Content-Length", "")
        if not etag or not re.fullmatch(r'"[^"]+"', etag):
            raise ValueError("ETAG")
        if not digest or not re.fullmatch(r"[0-9a-f]{64}", digest) or not length_text.isdecimal():
            raise ValueError("METADATA")
        length = int(length_text)
        metadata = {"etag": etag, "length": length, "sha256": digest}
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
        if size != length:
            raise OSError("SHORT_RESPONSE")
        if actual.hexdigest() != digest:
            raise ValueError("DIGEST")
        return metadata
    finally:
        connection.close()


def run_one(root):
    root = Path(root)
    with runtime.connect(root) as db:
        row = db.execute("SELECT id,url FROM downloads WHERE state IN ('queued','downloading') ORDER BY id LIMIT 1").fetchone()
        if row is None:
            return False
        name, url = row
        db.execute("UPDATE downloads SET state='downloading',received=0,etag=NULL,length=NULL,sha256=NULL,error=NULL WHERE id=?", (name,))
    temporary = root / "downloads" / (name + ".part")
    complete = root / "downloads" / (name + ".raw")

    def progress(size, metadata):
        with runtime.connect(root) as db:
            db.execute("UPDATE downloads SET received=?,etag=?,length=?,sha256=? WHERE id=?",
                       (size, metadata["etag"], metadata["length"], metadata["sha256"], name))
        runtime.barrier(root, "chunk")

    try:
        with temporary.open("wb") as target:
            metadata = download(url, target, progress)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, complete)
        runtime.sync_directory(complete.parent)
        runtime.barrier(root, "verified")
        batch = json.loads(complete.read_bytes())
        runtime.submit(root, batch)
        runtime.barrier(root, "accepted")
        runtime.run_one(root)
        runtime.barrier(root, "accounted")
        result = runtime.read(root, batch["batch_id"])
        if not result["available"]:
            raise ValueError("NOT_ACCOUNTED")
        with runtime.connect(root) as db:
            db.execute("UPDATE downloads SET state='completed',receipt=? WHERE id=?",
                       (json.dumps(result["receipt"], sort_keys=True), name))
    except (OSError, http.client.HTTPException) as error:
        with runtime.connect(root) as db:
            db.execute("UPDATE downloads SET state='queued',error=? WHERE id=?", (str(error), name))
    except ValueError as error:
        with runtime.connect(root) as db:
            db.execute("UPDATE downloads SET state='failed',error=? WHERE id=?", (str(error), name))
    return True


def status(root):
    with runtime.connect(root) as db:
        rows = db.execute("SELECT id,url,state,etag,length,sha256,received,error,receipt FROM downloads ORDER BY id").fetchall()
    keys = ("task_id", "url", "state", "etag", "length", "sha256", "received", "error", "receipt")
    result = [dict(zip(keys, row)) for row in rows]
    for row in result:
        row["receipt"] = json.loads(row["receipt"]) if row["receipt"] else None
    return result


def worker(root):
    import fcntl
    with (Path(root) / "worker.lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        while True:
            if not run_one(root):
                runtime.run_one(root)
            time.sleep(0.2)
