"""Manual runner and current unattended runner, sharing the existing kernel."""
import fcntl
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time

from pipeline import encode, receipt, stage, summarize, validate


SCHEMA = """
CREATE TABLE jobs(
 id TEXT PRIMARY KEY, digest TEXT NOT NULL, state TEXT NOT NULL,
 receipt TEXT, result_path TEXT
);
CREATE TABLE ledger(id TEXT PRIMARY KEY, result BLOB NOT NULL, receipt TEXT NOT NULL);
"""


@contextmanager
def connect(root):
    db = sqlite3.connect(str(Path(root) / "state.sqlite"), timeout=5)
    try:
        db.execute("PRAGMA synchronous=FULL")
        with db:
            yield db
    finally:
        db.close()


def init(root, control):
    root = Path(root).resolve()
    control = Path(control).resolve()
    root.mkdir(parents=True, exist_ok=True)
    control.mkdir(parents=True, exist_ok=True)
    for directory in ("inputs", "work", "results"):
        (root / directory).mkdir(exist_ok=True)
    with connect(root) as db:
        db.executescript(SCHEMA)
    (root / "config.json").write_bytes(encode({"control": str(control)}))


def barrier(root, phase):
    control = Path(json.loads((Path(root) / "config.json").read_bytes())["control"])
    plan = control / "hook.json"
    if not plan.exists() or json.loads(plan.read_bytes()).get("phase") != phase:
        return
    spent = control / (phase + ".spent")
    if spent.exists():
        return
    (control / (phase + ".ready")).write_text(str(os.getpid()))
    while not (control / (phase + ".go")).exists():
        time.sleep(0.05)
    spent.write_text("passed")


def sync_directory(directory):
    fd = os.open(str(directory), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def submit(root, batch):
    root = Path(root)
    validate(batch)
    body = encode(batch)
    digest = hashlib.sha256(body).hexdigest()
    name = batch["batch_id"]
    with connect(root) as db:
        db.execute("BEGIN IMMEDIATE")
        old = db.execute("SELECT digest FROM jobs WHERE id=?", (name,)).fetchone()
        if old:
            if old[0] != digest:
                raise ValueError("BATCH_CONFLICT")
            return {"accepted": name, "input_sha256": digest}
        if db.execute("SELECT 1 FROM jobs WHERE state != 'completed'").fetchone():
            raise ValueError("ACTIVE_BATCH")
        destination = root / "inputs" / (name + ".json")
        temporary = destination.with_suffix(".incoming")
        with temporary.open("wb") as target:
            target.write(body)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, destination)
        sync_directory(destination.parent)
        db.execute("INSERT INTO jobs(id,digest,state) VALUES(?,?,'pending')", (name, digest))
    return {"accepted": name, "input_sha256": digest}


def commit_result(db, name, result, ack, path):
    # Existing atomic accounting boundary, intentionally separate from file I/O.
    with db:
        db.execute("INSERT INTO ledger VALUES(?,?,?)",
                   (name, result, encode(ack).decode()))
        db.execute("UPDATE jobs SET state='completed',receipt=?,result_path=? WHERE id=?",
                   (encode(ack).decode(), path, name))


def run_one(root):
    root = Path(root)
    with connect(root) as db:
        job = db.execute("SELECT id,digest FROM jobs WHERE state IN ('pending','running') ORDER BY id LIMIT 1").fetchone()
        if job is None:
            return False
        name, digest = job
        db.execute("UPDATE jobs SET state='running' WHERE id=?", (name,))
    batch = json.loads((root / "inputs" / (name + ".json")).read_bytes())
    intermediate = root / "work" / (name + ".jsonl")
    barrier(root, "stage")
    with intermediate.open("wb") as target:
        stage(batch, target)
        target.flush()
        os.fsync(target.fileno())
    with intermediate.open("rb") as source:
        result = encode(summarize(source))
    destination = root / "results" / (name + ".json")
    barrier(root, "result")
    with destination.open("wb") as target:
        target.write(result)
        target.flush()
        os.fsync(target.fileno())
    ack = receipt(name, digest, result)
    barrier(root, "ledger")
    with connect(root) as db:
        commit_result(db, name, result, ack, str(destination.relative_to(root)))
    return True


def status(root):
    with connect(root) as db:
        rows = db.execute("SELECT id,state,receipt FROM jobs ORDER BY id").fetchall()
    return [{"batch_id": name, "state": state,
             "receipt": json.loads(ack) if ack is not None else None}
            for name, state, ack in rows]


def read(root, name):
    with connect(root) as db:
        row = db.execute("SELECT state,result_path,receipt FROM jobs WHERE id=?", (name,)).fetchone()
    if row is None or row[0] != "completed":
        return {"batch_id": name, "available": False}
    body = (Path(root) / row[1]).read_bytes()
    ack = json.loads(row[2])
    if hashlib.sha256(body).hexdigest() != ack["result_sha256"]:
        raise ValueError("RESULT_CORRUPT")
    return {"batch_id": name, "available": True, "result": json.loads(body), "receipt": ack}


def worker(root):
    root = Path(root)
    with (root / "worker.lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        while True:
            if not run_one(root):
                time.sleep(0.2)
