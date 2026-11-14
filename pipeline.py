"""Existing analysis kernel; deployment work must preserve these semantics."""
import hashlib
import json


def encode(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True,
                       separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")


def validate(batch):
    if not isinstance(batch, dict) or set(batch) != {"batch_id", "scale", "rows"}:
        raise ValueError("BATCH")
    name = batch["batch_id"]
    if not isinstance(name, str) or not name or any(c not in
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in name):
        raise ValueError("BATCH_ID")
    if type(batch["scale"]) is not int or not 1 <= batch["scale"] <= 100:
        raise ValueError("SCALE")
    if not isinstance(batch["rows"], list):
        raise ValueError("ROWS")
    versions = {}
    for row in batch["rows"]:
        if not isinstance(row, dict) or set(row) != {
                "id", "rev", "station", "minute", "value", "deleted"}:
            raise ValueError("ROW")
        if any(not isinstance(row[k], str) or not row[k] for k in ("id", "station")):
            raise ValueError("IDENTITY")
        if type(row["rev"]) is not int or row["rev"] < 1:
            raise ValueError("REV")
        if any(type(row[k]) is not int for k in ("minute", "value")):
            raise ValueError("NUMBER")
        if type(row["deleted"]) is not bool:
            raise ValueError("DELETED")
        key = (row["id"], row["rev"])
        if key in versions and versions[key] != row:
            raise ValueError("REVISION_CONFLICT")
        versions[key] = row
    return batch


def stage(batch, output):
    validate(batch)
    latest = {}
    for row in batch["rows"]:
        if row["id"] not in latest or row["rev"] > latest[row["id"]]["rev"]:
            latest[row["id"]] = row
    for name in sorted(latest):
        row = latest[name]
        if not row["deleted"]:
            output.write(encode([row["id"], row["station"], row["minute"],
                                 row["value"] * batch["scale"]]))


def summarize(source):
    groups = {}
    for line in source:
        _, station, minute, value = json.loads(line)
        cell = groups.setdefault((station, minute), [0, 0])
        cell[0] += 1
        cell[1] += value
    return [{"station": station, "minute": minute, "count": value[0], "sum": value[1]}
            for (station, minute), value in sorted(groups.items())]


def receipt(batch_id, input_hash, result_bytes):
    return {"batch_id": batch_id, "input_sha256": input_hash,
            "result_sha256": hashlib.sha256(result_bytes).hexdigest()}
