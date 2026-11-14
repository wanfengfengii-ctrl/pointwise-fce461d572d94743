"""Frozen business oracle and helpers for installed regression."""
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import sys

import pipeline
import runtime


def small():
    rows = [
        ("a", 1, "east", 0, 3, False),
        ("b", 1, "east", 0, -3, False),
        ("a", 2, "west", 1, 7, False),
        ("c", 1, "east", 0, 3, False),
        ("d", 1, "east", 2, 9, False),
        ("d", 2, "east", 2, 9, True),
    ]
    return {"batch_id": "sample", "scale": 10,
            "rows": [dict(zip(("id", "rev", "station", "minute", "value", "deleted"), r))
                     for r in rows]}


def large():
    # ~10 MB of input; one result bucket per live sample makes ledger writes real.
    return {"batch_id": "large", "scale": 10, "rows": [
        {"id": "r%06d" % i, "rev": 1, "station": "S" + str(i % 3),
         "minute": i, "value": i % 17 - 8, "deleted": False}
        for i in range(90000)]}


def expected(batch):
    # Independent oracle; never uses the stage or summarizer.
    winners = {}
    for row in batch["rows"]:
        old = winners.get(row["id"])
        if old is None or old["rev"] < row["rev"]:
            winners[row["id"]] = row
    cells = {}
    for row in winners.values():
        if row["deleted"]:
            continue
        key = (row["station"], row["minute"])
        count, total = cells.get(key, (0, 0))
        cells[key] = (count + 1, total + batch["scale"] * row["value"])
    return [{"station": s, "minute": m, "count": n, "sum": total}
            for (s, m), (n, total) in sorted(cells.items())]


def normal():
    batch = small()
    stream = io.BytesIO()
    pipeline.stage(batch, stream)
    stream.seek(0)
    result = pipeline.summarize(stream)
    assert result == [{"station": "east", "minute": 0, "count": 2, "sum": 0},
                      {"station": "west", "minute": 1, "count": 1, "sum": 70}]
    assert result == expected(batch)
    body = pipeline.encode(result)
    digest = hashlib.sha256(pipeline.encode(batch)).hexdigest()
    ack = pipeline.receipt("sample", digest, body)
    db = sqlite3.connect(":memory:")
    db.executescript(runtime.SCHEMA)
    db.execute("INSERT INTO jobs(id,digest,state) VALUES(?,?,?)", ("sample", digest, "running"))
    db.commit()
    runtime.commit_result(db, "sample", body, ack, "results/sample.json")
    assert db.execute("SELECT COUNT(*) FROM ledger").fetchone() == (1,)
    assert db.execute("SELECT state FROM jobs").fetchone() == ("completed",)
    assert json.loads(db.execute("SELECT result FROM ledger").fetchone()[0]) == result
    assert json.loads(db.execute("SELECT receipt FROM ledger").fetchone()[0]) == ack
    db.close()
    bad = small()
    bad["rows"].append(dict(bad["rows"][0], value=99))
    try:
        pipeline.validate(bad)
    except ValueError as error:
        assert str(error) == "REVISION_CONFLICT"
    else:
        raise AssertionError("conflicting revision accepted")
    return {"regression": "NORMAL_REGRESSION_OK", "result": result, "receipt": ack}


def installed(root, batch):
    actual = runtime.read(root, batch["batch_id"])
    assert actual["available"] and actual["result"] == expected(batch), "RESULT_MISMATCH"
    saved = (Path(root) / "inputs" / (batch["batch_id"] + ".json")).read_bytes()
    assert saved == pipeline.encode(batch), "ACCEPTED_INPUT_CHANGED"
    ack = actual["receipt"]
    assert ack == pipeline.receipt(batch["batch_id"], hashlib.sha256(saved).hexdigest(),
                                   pipeline.encode(expected(batch))), "RECEIPT_MISMATCH"
    accepted = runtime.submit(root, batch)
    assert accepted["input_sha256"] == ack["input_sha256"]
    with runtime.connect(root) as db:
        assert db.execute("SELECT COUNT(*) FROM ledger WHERE id=?",
                          (batch["batch_id"],)).fetchone() == (1,), "DUPLICATE_ACCOUNTING"
        assert json.loads(db.execute("SELECT result FROM ledger WHERE id=?",
                                     (batch["batch_id"],)).fetchone()[0]) == expected(batch)
    assert runtime.read(root, batch["batch_id"]) == actual
    return {"installed": "INSTALLED_REGRESSION_OK", "receipt": ack,
            "groups": len(actual["result"])}


def source():
    # Real HTTPResponse framing over emitted recording bytes; no sockets needed.
    import http.client
    import collector
    import portal

    fixture = portal.Portal()

    class Socket:
        def __init__(self, raw):
            self.raw = raw

        def makefile(self, *args):
            return io.BytesIO(self.raw)

    class Connection:
        def __init__(self, *args, **kwargs):
            pass

        def request(self, method, path, headers=None):
            self.method, self.path = method, path
            self.headers = {k.lower(): v for k, v in (headers or {}).items()}

        def getresponse(self):
            code, headers, body = fixture.reply(self.method, self.path, self.headers)
            wire = ("HTTP/1.1 %d Recorded\r\n" % code).encode()
            wire += b"".join(("%s: %s\r\n" % item).encode() for item in headers.items())
            response = http.client.HTTPResponse(Socket(wire + b"\r\n" + body), method=self.method)
            response.begin()
            return response

        def close(self):
            pass

    def fetch():
        output = io.BytesIO()
        metadata = collector.download("http://127.0.0.1/export", output,
                                      connection_factory=Connection)
        raw = output.getvalue()
        batch = json.loads(raw)
        staged = io.BytesIO()
        pipeline.stage(batch, staged)
        staged.seek(0)
        result = pipeline.summarize(staged)
        assert result == expected(batch)
        assert metadata["sha256"] == hashlib.sha256(raw).hexdigest()
        assert metadata["length"] == len(raw)
        return metadata, result

    metadata, result = fetch()
    assert result == normal()["result"]
    first = metadata
    fixture.reply("POST", "/control", {}, b'{"version":2}')
    metadata, result = fetch()
    assert metadata["etag"] != first["etag"] and result[1]["sum"] == 110

    def exchange(method, headers):
        connection = Connection()
        connection.request(method, "/export", headers=headers)
        response = connection.getresponse()
        return response, response.read()

    response, tail = exchange("GET", {"Range": "bytes=32768-", "If-Range": metadata["etag"]})
    assert response.status == 206 and tail == fixture.versions[2][32768:]
    assert response.getheader("Content-Range") == "bytes 32768-270335/270336"
    response, raw = exchange("GET", {"Range": "bytes=32768-", "If-Range": first["etag"]})
    assert response.status == 200 and raw == fixture.versions[2]
    response, raw = exchange("HEAD", {})
    assert response.status == 200 and not raw and response.getheader("Content-Length") == "270336"
    response, raw = exchange("GET", {"Range": "bytes=270336-", "If-Range": metadata["etag"]})
    assert response.status == 416 and not raw
    assert response.getheader("Content-Range") == "bytes */270336"
    fixture.reply("POST", "/control", {}, b'{"cut_after":20000}')
    try:
        fetch()
    except (OSError, http.client.HTTPException):
        pass
    else:
        raise AssertionError("TRUNCATION_ACCEPTED")
    fixture.reply("POST", "/control", {}, b'{"bad_digest":true}')
    try:
        fetch()
    except ValueError as error:
        assert str(error) == "DIGEST"
    else:
        raise AssertionError("CORRUPTION_ACCEPTED")
    return {"regression": "SOURCE_REGRESSION_OK", "version1": first,
            "version2": metadata, "version2_result": result,
            "requests": fixture.requests}


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "normal"
    if mode == "normal":
        print(json.dumps(normal(), ensure_ascii=False, sort_keys=True))
    elif mode == "source":
        print(json.dumps(source(), sort_keys=True))
    elif mode in ("verify", "demo"):
        # Released-artifact acceptance: install from a reproducible ZIP,
        # collect through the installed user service, and account the result.
        import lab
        value = lab.verify(sys.argv[2] if len(sys.argv) > 2 else None) if mode == "verify" \
            else lab.demo(sys.argv[2] if len(sys.argv) > 2 else None)
        print(json.dumps(value, ensure_ascii=False, sort_keys=True))
    elif mode == "business":
        print(json.dumps({"business": normal(), "source": source()}, sort_keys=True))
    elif mode == "fixture":
        Path(sys.argv[2]).write_bytes(pipeline.encode(large()))
    elif mode == "installed":
        print(json.dumps(installed(sys.argv[2], json.loads(Path(sys.argv[3]).read_bytes())),
                         sort_keys=True))
    else:
        raise SystemExit("usage: checks.py normal|demo [DIR]|source|verify [DIR]|"
                         "business|fixture PATH|installed ROOT INPUT")


if __name__ == "__main__":
    main()
