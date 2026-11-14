"""Installed acquisition, recovery drills and launch-time evidence entries.

Every scenario installs the release ZIP into a fresh ROOT, starts the recorded
source and the user agent from the installed app.pyz, and never imports task
code from the development directory.  Recovery drills use the CONTROL hooks and
a real agent restart (launchd on macOS, the same supervised-agent contract
elsewhere) to prove that the durable checkpoint is reused.
"""

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
from urllib.parse import urlsplit
import zipfile

import ops
import pipeline


def command(argv, **kwargs):
    return subprocess.run([str(x) for x in argv], check=True, capture_output=True,
                          text=True, **kwargs).stdout


def wait_for(predicate, message, seconds=60):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    raise AssertionError(message)


def service_pid(root):
    if shutil.which("launchctl"):
        import re
        target = "gui/%d/%s" % (os.getuid(), ops.identity(root))
        result = subprocess.run(["launchctl", "print", target], capture_output=True, text=True)
        match = re.search(r"\bpid = ([0-9]+)", result.stdout)
        return int(match[1]) if match else None
    state = Path(root).resolve()
    control = Path(json.loads((state / "config.json").read_bytes())["control"])
    record = control / "fallback-agent.json"
    try:
        return json.loads(record.read_text())["pid"]
    except (FileNotFoundError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Installed-release harness.

def unpack_development(release, directory):
    directory = Path(directory).resolve()
    development = directory / "development"
    development.mkdir()
    with zipfile.ZipFile(io.BytesIO(ops.unpack(release))) as package:
        for name in ops.CODE:
            (development / name).write_bytes(package.read(name))
    rebuilt = directory / "rebuilt.zip"
    command([sys.executable, development / "ops.py", "build", rebuilt], cwd=directory)
    if rebuilt.read_bytes() != Path(release).read_bytes():
        raise AssertionError("NONREPRODUCIBLE_PACKAGE")
    development.rename(directory / "development-moved")


class Installed:
    def __init__(self, release, directory):
        self.directory = Path(directory).resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        self.release = Path(release).resolve()
        if not (self.directory / "development-moved").exists():
            unpack_development(self.release, self.directory)
        self.root = self.directory / "service"
        self.control = self.directory / "control"
        self.app = self.root / "app.pyz"
        self.env = dict(os.environ)
        self.env.pop("PYTHONPATH", None)
        bootstrap = self.directory / "bootstrap.pyz"
        bootstrap.write_bytes(ops.unpack(self.release))
        self.call("install", self.release, self.root, "--control", self.control)
        self.source = None
        self.url = None
        self.agent = None

    def call(self, *args):
        return json.loads(command([sys.executable, app_or(self, args[0]), *args],
                                  cwd=self.directory, env=self.env))

    def start_source(self):
        self.source = subprocess.Popen(
            [sys.executable, self.app, "source", "--port", "0"],
            cwd=self.directory, env=self.env, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True)
        self.url = json.loads(self.source.stdout.readline())["url"]
        return self.url

    def start_agent(self):
        self.agent = self.call("start", self.root)
        return self.agent

    def stop(self):
        if self.agent is not None:
            try:
                self.call("stop", self.root)
            except Exception:
                pass
        if self.source is not None:
            self.source.terminate()
            try:
                self.source.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                self.source.kill()


def app_or(harness, operation):
    # install is performed by the bootstrap copy; everything else by app.pyz.
    if operation == "install":
        return harness.directory / "bootstrap.pyz"
    return harness.app


def http_json(url, method="GET", body=None):
    import http.client
    parts = urlsplit(url)
    connection = http.client.HTTPConnection(parts.hostname, parts.port or 80, timeout=5)
    try:
        headers = {"Accept-Encoding": "identity"}
        payload = None
        if body is not None:
            payload = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        connection.request(method, "/control" if body is not None else "/requests",
                           body=payload, headers=headers)
        response = connection.getresponse()
        data = response.read()
    finally:
        connection.close()
    return json.loads(data)


def control(url, **settings):
    return http_json(url, "POST", settings)


def requests_of(url):
    return http_json(url, "GET")


def hook(control_dir, phase):
    Path(control_dir).mkdir(parents=True, exist_ok=True)
    (Path(control_dir) / "hook.json").write_text(json.dumps({"phase": phase}))


def wait_ready(control_dir, phase, seconds=60):
    ready = Path(control_dir) / (phase + ".ready")
    wait_for(lambda: ready.exists() and ready.read_text().strip().isdigit(),
             "HOOK_NOT_REACHED:" + phase, seconds)
    return int(ready.read_text())


def unblock(control_dir, phase):
    (Path(control_dir) / (phase + ".go")).write_text("go")


def wait_new_pid(root, old, seconds=60):
    return wait_for(lambda: (lambda p: p if p is not None and p != old else None)(
        service_pid(root)), "AGENT_NOT_RESTARTED", seconds)


def downloads(harness):
    return harness.call("downloads", harness.root)


def only_download(harness):
    rows = downloads(harness)
    assert len(rows) == 1, "DOWNLOAD_ROWS"
    return rows[0]


def wait_state(harness, state, seconds=120):
    def check():
        row = only_download(harness)
        return row if row["state"] == state else None
    return wait_for(check, "DOWNLOAD_NOT_" + state.upper(), seconds)


def ledger_rows(root, batch_id="sample"):
    import sqlite3
    db = sqlite3.connect(str(Path(root) / "state.sqlite"))
    try:
        return db.execute("SELECT COUNT(*) FROM ledger WHERE id=?", (batch_id,)).fetchone()[0]
    finally:
        db.close()


def verify_final(harness, task_id, expected_etag, expected_length, expected_result,
                 expect_ledger=1):
    row = wait_state(harness, "completed")
    assert row["etag"] == expected_etag, "ETAG_CHANGED"
    assert row["received"] == row["length"] == expected_length, "RECEIVED_NOT_COMPLETE"
    raw = Path(harness.root) / "downloads" / (task_id + ".raw")
    body = raw.read_bytes()
    digest = hashlib.sha256(body).hexdigest()
    assert len(body) == expected_length and digest == row["sha256"], "RAW_MISMATCH"
    business = harness.call("read", harness.root, "sample")
    assert business["available"], "BUSINESS_NOT_AVAILABLE"
    assert business["result"] == expected_result, "RESULT_MISMATCH"
    assert business["receipt"] == row["receipt"], "RECEIPT_UNSTABLE"
    assert ledger_rows(harness.root) == expect_ledger, "LEDGER_COUNT"
    return {"download": row, "raw": {"length": len(body), "sha256": digest},
            "business": business}


# ---------------------------------------------------------------------------
# Normal launch (kept as the given real normal-install entry).

def normal(release, directory):
    release = Path(release).resolve()
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=False)
    evidence = {"source_moved": True}
    harness = None
    try:
        harness = Installed(release, directory)
        harness.start_source()
        evidence["accepted"] = harness.call("enqueue", harness.root, "normal", harness.url)
        evidence["agent"] = harness.start_agent()
        evidence["download"] = wait_state(harness, "completed")
        evidence["selftest"] = harness.call("selftest")
        evidence["business"] = harness.call("read", harness.root, "sample")
        assert evidence["business"]["available"]
        raw = (harness.root / "downloads" / "normal.raw").read_bytes()
        assert hashlib.sha256(raw).hexdigest() == evidence["download"]["sha256"]
        assert len(raw) == evidence["download"]["length"] == 262144
        assert evidence["download"]["etag"] == '"portal-1"'
        assert evidence["download"]["receipt"] == evidence["business"]["receipt"]
        assert ledger_rows(harness.root) == 1
        fixture = directory / "expected.json"
        from checks import small
        fixture.write_bytes(pipeline.encode(small()))
        evidence["installed"] = json.loads(command(
            [sys.executable, "-c",
             "import sys,json; sys.path.insert(0,sys.argv[1]); import checks; "
             "print(json.dumps(checks.installed(sys.argv[2],json.load(open(sys.argv[3])))))",
             harness.app, harness.root, fixture], cwd=directory, env=harness.env))
        evidence["regression"] = "INSTALLED_COLLECTION_OK"
        return evidence
    finally:
        if harness is not None:
            harness.stop()
        evidence.setdefault("directory", str(directory))
        (directory / "evidence.json").write_bytes(pipeline.encode(evidence))


# ---------------------------------------------------------------------------
# Recovery drills.  Each uses a fresh ROOT and its own recorded source.

V1_RESULT = [{"station": "east", "minute": 0, "count": 2, "sum": 0},
             {"station": "west", "minute": 1, "count": 1, "sum": 70}]
V2_RESULT = [{"station": "east", "minute": 0, "count": 2, "sum": 0},
             {"station": "west", "minute": 1, "count": 1, "sum": 110}]


def _new_harness(release, directory, name):
    return Installed(release, directory / name)


def truncation(release, directory):
    harness = _new_harness(release, directory, "truncation")
    try:
        url = harness.start_source()
        control(url, cut_after=20000)
        harness.call("enqueue", harness.root, "resume", url)
        harness.start_agent()
        final = verify_final(harness, "resume", '"portal-1"', 262144, V1_RESULT)
        log = requests_of(url)
        cut = [r for r in log if r["method"] == "GET" and r["status"] == 200]
        resumed = [r for r in log if r["method"] == "GET" and r["status"] == 206]
        prefix = 20000
        assert cut and cut[0]["body_bytes"] == prefix, "CUT_NOT_OBSERVED"
        assert resumed and resumed[0]["range"] == "bytes=%d-" % prefix, "NO_SUFFIX_RESUME"
        assert resumed[0]["if_range"] == '"portal-1"', "IF_RANGE_MISSING"
        for entry in resumed:
            offset = int(entry["range"].split("=")[1].rstrip("-"))
            assert offset >= prefix, "CONFIRMED_PREFIX_REFETCHED"
        assert not any(r["method"] == "GET" and r["status"] == 200 and r["body_bytes"] == 0
                       for r in log)
        evidence = {"ok": True, "confirmed_prefix": prefix, "requests": log, **final}
    finally:
        harness.stop()
    (harness.directory / "evidence.json").write_bytes(pipeline.encode(evidence))
    return evidence


def interrupted_download(release, directory):
    harness = _new_harness(release, directory, "interrupted-download")
    try:
        url = harness.start_source()
        hook(harness.control, "chunk")
        harness.call("enqueue", harness.root, "restart", url)
        harness.start_agent()
        old_pid = wait_ready(harness.control, "chunk")
        before = only_download(harness)
        seen = len(requests_of(url))
        assert before["state"] == "downloading", "NOT_DOWNLOADING"
        assert 0 < before["received"] < 262144, "NO_DURABLE_PREFIX"
        os.kill(old_pid, 9)
        new_pid = wait_new_pid(harness.root, old_pid)
        unblock(harness.control, "chunk")
        final = verify_final(harness, "restart", '"portal-1"', 262144, V1_RESULT)
        log = requests_of(url)
        after = log[seen:]
        resumed = [r for r in after if r["method"] == "GET"]
        assert resumed, "NO_RESUME_REQUEST"
        for entry in resumed:
            assert entry["status"] == 206 and entry["if_range"] == '"portal-1"'
            offset = int(entry["range"].split("=")[1].rstrip("-"))
            assert offset >= before["received"], "CONFIRMED_PREFIX_REFETCHED"
        evidence = {"ok": True, "pid_before": old_pid, "pid_after": new_pid,
                    "received_before_restart": before["received"],
                    "requests_before_restart": seen, "requests": log, **final}
    finally:
        harness.stop()
    (harness.directory / "evidence.json").write_bytes(pipeline.encode(evidence))
    return evidence


def version_change(release, directory):
    harness = _new_harness(release, directory, "version-change")
    try:
        url = harness.start_source()
        control(url, cut_after=20000)
        harness.call("enqueue", harness.root, "switch", url)
        harness.call("run", harness.root)
        partial = only_download(harness)
        assert partial["state"] == "queued" and partial["etag"] == '"portal-1"'
        assert 0 < partial["received"] < 262144, "NO_PARTIAL_V1"
        control(url, version=2)
        harness.call("run", harness.root)
        final = verify_final(harness, "switch", '"portal-2"', 270336, V2_RESULT)
        log = requests_of(url)
        assert not any(r["method"] == "GET" and r["status"] == 206 for r in log), "MIXED_VERSION"
        assert not any(r["if_range"] == '"portal-1"' and r["method"] == "GET" for r in log)
        evidence = {"ok": True, "partial": partial, "requests": log, **final}
    finally:
        harness.stop()
    (harness.directory / "evidence.json").write_bytes(pipeline.encode(evidence))
    return evidence


def bad_digest(release, directory):
    harness = _new_harness(release, directory, "bad-digest")
    try:
        url = harness.start_source()
        control(url, bad_digest=True)
        harness.call("enqueue", harness.root, "corrupt", url)
        harness.call("run", harness.root)
        row = wait_state(harness, "failed")
        assert row["error"] == "DIGEST", "WRONG_FAILURE"
        assert not (harness.root / "downloads" / "corrupt.raw").exists(), "BAD_RAW_PUBLISHED"
        assert ledger_rows(harness.root) == 0, "BAD_DIGEST_ACCOUNTED"
        business = harness.call("status", harness.root)
        assert business == [], "BUSINESS_ACCEPTED_CORRUPTION"
        evidence = {"ok": True, "failed": row, "requests": requests_of(url),
                    "ledger": 0, "business": business}
    finally:
        harness.stop()
    (harness.directory / "evidence.json").write_bytes(pipeline.encode(evidence))
    return evidence


def handoff(release, directory, phase, expect_ledger_before, expect_ledger_after=1):
    harness = _new_harness(release, directory, "handoff-" + phase)
    try:
        url = harness.start_source()
        hook(harness.control, phase)
        harness.call("enqueue", harness.root, "handoff", url)
        harness.start_agent()
        old_pid = wait_ready(harness.control, phase)
        raw = harness.root / "downloads" / "handoff.raw"
        assert raw.exists(), "RAW_NOT_READY"
        assert ledger_rows(harness.root) == expect_ledger_before, "LEDGER_BEFORE_RESTART"
        before = only_download(harness)
        seen = len(requests_of(url))
        os.kill(old_pid, 9)
        new_pid = wait_new_pid(harness.root, old_pid)
        unblock(harness.control, phase)
        final = verify_final(harness, "handoff", '"portal-1"', 262144, V1_RESULT,
                             expect_ledger_after)
        log = requests_of(url)
        assert len(log) == seen, "PAYLOAD_REFETCHED_AFTER_READY"
        evidence = {"ok": True, "phase": phase, "pid_before": old_pid,
                    "pid_after": new_pid, "state_before_restart": before["state"],
                    "received_before_restart": before["received"],
                    "ledger_before_restart": expect_ledger_before,
                    "requests": log, **final}
    finally:
        harness.stop()
    (harness.directory / "evidence.json").write_bytes(pipeline.encode(evidence))
    return evidence


def verified(release, directory):
    return handoff(release, directory, "verified", 0)


def accepted(release, directory):
    return handoff(release, directory, "accepted", 0)


def accounted(release, directory):
    return handoff(release, directory, "accounted", 1)


SCENARIOS = (("truncation", truncation),
             ("interrupted_download", interrupted_download),
             ("version_change", version_change),
             ("bad_digest", bad_digest),
             ("handoff_verified", verified),
             ("handoff_accepted", accepted),
             ("handoff_accounted", accounted))


def build_release(directory):
    release = Path(directory) / "release.zip"
    manifest = ops.build(release)
    return release, manifest


def prepare(directory, marker):
    """Make a dedicated evidence directory, refusing to wipe a foreign one."""
    directory = Path(directory).resolve()
    sign = directory / (".pointwise-" + marker)
    if directory.exists() and any(directory.iterdir()):
        if not sign.exists():
            raise SystemExit("EVIDENCE_DIRECTORY_NOT_OWNED: " + str(directory))
        shutil.rmtree(directory)
    directory.mkdir(parents=True, exist_ok=True)
    sign.write_text(marker)
    return directory


def verify(directory=None):
    if directory is None:
        directory = tempfile.mkdtemp(prefix="pointwise-verify-")
    directory = prepare(directory, "verify")
    release, manifest = build_release(directory)
    summary = {"release": {"path": str(release), "sha256": manifest["sha256"]},
               "normal": normal(release, directory / "normal")}
    summary["scenarios"] = {}
    for name, function in SCENARIOS:
        summary["scenarios"][name] = function(release, directory)
    summary["verify"] = "RECOVERY_REGRESSION_OK"
    summary["directory"] = str(directory)
    (directory / "evidence.json").write_bytes(pipeline.encode(summary))
    return summary


def demo(directory=None):
    if directory is None:
        directory = tempfile.mkdtemp(prefix="pointwise-demo-")
    directory = prepare(directory, "demo")
    release, manifest = build_release(directory)
    harness = Installed(release, directory / "demo")
    try:
        url = harness.start_source()
        harness.call("enqueue", harness.root, "demo", url)
        start = time.time()
        harness.start_agent()
        final = verify_final(harness, "demo", '"portal-1"', 262144, V1_RESULT)
        log = requests_of(url)
        download = final["download"]
        evidence = {
            "demo": "REAL_INSTALLED_COLLECTION_OK",
            "release_sha256": manifest["sha256"],
            "root": str(harness.root),
            "source_url": url,
            "source_requests": log,
            "raw": final["raw"],
            "result": final["business"]["result"],
            "receipt": final["business"]["receipt"],
            "ledger_rows": ledger_rows(harness.root),
            "seconds": round(time.time() - start, 2),
            "downloads": downloads(harness),
        }
    finally:
        harness.stop()
    evidence["directory"] = str(directory)
    (directory / "evidence.json").write_bytes(pipeline.encode(evidence))
    return evidence


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=("normal", "verify", "demo"))
    parser.add_argument("arguments", nargs="*")
    args = parser.parse_args()
    if args.operation == "normal":
        if len(args.arguments) != 2:
            raise SystemExit("usage: lab.py normal RELEASE NEW_DIRECTORY")
        value = normal(args.arguments[0], args.arguments[1])
    else:
        if len(args.arguments) > 1:
            raise SystemExit("usage: lab.py %s [DIRECTORY]" % args.operation)
        value = (verify if args.operation == "verify" else demo)(
            args.arguments[0] if args.arguments else None)
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
