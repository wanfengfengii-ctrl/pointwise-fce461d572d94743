"""Normal installed acquisition on macOS. Recovery cases are the assignment."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import zipfile

import ops
import pipeline


def command(argv, **kwargs):
    return subprocess.run([str(x) for x in argv], check=True, capture_output=True,
                          text=True, **kwargs).stdout


def wait_for(predicate, message, seconds=30):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.1)
    raise AssertionError(message)


def service_pid(root):
    import re
    target = "gui/%d/%s" % (os.getuid(), ops.identity(root))
    result = subprocess.run(["launchctl", "print", target], capture_output=True, text=True)
    match = re.search(r"\bpid = ([0-9]+)", result.stdout)
    return int(match[1]) if match else None


def normal(release, directory):
    release = Path(release).resolve()
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=False)
    development = directory / "development"
    development.mkdir()
    with zipfile.ZipFile(io.BytesIO(ops.unpack(release))) as package:
        for name in ops.CODE:
            (development / name).write_bytes(package.read(name))
    rebuilt = directory / "rebuilt.zip"
    command([sys.executable, development / "ops.py", "build", rebuilt], cwd=directory)
    assert rebuilt.read_bytes() == release.read_bytes(), "NONREPRODUCIBLE_PACKAGE"
    development.rename(directory / "development-moved")
    root, control = directory / "service", directory / "control"
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    evidence = {"source_moved": True}
    app = root / "app.pyz"
    source = None
    installed = False

    def call(*args):
        return json.loads(command([sys.executable, app, *args], cwd=directory, env=env))

    try:
        # Installation itself is performed by the rebuilt release, not host imports.
        bootstrap = directory / "bootstrap.pyz"
        bootstrap.write_bytes(ops.unpack(rebuilt))
        command([sys.executable, bootstrap, "install", rebuilt, root,
                 "--control", control], cwd=directory, env=env)
        installed = True
        source = subprocess.Popen([sys.executable, app, "source", "--port", "0"],
                                  cwd=directory, env=env, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, text=True)
        address = json.loads(source.stdout.readline())["url"]
        evidence["accepted"] = call("enqueue", root, "normal", address)
        evidence["agent"] = call("start", root)

        def completed():
            value = call("downloads", root)
            return value[0] if value and value[0]["state"] == "completed" else None

        evidence["download"] = wait_for(completed, "COLLECTION_NOT_COMPLETED")
        evidence["selftest"] = call("selftest")
        evidence["business"] = call("read", root, "sample")
        assert evidence["business"]["available"]
        raw = (root / "downloads" / "normal.raw").read_bytes()
        assert hashlib.sha256(raw).hexdigest() == evidence["download"]["sha256"]
        assert len(raw) == evidence["download"]["length"] == 262144
        assert evidence["download"]["etag"] == '"portal-1"'
        assert evidence["download"]["receipt"] == evidence["business"]["receipt"]
        fixture = directory / "expected.json"
        from checks import small
        fixture.write_bytes(pipeline.encode(small()))
        evidence["installed"] = json.loads(command(
            [sys.executable, "-c",
             "import sys,json; sys.path.insert(0,sys.argv[1]); import checks; "
             "print(json.dumps(checks.installed(sys.argv[2],json.load(open(sys.argv[3])))))",
             app, root, fixture], cwd=directory, env=env))
        evidence["regression"] = "INSTALLED_COLLECTION_OK"
        return evidence
    finally:
        if installed:
            call("stop", root)
        if source is not None:
            source.terminate()
            source.communicate(timeout=5)
        (directory / "evidence.json").write_bytes(pipeline.encode(evidence))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=("normal",))
    parser.add_argument("release")
    parser.add_argument("directory")
    args = parser.parse_args()
    print(json.dumps(normal(args.release, args.directory), sort_keys=True))


if __name__ == "__main__":
    main()
