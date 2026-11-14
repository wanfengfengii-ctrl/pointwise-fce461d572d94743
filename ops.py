"""Reproducible packaging and the RestartingAgent user-service scaffold."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import plistlib
import shutil
import subprocess
import sys
import time
import zipfile

import runtime
import collector

CODE = ("pipeline.py", "runtime.py", "ops.py", "checks.py", "lab.py", "collector.py", "portal.py")
MAIN = "from ops import main\nmain()\n"


def archive(items):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_STORED) as target:
        for name, body in sorted(items.items()):
            info = zipfile.ZipInfo(name, date_time=(2020, 1, 1, 0, 0, 0))
            info.external_attr = 0o100644 << 16
            target.writestr(info, body)
    return buffer.getvalue()


def build(destination):
    source = Path(__file__).resolve().parent
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    app = archive({**{name: (source / name).read_bytes() for name in CODE},
                   "__main__.py": MAIN.encode()})
    manifest = {"format": 1, "sha256": hashlib.sha256(app).hexdigest()}
    destination.write_bytes(archive({"app.pyz": app,
                                     "manifest.json": runtime.encode(manifest)}))
    return manifest


def unpack(release):
    with zipfile.ZipFile(release) as package:
        if sorted(package.namelist()) != ["app.pyz", "manifest.json"]:
            raise ValueError("PACKAGE_LAYOUT")
        manifest = json.loads(package.read("manifest.json"))
        app = package.read("app.pyz")
    if manifest != {"format": 1, "sha256": hashlib.sha256(app).hexdigest()}:
        raise ValueError("PACKAGE_HASH")
    return app


def identity(root):
    return "local.sampling." + hashlib.sha256(str(Path(root).resolve()).encode()).hexdigest()[:16]


def agent_path(root):
    return Path.home() / "Library" / "LaunchAgents" / (identity(root) + ".plist")


def install(release, root, control):
    app = unpack(release)  # Verify before creating or changing ROOT.
    root = Path(root).resolve()
    if root.exists() and any(root.iterdir()):
        raise ValueError("INSTALL_TARGET_NOT_EMPTY")
    runtime.init(root, control)
    collector.init(root)
    (root / "app.pyz").write_bytes(app)
    return {"root": str(root), "sha256": hashlib.sha256(app).hexdigest()}


def start(root):
    root = Path(root).resolve()
    control = Path(json.loads((root / "config.json").read_bytes())["control"])
    control.mkdir(parents=True, exist_ok=True)
    config = {
        "Label": identity(root),
        "ProgramArguments": [str(Path(sys.executable).resolve()), str(root / "app.pyz"),
                             "worker", str(root)],
        "WorkingDirectory": str(root),
        "RunAtLoad": True,
        "KeepAlive": True,
        "ThrottleInterval": 2,
        "StandardOutPath": str(control / "service.stdout"),
        "StandardErrorPath": str(control / "service.stderr"),
    }
    path = agent_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(plistlib.dumps(config))
    if shutil.which("launchctl"):
        domain = "gui/" + str(os.getuid())
        registered = subprocess.run(["launchctl", "print", domain + "/" + identity(root)],
                                    capture_output=True).returncode == 0
        if not registered:
            subprocess.run(["launchctl", "bootstrap", domain, str(path)], check=True)
        subprocess.run(["launchctl", "kickstart", domain + "/" + identity(root)], check=True)
    else:
        # Non-macOS development hosts still get a supervised process with the
        # same restart contract, using the installed personal-agent plist.
        _spawn_fallback(root, control)
    return {"label": identity(root), "plist": str(path)}


def _fallback_state(root):
    root = Path(root).resolve()
    control = Path(json.loads((root / "config.json").read_bytes())["control"])
    return control / "fallback-agent.json"


_SUPERVISOR = """
import json, os, subprocess, sys, time
app, root, state = sys.argv[1:4]
executable = sys.executable
pidfile = state + ".tmp"
while True:
    child = subprocess.Popen([executable, app, "worker", root], cwd=root,
                             stdin=subprocess.DEVNULL, stdout=sys.stderr, stderr=sys.stderr)
    with open(pidfile, "w") as target:
        json.dump({"pid": child.pid, "supervisor": os.getpid()}, target)
    os.replace(pidfile, state)
    child.wait()
    time.sleep(2)
"""


def _spawn_fallback(root, control):
    control.mkdir(parents=True, exist_ok=True)
    state = _fallback_state(root)
    if state.exists():
        try:
            record = json.loads(state.read_text())
        except ValueError:
            record = {}
        if all(isinstance(record.get(key), int) and _alive(record[key])
               for key in ("pid", "supervisor")):
            return
        state.unlink(missing_ok=True)
    logs = [open(control / "service.stdout", "ab"), open(control / "service.stderr", "ab")]
    process = subprocess.Popen(
        [str(Path(sys.executable).resolve()), "-c", _SUPERVISOR,
         str(Path(root) / "app.pyz"), str(root), str(state)],
        cwd=str(root), stdin=subprocess.DEVNULL, stdout=logs[0], stderr=logs[1],
        start_new_session=True)
    for _ in range(100):
        if state.exists():
            return
        time.sleep(0.05)
    raise RuntimeError("AGENT_NOT_READY")


def _alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _stop_fallback(root):
    state = _fallback_state(root)
    if not state.exists():
        return
    try:
        record = json.loads(state.read_text())
    except ValueError:
        record = {}
    supervisor = record.get("supervisor")
    child = record.get("pid")
    if isinstance(supervisor, int):
        try:
            os.kill(supervisor, 15)  # Stop respawning before the child.
        except ProcessLookupError:
            pass
    if isinstance(child, int):
        try:
            os.kill(child, 15)
        except ProcessLookupError:
            pass
    for pid in (supervisor, child):
        if not isinstance(pid, int):
            continue
        for _ in range(50):
            if not _alive(pid):
                break
            time.sleep(0.1)
        else:
            try:
                os.kill(pid, 9)
            except ProcessLookupError:
                pass
    state.unlink(missing_ok=True)


def stop(root):
    if shutil.which("launchctl"):
        target = "gui/" + str(os.getuid()) + "/" + identity(root)
        subprocess.run(["launchctl", "bootout", target], capture_output=True)
    else:
        _stop_fallback(root)
    agent_path(root).unlink(missing_ok=True)
    return {"stopped": identity(root)}


def main(argv=None):
    parser = argparse.ArgumentParser()
    subs = parser.add_subparsers(dest="operation", required=True)
    build_parser = subs.add_parser("build")
    build_parser.add_argument("release")
    install_parser = subs.add_parser("install")
    install_parser.add_argument("release")
    install_parser.add_argument("root")
    install_parser.add_argument("--control", required=True)
    for name in ("start", "stop", "status", "downloads", "run", "worker"):
        subs.add_parser(name).add_argument("root")
    for name, argument in (("submit", "input"), ("read", "batch_id")):
        item = subs.add_parser(name)
        item.add_argument("root")
        item.add_argument(argument)
    task = subs.add_parser("enqueue")
    task.add_argument("root")
    task.add_argument("task_id")
    task.add_argument("url")
    source = subs.add_parser("source")
    source.add_argument("--port", type=int, default=8765)
    subs.add_parser("selftest")
    args = parser.parse_args(argv)
    if args.operation == "build":
        value = build(args.release)
    elif args.operation == "install":
        value = install(args.release, args.root, args.control)
    elif args.operation == "start":
        value = start(args.root)
    elif args.operation == "stop":
        value = stop(args.root)
    elif args.operation == "status":
        value = runtime.status(args.root)
    elif args.operation == "downloads":
        value = collector.status(args.root)
    elif args.operation == "enqueue":
        value = collector.enqueue(args.root, args.task_id, args.url)
    elif args.operation == "source":
        import portal
        portal.main(["--port", str(args.port)])
        return
    elif args.operation == "submit":
        value = runtime.submit(args.root, json.loads(Path(args.input).read_bytes()))
    elif args.operation == "read":
        value = runtime.read(args.root, args.batch_id)
    elif args.operation == "run":
        processed = collector.run_one(args.root)
        processed = runtime.run_one(args.root) or processed
        value = {"processed": processed}
    elif args.operation == "worker":
        collector.worker(args.root)
        return
    else:
        import checks
        value = {"business": checks.normal(), "source": checks.source()}
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
