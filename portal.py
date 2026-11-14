"""Public, mutable HTTP recording source; no credentials or external service."""
import argparse
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import re
import threading

import pipeline


class Portal:
    def __init__(self):
        from checks import small
        first = small()
        second = small()
        second["rows"][2]["value"] = 11
        self.versions = {}
        for version, batch, size in ((1, first, 262144), (2, second, 270336)):
            raw = pipeline.encode(batch)
            self.versions[version] = raw + b" " * (size - len(raw))
        self.version = 1
        self.cut_after = None
        self.bad_digest = False
        self.requests = []
        self.lock = threading.Lock()

    def reply(self, method, path, headers, body=b""):
        with self.lock:
            if path == "/control" and method == "POST":
                config = json.loads(body)
                if set(config) - {"version", "cut_after", "bad_digest"}:
                    raise ValueError("CONTROL")
                if "version" in config:
                    if type(config["version"]) is not int or config["version"] not in self.versions:
                        raise ValueError("VERSION")
                if "cut_after" in config:
                    n = config["cut_after"]
                    if n is not None and (type(n) is not int or n < 0):
                        raise ValueError("CUT")
                if "bad_digest" in config and type(config["bad_digest"]) is not bool:
                    raise ValueError("DIGEST_FLAG")
                for key, value in config.items():
                    setattr(self, key, value)
                return 200, {"Content-Length": "2"}, b"{}"
            if path == "/requests" and method == "GET":
                raw = pipeline.encode(self.requests)
                return 200, {"Content-Length": str(len(raw))}, raw
            if path != "/export" or method not in ("GET", "HEAD"):
                return 404, {"Content-Length": "0"}, b""
            version = self.version
            raw = self.versions[version]
            etag = '"portal-%d"' % version
            digest = hashlib.sha256(raw).hexdigest() if not self.bad_digest else "0" * 64
            output = {"ETag": etag, "X-Content-SHA256": digest, "Accept-Ranges": "bytes",
                      "Content-Type": "application/json", "Content-Length": str(len(raw))}
            selected, status = raw, 200
            requested = headers.get("range")
            validator = headers.get("if-range")
            if method == "GET" and requested and (validator is None or validator == etag):
                match = re.fullmatch(r"bytes=(\d+)-", requested)
                if not match or int(match[1]) >= len(raw):
                    status, selected = 416, b""
                    output["Content-Range"] = "bytes */%d" % len(raw)
                else:
                    offset = int(match[1])
                    status, selected = 206, raw[offset:]
                    output["Content-Range"] = "bytes %d-%d/%d" % (offset, len(raw)-1, len(raw))
                output["Content-Length"] = str(len(selected))
            if method == "HEAD":
                selected = b""
            if method == "GET" and self.cut_after is not None:
                selected = selected[:self.cut_after]
                self.cut_after = None
            entry = {"method": method, "path": path, "range": requested, "if_range": validator,
                     "version": version, "status": status, "body_bytes": len(selected)}
            self.requests.append(entry)
            return status, output, selected


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def handle_request(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        try:
            status, headers, data = self.server.portal.reply(
                self.command, self.path, {k.lower(): v for k, v in self.headers.items()}, body)
        except (ValueError, TypeError):
            status, headers, data = 400, {"Content-Length": "0"}, b""
        self.send_response(status)
        for key, value in headers.items():
            self.send_header(key, value)
        self.send_header("Connection", "close")
        self.end_headers()
        try:
            for offset in range(0, len(data), 8192):
                self.wfile.write(data[offset:offset+8192])
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        self.close_connection = True

    do_GET = do_HEAD = do_POST = handle_request

    def log_message(self, *args):
        pass


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args(argv)
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    server.daemon_threads = True
    server.portal = Portal()
    print(json.dumps({"url": "http://127.0.0.1:%d/export" % server.server_port}), flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
