#!/usr/bin/env python3
"""
Reference orchestrator for pi-bridge.

Stdlib only, one file: submit a task, then listen for the bridge's signed
callback and verify it. The bridge is *deployed* pointed at this process - set
`PI_BRIDGE_NOTIFY_URL` to the address this process binds and
`PI_BRIDGE_NOTIFY_SECRET` to the same shared key on both sides. Nothing here is
specific to this orchestrator; any process that speaks HTTP and HMAC does.
"""

from __future__ import annotations

import argparse
import functools
import hashlib
import hmac
import http.server
import json
import os
import sys
import threading
import urllib.error
import urllib.request


def verify(secret: str, body: bytes, header: str | None) -> bool:
    """Return True only when *header* is the sha256 HMAC of *body* under *secret*."""
    if not header or not header.startswith("sha256="):
        return False
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    try:
        return hmac.compare_digest(header[len("sha256="):], expected)
    except TypeError:
        return False


class Receiver(http.server.BaseHTTPRequestHandler):
    """Verify a notification's signature before parsing it, then hand it to the callback."""

    def __init__(self, request, client_address, server, secret="", path="/hook", callback=None):
        self.secret = secret
        self.hook_path = path
        self.callback = callback
        super().__init__(request, client_address, server)

    def log_message(self, format, *args):
        """Keep the example's own output the only thing on stdout."""

    def do_POST(self):
        """401 anything unsigned, 200 and hand off anything correctly signed."""
        if self.path != self.hook_path:
            self.send_error(404)
            return
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        if not verify(self.secret, body, self.headers.get("X-Hub-Signature-256")):
            self.send_response(401)
            self.end_headers()
            return
        try:
            payload = json.loads(body)
        except ValueError:
            self.send_error(400)
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok":true}')
        if self.callback:
            self.callback(self.headers.get("X-GitHub-Event"), payload)


def main(argv=None) -> int:
    """Bind the receiver, submit one task, then wait for its signed callback."""
    parser = argparse.ArgumentParser(description="Minimal pi-bridge orchestrator: submit a task and verify the callback.")
    parser.add_argument("--bridge", default="http://127.0.0.1:8799", help="bridge base URL")
    parser.add_argument("--listen", default="127.0.0.1:8901", help="host:port this process binds")
    parser.add_argument("--path", default="/hook", help="path the bridge posts the callback to")
    parser.add_argument("--secret", default=os.environ.get("PI_BRIDGE_NOTIFY_SECRET", ""), help="HMAC key (defaults to $PI_BRIDGE_NOTIFY_SECRET)")
    parser.add_argument("--cwd", required=True, help="working directory for the Pi task")
    parser.add_argument("--prompt", required=True, help="the task prompt")
    parser.add_argument("--timeout", type=int, default=900, help="task timeout in seconds, sent to /submit")
    parser.add_argument("--wait", type=int, default=1800, help="how long to wait for the callback, in seconds")
    args = parser.parse_args(argv)

    if not args.secret:
        print("[caller] --secret or PI_BRIDGE_NOTIFY_SECRET is required", file=sys.stderr)
        return 1

    host, _, port = args.listen.rpartition(":")
    received: dict = {}
    done = threading.Event()

    def on_payload(event: str | None, payload: dict) -> None:
        received["event"] = event
        received["payload"] = payload
        done.set()

    handler = functools.partial(Receiver, secret=args.secret, path=args.path, callback=on_payload)
    httpd = http.server.ThreadingHTTPServer((host or "127.0.0.1", int(port)), handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    print(f"[caller] listening on http://{httpd.server_address[0]}:{httpd.server_address[1]}{args.path}")

    try:
        url = args.bridge.rstrip("/") + "/submit"
        body = json.dumps({"prompt": args.prompt, "cwd": args.cwd, "timeout": args.timeout}).encode()
        request = urllib.request.Request(url, data=body, method="POST", headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                status, raw = response.status, response.read()
        except urllib.error.HTTPError as exc:
            status, raw = exc.code, exc.read()
        except OSError as exc:
            print(f"[caller] submit failed: {exc}", file=sys.stderr)
            return 1
        try:
            task_id = json.loads(raw).get("task_id")
        except (ValueError, AttributeError):
            task_id = None
        if not 200 <= status < 300 or not task_id:
            print(f"[caller] submit failed: {status} {raw.decode(errors='replace')}", file=sys.stderr)
            return 1
        print(f"[caller] submitted task {task_id}")

        if not done.wait(args.wait):
            print(f"[caller] no callback within {args.wait}s - the bridge may not be pointed here", file=sys.stderr)
            return 2

        payload = received["payload"]
        print(f"[caller] event={received['event']}")
        print(f"[caller] task_id={payload.get('task_id')}")
        print(f"[caller] status={payload.get('status')}")
        print(f"[caller] result_text={payload.get('result_text')}")
        return 0
    finally:
        httpd.shutdown()
        httpd.server_close()


if __name__ == "__main__":
    sys.exit(main())
