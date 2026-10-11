import functools
import hashlib
import hmac
import importlib.util
import json
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    "generic_caller", Path(__file__).resolve().parent.parent / "integrations" / "generic" / "caller.py"
)
assert _spec is not None and _spec.loader is not None
caller = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(caller)

KEY = "shared-key"
BODY = b'{"task_id":"abc","status":"succeeded","result_text":"done"}'

def _sign(body, secret=KEY):
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()

def test_verify_accepts_correct_signature():
    assert caller.verify(KEY, BODY, _sign(BODY)) is True

def test_verify_rejects_wrong_secret():
    assert caller.verify(KEY, BODY, _sign(BODY, "other")) is False

def test_verify_rejects_tampered_body():
    assert caller.verify(KEY, BODY + b" ", _sign(BODY)) is False

def test_verify_rejects_malformed_headers():
    for header in (None, "", hashlib.sha256(BODY).hexdigest(), "sha256=zzzz"):
        assert caller.verify(KEY, BODY, header) is False

def test_receiver_round_trip():
    seen = []
    called = threading.Event()

    def on_payload(event, payload):
        seen.append((event, payload))
        called.set()

    handler = functools.partial(caller.Receiver, secret=KEY, path="/hook", callback=on_payload)
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{httpd.server_address[1]}/hook"
    try:
        req = urllib.request.Request(
            url, data=BODY, method="POST",
            headers={"Content-Type": "application/json", "X-Hub-Signature-256": _sign(BODY), "X-GitHub-Event": "pi.settled"},
        )
        with urllib.request.urlopen(req, timeout=5) as r:
            assert r.status == 200
        assert called.wait(5)
        assert seen == [("pi.settled", json.loads(BODY))]

        unsigned = urllib.request.Request(url, data=BODY, method="POST", headers={"Content-Type": "application/json"})
        with pytest.raises(urllib.error.HTTPError) as exc:
            urllib.request.urlopen(unsigned, timeout=5)
        assert exc.value.code == 401
        assert len(seen) == 1
    finally:
        httpd.shutdown()
        httpd.server_close()
