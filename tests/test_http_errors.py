import json
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

import pi_bridge as b
import pytest

@pytest.fixture
def server(monkeypatch):
    monkeypatch.setattr(b, "submit", lambda *a, **k: "fake-id")
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), b.Handler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()
    httpd.server_close()

def _post(url, body):
    req = urllib.request.Request(
        url + "/submit", data=json.dumps(body).encode(), method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())

def test_submit_without_prompt_is_400(server):
    assert _post(server, {}) == (400, {"ok": False, "error": "prompt is required"})

def test_submit_with_bad_timeout_is_400(server):
    code, body = _post(server, {"prompt": "x", "timeout": "abc"})
    assert code == 400 and body["error"] == "timeout must be an integer"

def test_submit_with_array_body_is_400(server):
    code, body = _post(server, [])
    assert code == 400 and body["error"] == "body must be a json object"

def test_submit_with_valid_body_is_200(server):
    assert _post(server, {"prompt": "x", "cwd": "/tmp"}) == (200, {"ok": True, "task_id": "fake-id"})
