import json

import pi_bridge as b


def test_task_status_working(monkeypatch, mk_task, session_file, activity_line):
    monkeypatch.setattr(b, "pid_alive", lambda _p: True)
    session_file("pi-w", "/tmp/w", lines=[activity_line()])
    row = mk_task(b.db(), "w", cwd="/tmp/w", session_id="pi-w", pid=1)
    got = b.task_status(row)
    assert got["state"] == "working"
    assert got["iteration"] == 1
    assert got["last_tool"] == "read"
    assert got["detail"] == "now updating the test fixtures"


def test_task_status_idle(monkeypatch, mk_task, session_file, activity_line):
    monkeypatch.setattr(b, "pid_alive", lambda _p: True)
    session_file("pi-i", "/tmp/i", lines=[activity_line()],
                 age_s=b.HEARTBEAT_INTERVAL * 10)
    row = mk_task(b.db(), "i", cwd="/tmp/i", session_id="pi-i", pid=1)
    got = b.task_status(row)
    assert got["state"] == "idle"
    assert "no session activity for" in got["detail"]


def test_task_status_dne(monkeypatch, mk_task):
    monkeypatch.setattr(b, "pid_alive", lambda _p: False)
    row = mk_task(b.db(), "d", cwd="/tmp/d", session_id="pi-d", pid=1)
    assert b.task_status(row)["state"] == "dne"


def test_task_status_error_needs_a_real_error(monkeypatch, mk_task, tmp_path):
    monkeypatch.setattr(b, "pid_alive", lambda _p: True)
    log = tmp_path / "e.log"
    log.write_text('Error: Model "nope" not found.\n', encoding="utf-8")
    row = mk_task(b.db(), "e", cwd="/tmp/e", session_id="pi-e", pid=1, log_path=str(log))
    got = b.task_status(row)
    assert got["state"] == "error"
    assert "not found" in got["detail"]


def test_task_status_startup_warnings_are_not_an_error(monkeypatch, mk_task, tmp_path):
    monkeypatch.setattr(b, "pid_alive", lambda _p: True)
    log = tmp_path / "w.log"
    log.write_text(
        "Warning: No project session found with id 'pi-s'; creating a new session with that id.\n"
        'Warning: Model "x" not found for provider "local". Using custom model id.\n',
        encoding="utf-8",
    )
    row = mk_task(b.db(), "s", cwd="/tmp/s", session_id="pi-s", pid=1, log_path=str(log))
    assert b.task_status(row)["state"] == "working"


def test_context_is_merged_and_reserved_keys_protected(mk_task):
    row = mk_task(b.db(), "c1", context=json.dumps(
        {"chat_id": "c9", "run_id": "n42", "task_id": "evil", "status": "evil", "drop": None}))
    got = b._wake_payload(row)
    assert got["task_id"] == "c1"
    assert got["status"] == "running"
    assert got["chat_id"] == "c9"
    assert got["run_id"] == "n42"
    assert "drop" not in got
    assert b.task_status(row)["chat_id"] == "c9"


def test_context_origin_columns_win_over_blob(mk_task):
    row = mk_task(b.db(), "c2", context=json.dumps({"chat_id": "from-blob"}),
                  origin_chat_id="from-column", origin_thread_id="t7")
    got = b._wake_payload(row)
    assert got["chat_id"] == "from-column"
    assert got["thread_id"] == "t7"


def test_context_bad_json_is_ignored(mk_task):
    row = mk_task(b.db(), "c3", context="not json at all")
    got = b._wake_payload(row)
    assert "chat_id" not in got and "thread_id" not in got


def test_wake_payload_truncates_result_text(mk_task):
    row = mk_task(b.db(), "c4", status="done", result_text="x" * 5000)
    assert len(b._wake_payload(row)["result_text"]) == 4000


def test_post_signed_signs_and_never_raises(monkeypatch):
    import hashlib
    import hmac
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer

    monkeypatch.setattr(b, "NOTIFY_SECRET", "test-secret")
    seen = {}

    class H(BaseHTTPRequestHandler):
        def do_POST(self):
            seen["body"] = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            seen["sig"] = self.headers.get("X-Hub-Signature-256")
            seen["event"] = self.headers.get("X-GitHub-Event")
            seen["delivery"] = self.headers.get("X-GitHub-Delivery")
            self.send_response(200)
            self.end_headers()

        def log_message(self, *a):
            pass

    srv = HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        b._post_signed(f"http://127.0.0.1:{srv.server_port}/x",
                       {"task_id": "t9", "delivery_id": "d9"}, "pi-status")
        want = hmac.new(b"test-secret", seen["body"], hashlib.sha256).hexdigest()
        assert seen["sig"] == f"sha256={want}"
        assert seen["event"] == "pi-status"
        assert seen["delivery"] == "d9"
    finally:
        srv.shutdown()

    b._post_signed("http://127.0.0.1:1/nope", {"task_id": "t9"}, "pi-task")
    b._post_signed("", {"task_id": "t9"}, "pi-task")


def test_resolve_is_idempotent(mk_task):
    conn = b.db()
    mk_task(conn, "r1")
    assert b.resolve("r1", "done", text="first") is True
    assert b.resolve("r1", "failed", text="second") is False
    row = conn.execute("SELECT * FROM tasks WHERE task_id='r1'").fetchone()
    assert row["status"] == "done" and row["result_text"] == "first"


def test_resolve_unknown_task_is_false():
    assert b.resolve("nope", "done") is False


def test_handle_callback_maps_status(mk_task):
    conn = b.db()
    mk_task(conn, "h1")
    mk_task(conn, "h2")
    assert b.handle_callback({"task_id": "h1", "status": "done", "text": "ok"}) is True
    assert b.handle_callback({"task_id": "h2", "status": "failed",
                              "stop_reason": "error", "error": "boom"}) is True
    assert b.handle_callback({"status": "done"}) is False
    r1 = conn.execute("SELECT * FROM tasks WHERE task_id='h1'").fetchone()
    r2 = conn.execute("SELECT * FROM tasks WHERE task_id='h2'").fetchone()
    assert r1["status"] == "done" and r1["resolution"] == "callback"
    assert r2["status"] == "failed" and "boom" in r2["result_text"]


def test_resolve_model_explicit_and_env(monkeypatch):
    assert b.resolve_model("chosen") == ("chosen", "explicit")
    monkeypatch.setenv("PI_BRIDGE_MODEL", "from-env")
    assert b.resolve_model() == ("from-env", "env")


def test_resolve_model_falls_back(monkeypatch):
    monkeypatch.delenv("PI_BRIDGE_MODEL", raising=False)
    monkeypatch.setattr(b, "active_model_from_state", lambda _s=None: (None, None))
    monkeypatch.setattr(b, "config_default_model", lambda: None)
    assert b.resolve_model() == (b.FALLBACK_MODEL, "fallback")


def test_resolve_model_skips_alias_when_a_concrete_default_exists(monkeypatch):
    monkeypatch.delenv("PI_BRIDGE_MODEL", raising=False)
    monkeypatch.setattr(b, "_ALIAS_PREFIXES", ("hermes",))
    monkeypatch.setattr(b, "active_model_from_state", lambda _s=None: ("hermes", None))
    monkeypatch.setattr(b, "config_default_model", lambda: "real-model")
    assert b.resolve_model() == ("real-model", "config")


def test_resolve_model_uses_alias_when_nothing_else(monkeypatch):
    monkeypatch.delenv("PI_BRIDGE_MODEL", raising=False)
    monkeypatch.setattr(b, "_ALIAS_PREFIXES", ("hermes",))
    monkeypatch.setattr(b, "active_model_from_state", lambda _s=None: ("hermes", None))
    monkeypatch.setattr(b, "config_default_model", lambda: None)
    assert b.resolve_model() == ("hermes", "session-alias")


def test_looks_like_alias(monkeypatch):
    monkeypatch.setattr(b, "_ALIAS_PREFIXES", ("hermes", "code-reviewer"))
    assert b._looks_like_alias("hermes")
    assert b._looks_like_alias("code-reviewer")
    assert not b._looks_like_alias("gpt-4o-mini")
    assert not b._looks_like_alias("")
