"""Test gate on done, and the fresh-session reviewer pass."""

import json

import pi_bridge as b

def _stub_spawn(monkeypatch):
    captured = {}

    class FakeProc:
        pid = 1

        def wait(self):
            return 0

    def fake_popen(args, **kw):
        captured["args"] = args
        return FakeProc()

    monkeypatch.setattr(b.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(b, "_watch_task_proc", lambda *a, **k: None)
    return captured

def _row(task_id):
    conn = b.db()
    try:
        return conn.execute(
            "SELECT * FROM tasks WHERE task_id=?", (task_id,)
        ).fetchone()
    finally:
        conn.close()

def test_passing_test_cmd_keeps_done(mk_task):
    conn = b.db()
    try:
        mk_task(conn, "t-gate1", test_cmd="true")
    finally:
        conn.close()

    assert b.resolve("t-gate1", "done", text="ok") is True
    row = _row("t-gate1")
    assert row["status"] == "done"
    assert json.loads(row["artifacts_json"])["tests"]["rc"] == 0

def test_failing_test_cmd_marks_failed(mk_task):
    conn = b.db()
    try:
        mk_task(conn, "t-gate2", test_cmd="false")
    finally:
        conn.close()

    assert b.resolve("t-gate2", "done", text="ok") is True
    row = _row("t-gate2")
    assert row["status"] == "failed"
    assert row["result_text"].startswith("tests failed (rc=1)")

def test_no_test_cmd_is_unchanged(mk_task):
    conn = b.db()
    try:
        mk_task(conn, "t-gate3")
    finally:
        conn.close()

    assert b.resolve("t-gate3", "done", text="ok") is True
    row = _row("t-gate3")
    assert row["status"] == "done"
    art = json.loads(row["artifacts_json"]) if row["artifacts_json"] else {}
    assert "tests" not in art

def test_review_prompt_carries_parent_context(monkeypatch, mk_task, tmp_path):
    captured = _stub_spawn(monkeypatch)
    conn = b.db()
    try:
        mk_task(conn, "t-parent1", status="done", prompt="make the widget blue",
                cwd=str(tmp_path))
    finally:
        conn.close()

    new_id = b.review_task(_row("t-parent1"))

    args = captured["args"]
    assert "--session-id" in args
    assert "--session" not in args
    stored = (b.TASKS_DIR / f"{new_id}.txt").read_text(encoding="utf-8")
    assert "Original request:" in stored
    assert "make the widget blue" in stored

def test_review_uses_review_template(monkeypatch, mk_task, tmp_path):
    _stub_spawn(monkeypatch)
    conn = b.db()
    try:
        mk_task(conn, "t-parent2", status="done", prompt="p", cwd=str(tmp_path))
    finally:
        conn.close()

    new_id = b.review_task(_row("t-parent2"))
    stored = (b.TASKS_DIR / f"{new_id}.txt").read_text(encoding="utf-8")
    assert stored.startswith("Review, do not rewrite.")
