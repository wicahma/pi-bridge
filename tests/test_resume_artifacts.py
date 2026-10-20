"""Resume, artifact capture and per-repo cost accounting."""

import json
import subprocess

import pytest

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

def _git_repo(path):
    subprocess.run(["git", "init", "-q", str(path)], check=True)
    (path / "f.txt").write_text("hi\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(path), "add", "f.txt"], check=True)
    subprocess.run(
        ["git", "-C", str(path), "-c", "user.email=t@t", "-c", "user.name=t",
         "commit", "-q", "-m", "init"],
        check=True,
    )
    return path

def _row(task_id):
    conn = b.db()
    try:
        return conn.execute(
            "SELECT * FROM tasks WHERE task_id=?", (task_id,)
        ).fetchone()
    finally:
        conn.close()

def test_resume_reuses_parent_session_and_cwd(monkeypatch, mk_task, tmp_path):
    captured = _stub_spawn(monkeypatch)
    conn = b.db()
    try:
        mk_task(conn, "t-res1", session_id="pi-parent", cwd=str(tmp_path))
    finally:
        conn.close()

    new_id = b.submit("again", cwd="/elsewhere", resume_of="t-res1")

    assert "--session" in captured["args"]
    assert "--session-id" not in captured["args"]
    row = _row(new_id)
    assert row["session_id"] == "pi-parent"
    assert row["cwd"] == str(tmp_path)
    assert row["parent_task_id"] == "t-res1"

def test_resume_unknown_task_raises(monkeypatch):
    _stub_spawn(monkeypatch)
    with pytest.raises(ValueError):
        b.submit("again", cwd="/tmp", resume_of="nope")

def test_fresh_submit_still_uses_session_id(monkeypatch):
    captured = _stub_spawn(monkeypatch)
    new_id = b.submit("fresh", cwd="/tmp")
    assert "--session-id" in captured["args"]
    assert "--session" not in captured["args"]
    assert _row(new_id)["parent_task_id"] is None

def test_capture_artifacts_on_git_repo(tmp_path):
    repo = _git_repo(tmp_path)
    art = b.capture_artifacts(str(repo))
    assert art is not None
    assert art["head"]
    assert art["branch"]

def test_capture_artifacts_outside_git(tmp_path):
    assert b.capture_artifacts(str(tmp_path)) is None

def test_resolve_stores_artifacts(mk_task, tmp_path):
    repo = _git_repo(tmp_path)
    conn = b.db()
    try:
        mk_task(conn, "t-art1", cwd=str(repo))
    finally:
        conn.close()

    assert b.resolve("t-art1", "done", text="ok") is True
    art = json.loads(_row("t-art1")["artifacts_json"])
    assert art["head"]
