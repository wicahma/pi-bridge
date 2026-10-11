import json
import sqlite3

import pi_bridge as b


def _origin_db(path):
    conn = sqlite3.connect(path)
    conn.execute(
        """CREATE TABLE sessions (
               id TEXT, chat_id TEXT, thread_id TEXT, model TEXT,
               ended_at REAL, last_activity_at REAL)"""
    )
    return conn


def test_task_context_plain_dm_gets_empty_thread(mk_task):
    row = mk_task(b.db(), "n1", origin_chat_id="1274076905", origin_thread_id=None)
    got = b._task_context(row)
    assert got["chat_id"] == "1274076905"
    assert got["thread_id"] == ""


def test_task_context_no_route_emits_empty_pair(mk_task):
    row = mk_task(b.db(), "n2", origin_chat_id=None, origin_thread_id=None)
    got = b._task_context(row)
    assert got["chat_id"] == "" and got["thread_id"] == ""

def test_task_context_keeps_blob_route_when_no_origin(mk_task):
    row = mk_task(
        b.db(), "n2b", origin_chat_id=None,
        context=json.dumps({"chat_id": "from-blob", "thread_id": "from-blob-t"}),
    )
    got = b._task_context(row)
    assert got["chat_id"] == "from-blob" and got["thread_id"] == "from-blob-t"


def test_task_context_both_route_keys_pass_through(mk_task):
    row = mk_task(b.db(), "n3", origin_chat_id="c1", origin_thread_id="t9")
    got = b._task_context(row)
    assert got["chat_id"] == "c1" and got["thread_id"] == "t9"


def test_task_context_blob_survives_and_reserved_keys_stripped(mk_task):
    row = mk_task(b.db(), "n4", context=json.dumps(
        {"chat_id": "from-blob", "thread_id": "from-blob-t", "run_id": "r1",
         "task_id": "evil", "status": "evil", "state": "evil", "delivery_id": "evil"}))
    got = b._task_context(row)
    assert got["chat_id"] == "from-blob"
    assert got["thread_id"] == "from-blob-t"
    assert got["run_id"] == "r1"
    for reserved in ("task_id", "status", "state", "delivery_id"):
        assert reserved not in got


def test_active_session_origin_skips_webhook_sessions(monkeypatch, tmp_path):
    db = tmp_path / "orchestrator.db"
    conn = _origin_db(db)
    conn.execute("INSERT INTO sessions VALUES ('s-webhook','webhook:v2:abc',NULL,'m',NULL,200.0)")
    conn.execute("INSERT INTO sessions VALUES ('s-real','1274076905',NULL,'m',NULL,100.0)")
    conn.commit()
    conn.close()
    monkeypatch.setattr(b, "ORCHESTRATOR_DB", db)
    assert b.active_session_origin() == ("s-real", "1274076905", None)


def test_active_session_origin_none_when_only_unroutable(monkeypatch, tmp_path):
    db = tmp_path / "orchestrator.db"
    conn = _origin_db(db)
    conn.execute("INSERT INTO sessions VALUES ('s-webhook','webhook:v2:abc',NULL,'m',NULL,200.0)")
    conn.execute("INSERT INTO sessions VALUES ('s-null',NULL,NULL,'m',NULL,150.0)")
    conn.commit()
    conn.close()
    monkeypatch.setattr(b, "ORCHESTRATOR_DB", db)
    assert b.active_session_origin() == (None, None, None)


def test_env_route_prefers_pi_bridge_names(monkeypatch):
    monkeypatch.setenv("PI_BRIDGE_SESSION_ID", "s-pi")
    monkeypatch.setenv("PI_BRIDGE_CHAT_ID", "c-pi")
    monkeypatch.setenv("PI_BRIDGE_THREAD_ID", "t-pi")
    monkeypatch.setenv("HERMES_SESSION_ID", "s-hermes")
    monkeypatch.setenv("HERMES_SESSION_CHAT_ID", "c-hermes")
    monkeypatch.setenv("HERMES_SESSION_THREAD_ID", "t-hermes")
    assert b._env_route() == ("s-pi", "c-pi", "t-pi")


def test_env_route_falls_back_to_hermes_names(monkeypatch):
    monkeypatch.setenv("HERMES_SESSION_ID", "s-hermes")
    monkeypatch.setenv("HERMES_SESSION_CHAT_ID", "1274076905")
    monkeypatch.setenv("HERMES_SESSION_THREAD_ID", "3482164")
    assert b._env_route() == ("s-hermes", "1274076905", "3482164")


def test_env_route_empty_when_unset(monkeypatch):
    assert b._env_route() == (None, None, None)


def test_submit_routes_from_hermes_env_when_no_session(monkeypatch, tmp_path):
    db = tmp_path / "orchestrator.db"
    conn = _origin_db(db)
    conn.execute("INSERT INTO sessions VALUES ('s-other','999',NULL,'m',NULL,200.0)")
    conn.execute("INSERT INTO sessions VALUES ('s-me','1274076905','3482164','m',NULL,100.0)")
    conn.commit()
    conn.close()
    monkeypatch.setattr(b, "ORCHESTRATOR_DB", db)
    monkeypatch.setenv("HERMES_SESSION_ID", "s-me")
    monkeypatch.setattr(b, "resolve_model", lambda *a, **k: ("hermes", "session-alias"))
    monkeypatch.setattr(b, "fit_model_to_pi", lambda m, p: (m, p, None))
    monkeypatch.setattr(b, "ensure_provider_placeholder", lambda: None)

    class _Proc:
        pid = 4242

    monkeypatch.setattr(b.subprocess, "Popen", lambda *a, **k: _Proc())
    tid = b.submit("probe", "/tmp")
    row = b.db().execute("SELECT * FROM tasks WHERE task_id=?", (tid,)).fetchone()
    assert row["origin_chat_id"] == "1274076905"
    assert row["origin_thread_id"] == "3482164"


def test_submit_does_not_route_to_guessed_session(monkeypatch, tmp_path):
    db = tmp_path / "orchestrator.db"
    conn = _origin_db(db)
    conn.execute("INSERT INTO sessions VALUES ('s-other','999',NULL,'m',NULL,200.0)")
    conn.commit()
    conn.close()
    monkeypatch.setattr(b, "ORCHESTRATOR_DB", db)
    monkeypatch.setattr(b, "resolve_model", lambda *a, **k: ("hermes", "session-alias"))
    monkeypatch.setattr(b, "fit_model_to_pi", lambda m, p: (m, p, None))
    monkeypatch.setattr(b, "ensure_provider_placeholder", lambda: None)

    class _Proc:
        pid = 4243

    monkeypatch.setattr(b.subprocess, "Popen", lambda *a, **k: _Proc())
    tid = b.submit("probe", "/tmp")
    row = b.db().execute("SELECT * FROM tasks WHERE task_id=?", (tid,)).fetchone()
    assert row["origin_chat_id"] is None
    assert row["origin_thread_id"] is None
