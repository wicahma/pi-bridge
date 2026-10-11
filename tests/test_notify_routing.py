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
