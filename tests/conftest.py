import json
import os
import tempfile
import time

import pytest

_ROOT = tempfile.mkdtemp(prefix="pi-bridge-test-")
os.environ["PI_BRIDGE_STATE_DIR"] = os.path.join(_ROOT, "state")
os.environ["PI_CODING_AGENT_SESSION_DIR"] = os.path.join(_ROOT, "pi-sessions")
for _k in ("PI_BRIDGE_ORCHESTRATOR_DB", "PI_BRIDGE_ORCHESTRATOR_CONFIG",
           "PI_BRIDGE_NOTIFY_URL", "PI_BRIDGE_HEARTBEAT_URL", "PI_BRIDGE_MODEL",
           "PI_BRIDGE_NOTIFY_SECRET", "PI_BRIDGE_SESSION_ID", "PI_BRIDGE_CHAT_ID",
           "PI_BRIDGE_THREAD_ID", "HERMES_SESSION_ID", "HERMES_SESSION_CHAT_ID",
           "HERMES_SESSION_THREAD_ID"):
    os.environ.pop(_k, None)

import pi_bridge as b


def _mk_task(conn, task_id="t1", **cols):
    fields = {
        "task_id": task_id, "prompt": "p", "cwd": "/tmp", "session_id": f"pi-{task_id}",
        "status": "running", "created_at": 0.0, "started_at": 0.0, "pid": None,
        "log_path": None, "origin_chat_id": None, "origin_thread_id": None, "context": None,
    }
    fields.update(cols)
    conn.execute(
        f"INSERT INTO tasks ({','.join(fields)}) VALUES ({','.join('?' * len(fields))})",
        tuple(fields.values()),
    )
    conn.commit()
    return conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()


def _session_file(session_id, cwd, lines=(), age_s=0.0):
    d = b.pi_session_dir_for(cwd)
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"20260101T000000_{session_id}.jsonl"
    p.write_text("".join(json.dumps(x) + "\n" for x in lines), encoding="utf-8")
    if age_s:
        old = time.time() - age_s
        os.utime(p, (old, old))
    return p


def _activity_line(tool="read", text="now updating the test fixtures"):
    return {"message": {"content": [
        {"type": "toolCall", "name": tool},
        {"type": "text", "text": text},
    ]}}


@pytest.fixture
def mk_task():
    return _mk_task


@pytest.fixture
def session_file():
    return _session_file


@pytest.fixture
def activity_line():
    return _activity_line
