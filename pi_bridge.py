#!/usr/bin/env python3
"""
Pi agent bridge.

Supervise the Pi coding agent from any orchestrator: dispatch a task and return
immediately, then learn the outcome from a signed push callback. The bridge is
deliberately orchestrator-agnostic - it stores whatever opaque context the caller
supplies and echoes it back in notifications, so it never needs to know what a
chat, topic or thread is.
Spawning a Pi task is fire-and-forget: this process never blocks on Pi.
Completion arrives by push callback (Pi -> here), and the watchdog closes the
two gaps a push-only design leaves open:

  1. Pi dies mid-task        -> no callback ever arrives. Watchdog sees the pid
                                is gone, then resolves the outcome from the Pi
                                session file.
  2. This process restarts   -> callbacks in flight are lost. reconcile() runs
                                on startup and re-resolves every task still
                                marked 'running'.

State is SQLite so a restart never loses a task. Everything is idempotent:
a task is resolved exactly once, whichever path gets there first.

Layout
------
  $PI_BRIDGE_STATE_DIR/state.db    task state
  $PI_BRIDGE_STATE_DIR/tasks/      one prompt file per task
  $PI_BRIDGE_STATE_DIR/logs/       per-task stdout/stderr
State defaults to $XDG_STATE_HOME/pi-bridge (~/.local/state/pi-bridge).

Usage
-----
  python3 pi_bridge.py serve                 # callback receiver + watchdog
  python3 pi_bridge.py submit "<prompt>" --cwd DIR [--model M] [--timeout S]
  python3 pi_bridge.py status <task_id>
  python3 pi_bridge.py list
  python3 pi_bridge.py reconcile             # manual reconcile pass
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

# ---------------------------------------------------------------- config

HOME = Path.home()
_XDG_STATE = Path(os.environ.get("XDG_STATE_HOME") or HOME / ".local" / "state")
BRIDGE_DIR = Path(
    os.environ.get("PI_BRIDGE_STATE_DIR")
    or os.environ.get("PI_BRIDGE_DIR")
    or _XDG_STATE / "pi-bridge"
)
TASKS_DIR = BRIDGE_DIR / "tasks"
LOGS_DIR = BRIDGE_DIR / "logs"
DB_PATH = BRIDGE_DIR / "state.db"

CALLBACK_HOST = os.environ.get("PI_BRIDGE_HOST", "127.0.0.1")
CALLBACK_PORT = int(os.environ.get("PI_BRIDGE_PORT", "8799"))
CALLBACK_PATH = "/callback"

# ---------------------------------------------------------------- wake layer

# Optional: POST a signed JSON notification to an HTTP endpoint when a task
# reaches a terminal state, so an idle orchestrator can be woken instead of
# polled. Entirely additive - if unconfigured or failing, task resolution is
# unaffected.
NOTIFY_URL = os.environ.get("PI_BRIDGE_NOTIFY_URL", "")
NOTIFY_SECRET = os.environ.get("PI_BRIDGE_NOTIFY_SECRET", "")

# Optional: periodic progress heartbeat, posted to its own endpoint so a long run
# is never silent. Unset -> no heartbeat.
HEARTBEAT_URL = os.environ.get("PI_BRIDGE_HEARTBEAT_URL", "")
HEARTBEAT_INTERVAL = int(os.environ.get("PI_BRIDGE_HEARTBEAT_INTERVAL", "60"))
# Silence longer than this still produces one line, so a wedged task is not
# indistinguishable from a busy one. 0 disables the keepalive entirely.
HEARTBEAT_IDLE = int(os.environ.get("PI_BRIDGE_HEARTBEAT_IDLE", "600"))
STREAM = int(os.environ.get("PI_BRIDGE_STREAM", "0") or 0)
STREAM_MAX = max(1, int(os.environ.get("PI_BRIDGE_STREAM_MAX", "6") or 6))


def _task_context(row: sqlite3.Row) -> dict:
    """Routing context to attach to every notification for this task.

    Merges the caller-supplied opaque `context` blob with the orchestrator's
    session route when one was resolved at submit time. Keys the engine owns are
    never overwritten, and null values in the caller's blob are dropped so a
    receiver sees an absent key rather than a null one. The orchestrator-resolved
    route is always emitted as a `chat_id`/`thread_id` pair - empty strings when
    no route was resolved - so a receiver template never renders a literal
    placeholder. A pair supplied by the caller's blob is kept when the engine
    resolved no route.
    """
    out: dict = {}
    raw = row["context"] if "context" in row.keys() else None
    if raw:
        try:
            blob = json.loads(raw)
            if isinstance(blob, dict):
                out.update({k: v for k, v in blob.items() if v is not None})
        except (json.JSONDecodeError, TypeError):
            pass
    chat_id = row["origin_chat_id"] if "origin_chat_id" in row.keys() else None
    thread_id = row["origin_thread_id"] if "origin_thread_id" in row.keys() else None
    if chat_id:
        out["chat_id"] = chat_id
        out["thread_id"] = thread_id or ""
    else:
        out.setdefault("chat_id", "")
        out.setdefault("thread_id", "")
    for reserved in ("task_id", "status", "state", "delivery_id"):
        out.pop(reserved, None)
    return out


def _wake_payload(row: sqlite3.Row) -> dict:
    """Shape the callback payload from the finished task row."""
    payload = {
        "task_id": row["task_id"],
        "status": row["status"],
        "cwd": row["cwd"],
        "model": row["model"],
        "resolution": row["resolution"],
        "resolved_by": row["resolved_by"],
        "result_text": (row["result_text"] or "")[:4000],
    }
    payload.update(_task_context(row))
    return payload


def _post_signed(url: str, payload: dict, event: str) -> None:
    """POST a signed JSON body. Never raises into the caller."""
    if not url:
        return
    try:
        body = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if NOTIFY_SECRET:
            sig = hmac.new(
                NOTIFY_SECRET.encode("utf-8"), body, hashlib.sha256
            ).hexdigest()
            # GitHub-style headers, which is what the Hermes webhook platform verifies.
            headers["X-Hub-Signature-256"] = f"sha256={sig}"
            headers["X-GitHub-Event"] = event
            # Must be unique per POST or the receiver's idempotency cache drops it.
            headers["X-GitHub-Delivery"] = (
                payload.get("delivery_id") or payload.get("task_id", "unknown")
            )
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f"[{event}] {payload.get('task_id')} -> {resp.status}", flush=True)
    except Exception as exc:  # a notification failure must never affect the task
        print(f"[{event}] failed for {payload.get('task_id')}: {exc}", flush=True)


def _post_wake(payload: dict) -> None:
    """POST the wake payload. Never raises into the caller."""
    _post_signed(NOTIFY_URL, payload, "pi-task")


def wake_async(task_id: str) -> None:
    """
    Fire the wake notification on a daemon thread.

    Deliberately asynchronous: the task is already resolved by the time this is
    called, so a slow or dead webhook endpoint must not block the bridge, hold a
    request thread, or delay any other task. Failures are logged and dropped.
    """
    if not NOTIFY_URL:
        return

    def _run() -> None:
        try:
            conn = db()
            try:
                row = conn.execute(
                    "SELECT * FROM tasks WHERE task_id=?", (task_id,)
                ).fetchone()
            finally:
                conn.close()
            if row is not None:
                _post_wake(_wake_payload(row))
        except Exception as exc:
            print(f"[wake] lookup failed for {task_id}: {exc}", flush=True)

    threading.Thread(target=_run, daemon=True, name=f"wake-{task_id}").start()


# ---------------------------------------------------------------- orchestrator

# Optional orchestrator integration. Point these at the orchestrator's own state
# and the bridge can (a) inherit the model it is currently using and (b) attach
# routing context to notifications. Both are opt-in: leave them unset and the
# bridge is a plain task runner with no knowledge of the caller.
ORCHESTRATOR_DB = (
    Path(os.environ["PI_BRIDGE_ORCHESTRATOR_DB"])
    if os.environ.get("PI_BRIDGE_ORCHESTRATOR_DB")
    else None
)
ORCHESTRATOR_CONFIG = (
    Path(os.environ["PI_BRIDGE_ORCHESTRATOR_CONFIG"])
    if os.environ.get("PI_BRIDGE_ORCHESTRATOR_CONFIG")
    else None
)

# Model ids that are internal routing aliases rather than real models the gateway
# accepts. Pinning Pi to one silently pins it to whatever the gateway default is,
# so these are skipped whenever a concrete model is available.
_ALIAS_PREFIXES = tuple(
    a.strip()
    for a in os.environ.get(
        "PI_BRIDGE_MODEL_ALIASES",
        "",
    ).split(",")
    if a.strip()
)


def _looks_like_alias(model: str) -> bool:
    return any(model == a or model.startswith(a + "-") for a in _ALIAS_PREFIXES)


def active_model_from_state(session_id: str | None = None) -> tuple[str | None, str | None]:
    """
    Read the model currently in use by Hermes from its state database.

    Returns (model, provider). Prefers the newest live session; falls back to the
    newest session overall. Hermes stores the active model per session in
    `sessions.model`, so 'the model Hermes is using right now' is a session-scoped
    question - not a single global value.
    """
    if not ORCHESTRATOR_DB or not ORCHESTRATOR_DB.is_file():
        return (None, None)
    try:
        conn = sqlite3.connect(f"file:{ORCHESTRATOR_DB}?mode=ro", uri=True, timeout=10)
    except sqlite3.Error:
        return (None, None)
    try:
        if session_id:
            row = conn.execute(
                "SELECT model FROM sessions WHERE id=?", (session_id,)
            ).fetchone()
            if row and row[0]:
                return (row[0], None)
        # Live session first (no end_reason), newest activity wins.
        row = conn.execute(
            """SELECT model FROM sessions
               WHERE model IS NOT NULL AND model != ''
               ORDER BY (ended_at IS NULL) DESC, last_activity_at DESC
               LIMIT 1"""
        ).fetchone()
        if row and row[0]:
            return (row[0], None)
    except sqlite3.Error:
        pass
    finally:
        conn.close()
    return (None, None)


def _env_route() -> tuple[str | None, str | None, str | None]:
    """
    (session_id, chat_id, thread_id) declared by the calling environment.

    A submit made from the orchestrator's own shell can then route back to the
    chat that asked, without every caller threading `--session` through. The
    generic PI_BRIDGE_* names win; the Hermes integration's own variables are the
    built-in fallback.
    """
    def pick(*names: str) -> str | None:
        for name in names:
            value = os.environ.get(name)
            if value:
                return value
        return None

    return (
        pick("PI_BRIDGE_SESSION_ID", "HERMES_SESSION_ID"),
        pick("PI_BRIDGE_CHAT_ID", "HERMES_SESSION_CHAT_ID"),
        pick("PI_BRIDGE_THREAD_ID", "HERMES_SESSION_THREAD_ID"),
    )


def active_session_origin(session_id: str | None = None) -> tuple[str | None, str | None, str | None]:
    """
    Returns (session_id, chat_id, thread_id) for the active session in the
    orchestrator's state db, so notifications can be routed back to the chat and
    topic that asked. Opt-in: returns (None, None, None) unless
    PI_BRIDGE_ORCHESTRATOR_DB is set and its `sessions` table has these columns.
    Callers that route their own way should pass `context=` to submit() instead.
    """
    if not ORCHESTRATOR_DB or not ORCHESTRATOR_DB.is_file():
        return (None, None, None)
    try:
        conn = sqlite3.connect(f"file:{ORCHESTRATOR_DB}?mode=ro", uri=True, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            if session_id:
                row = conn.execute(
                    "SELECT id, chat_id, thread_id FROM sessions WHERE id=?", (session_id,)
                ).fetchone()
                if row:
                    return (row["id"], row["chat_id"], row["thread_id"])
            row = conn.execute(
                """SELECT id, chat_id, thread_id FROM sessions
                   WHERE model IS NOT NULL AND model != ''
                     AND chat_id IS NOT NULL AND chat_id NOT LIKE 'webhook:%'
                   ORDER BY (ended_at IS NULL) DESC, last_activity_at DESC
                   LIMIT 1"""
            ).fetchone()
            if row:
                return (row["id"], row["chat_id"], row["thread_id"])
        finally:
            conn.close()
    except Exception:
        pass
    return (None, None, None)


_ERROR_SIGNATURE = re.compile(r"^\s*(Error|ERROR|Exception|Traceback|Fatal)\b")

def _read_log_error(log_path_str: str | None) -> str | None:
    """Return the last lines that look like a real error, or None.

    A non-empty log is NOT an error: Pi prints two benign Warnings on every run,
    so only an actual error signature counts.
    """
    if not log_path_str:
        return None
    p = Path(log_path_str)
    if not p.is_file():
        return None
    try:
        content = p.read_text(encoding="utf-8", errors="replace").strip()
        if not content:
            return None
        hits = [l for l in content.splitlines() if l.strip() and _ERROR_SIGNATURE.search(l)]
        if not hits:
            return None
        return "\n".join(hits[-10:])[:2000]
    except Exception:
        return None


def config_default_model() -> str | None:
    """Fallback: the configured default model in config.yaml."""
    if not ORCHESTRATOR_CONFIG or not ORCHESTRATOR_CONFIG.is_file():
        return None
    try:
        import yaml  # optional dependency
        with ORCHESTRATOR_CONFIG.open(encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh) or {}
        m = (cfg.get("model") or {}).get("default")
        return m or None
    except Exception:
        # Minimal fallback that does not require pyyaml.
        try:
            text = ORCHESTRATOR_CONFIG.read_text(encoding="utf-8")
        except OSError:
            return None
        m = re.search(r"^model:\s*\n\s+default:\s*(\S+)", text, re.M)
        return m.group(1) if m else None


def resolve_model(explicit: str | None = None, session_id: str | None = None,
                  allow_alias: bool = False) -> tuple[str, str]:
    """
    Decide which model Pi should run. Returns (model, source).

    Order:
      1. explicit override (--model / PI_BRIDGE_MODEL)
      2. the model Hermes is currently using (live session)
      3. config.yaml default
      4. PI_BRIDGE_FALLBACK_MODEL, when set

    Internal aliases are skipped when a concrete model is available, because
    pinning Pi to an alias does not guarantee it follows Hermes.
    """
    if explicit:
        return (explicit, "explicit")

    env_override = os.environ.get("PI_BRIDGE_MODEL")
    if env_override:
        return (env_override, "env")

    model, _ = active_model_from_state(session_id)
    if model and (allow_alias or not _looks_like_alias(model)):
        return (model, "session")
    if model:
        # Only an alias was found; prefer a concrete configured default.
        cfg = config_default_model()
        if cfg and not _looks_like_alias(cfg):
            return (cfg, "config")
        return (model, "session-alias")

    cfg = config_default_model()
    if cfg:
        return (cfg, "config")
    return (FALLBACK_MODEL, "fallback")


# Where Pi keeps sessions. Pi groups by working directory.
PI_SESSIONS_DIR = Path(
    os.environ.get("PI_CODING_AGENT_SESSION_DIR", HOME / ".pi" / "agent" / "sessions")
)

DEFAULT_TIMEOUT = int(os.environ.get("PI_BRIDGE_TIMEOUT", "7200"))  # 2h
TEST_TIMEOUT = int(os.environ.get("PI_BRIDGE_TEST_TIMEOUT", "900"))
WATCHDOG_INTERVAL = int(os.environ.get("PI_BRIDGE_WATCHDOG_INTERVAL", "30"))
# An orphan is a Pi process whose task has no running row. Only reap one that has
# gone this long without writing to its session file.
ORPHAN_IDLE = int(os.environ.get("PI_BRIDGE_ORPHAN_IDLE", "900"))

PI_BIN = os.environ.get("PI_BIN", "pi")
# Model to use when nothing else resolves (e.g. a gateway routing alias).
# Empty means: do not pass --model, let Pi use its own configured default.
FALLBACK_MODEL = os.environ.get("PI_BRIDGE_FALLBACK_MODEL", "")

# Terminal statuses. Anything else means "still in flight".
FINAL = {"done", "failed", "timeout", "lost"}

# ---------------------------------------------------------------- state

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id       TEXT PRIMARY KEY,
    prompt        TEXT NOT NULL,
    cwd           TEXT NOT NULL,
    session_id    TEXT NOT NULL,
    model         TEXT,
    provider      TEXT,
    pid           INTEGER,
    status        TEXT NOT NULL,          -- pending|running|done|failed|timeout|lost
    created_at    REAL NOT NULL,
    started_at    REAL,
    deadline      REAL,
    finished_at   REAL,
    exit_code     INTEGER,
    result_text   TEXT,
    usage_json    TEXT,
    resolution    TEXT,                   -- callback|session-file|watchdog|exit
    resolved_by   TEXT,                   -- who first resolved it
    log_path      TEXT,
    model_source  TEXT,                   -- explicit|env|session|config|fallback
    origin_chat_id   TEXT,
    origin_thread_id TEXT,
    context          TEXT,                -- opaque JSON supplied by the caller
    parent_task_id   TEXT,                -- task this one resumed, if any
    artifacts_json   TEXT,                -- git snapshot captured at resolution
    test_cmd         TEXT,                -- verification command run on done
    max_fixes        INTEGER DEFAULT 0,   -- auto-fix attempts allowed after a red gate
    fix_attempt      INTEGER DEFAULT 0    -- how many fixes deep this task is
);
"""

_lock = threading.Lock()


def db() -> sqlite3.Connection:
    BRIDGE_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    # Migrate databases created before newer columns existed.
    cols = {r[1] for r in conn.execute("PRAGMA table_info(tasks)")}
    if "model_source" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN model_source TEXT")
        conn.commit()
    if "origin_chat_id" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN origin_chat_id TEXT")
        conn.commit()
    if "origin_thread_id" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN origin_thread_id TEXT")
        conn.commit()
    if "context" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN context TEXT")
        conn.commit()
    if "parent_task_id" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN parent_task_id TEXT")
        conn.commit()
    if "artifacts_json" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN artifacts_json TEXT")
        conn.commit()
    if "test_cmd" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN test_cmd TEXT")
        conn.commit()
    if "max_fixes" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN max_fixes INTEGER DEFAULT 0")
        conn.commit()
    if "fix_attempt" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN fix_attempt INTEGER DEFAULT 0")
        conn.commit()
    return conn


# ---------------------------------------------------------------- pi helpers


def pi_session_dir_for(cwd: str) -> Path:
    """Pi slugs the working directory into a folder name: /tmp/pi-ws -> --tmp-pi-ws--"""
    slug = "--" + cwd.strip("/").replace("/", "-") + "--"
    return PI_SESSIONS_DIR / slug


def find_session_file(session_id: str, cwd: str) -> Path | None:
    """Locate the JSONL session file for a task. Pi names it <ts>_<session_id>.jsonl."""
    d = pi_session_dir_for(cwd)
    if not d.is_dir():
        # fall back to a global search; slug rules can differ across versions
        for cand in PI_SESSIONS_DIR.glob(f"**/*_{session_id}.jsonl"):
            return cand
        return None
    hits = sorted(d.glob(f"*_{session_id}.jsonl"))
    return hits[-1] if hits else None


def read_session_outcome(session_id: str, cwd: str) -> tuple[str, dict | None] | None:
    """
    Resolve a task's outcome from its Pi session file. Returns (text, usage) or
    None if the session cannot be read *or* does not hold a settled result.

    Only the last assistant message counts: earlier ones are intermediate turns.

    Critically, the last assistant message must carry a TERMINAL stop reason.
    A message with `stopReason: "toolUse"` means Pi was still working - it had
    asked for a tool call and never got to consume the result (typically because
    the process was killed). Treating that as a result is how a killed task gets
    silently reported as done. Such a session is NOT settled; return None so the
    caller can classify it as failed/lost instead.
    """
    path = find_session_file(session_id, cwd)
    if not path or not path.is_file():
        return None

    last_text: str | None = None
    last_usage: dict | None = None
    stop_reason: str | None = None
    err_msg: str | None = None

    try:
        with path.open(encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue  # a torn final line while Pi is still writing
                if entry.get("type") != "message":
                    continue
                msg = entry.get("message") or {}
                if msg.get("role") != "assistant":
                    continue
                content = msg.get("content")
                if isinstance(content, list):
                    text = "".join(
                        p.get("text", "") for p in content if p.get("type") == "text"
                    )
                else:
                    text = content or ""
                last_text = text
                last_usage = msg.get("usage")
                stop_reason = msg.get("stopReason")
                err_msg = msg.get("errorMessage")
    except OSError:
        return None

    if last_text is None and err_msg is None:
        return None

    # Non-terminal stop reasons: the agent was still mid-flight when the
    # process ended. There is no settled result in this file.
    if stop_reason not in ("stop", "error", "aborted"):
        return None

    if stop_reason in ("error", "aborted"):
        # Keep the text/errorMessage so the orchestrator can see why it failed.
        err_detail = last_text or err_msg or f"error (stopReason={stop_reason})"
        return ("__ERROR__" + err_detail, last_usage)
    return (last_text or "", last_usage)


def _session_activity(session_id: str, cwd: str) -> dict:
    """
    Best-effort snapshot of what Pi is doing right now, read from its session
    file. Pi writes almost nothing to stdout while it works, so the session
    jsonl is the only real liveness signal.
    """
    info = {"bytes": 0, "age": None, "iteration": 0, "last_tool": None, "last_text": None}
    path = find_session_file(session_id, cwd)
    if not path or not path.is_file():
        return info
    try:
        st = path.stat()
    except OSError:
        return info
    info["bytes"] = st.st_size
    info["age"] = max(0.0, time.time() - st.st_mtime)
    try:
        with path.open(encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                content = (entry.get("message") or {}).get("content")
                if not isinstance(content, list):
                    continue
                for part in content:
                    if part.get("type") == "toolCall":
                        info["iteration"] += 1
                        info["last_tool"] = part.get("name") or part.get("toolName")
                    elif part.get("type") == "text" and part.get("text"):
                        lines = part["text"].strip().splitlines()
                        if lines:
                            info["last_text"] = lines[0][:140]
    except OSError:
        pass
    return info


def _stream_events(path, off: int) -> tuple[list[tuple[str, str]], int]:
    """New (kind, text) events written to the session file since `off`.

    Only whole lines are consumed: a half-written jsonl line stays unread and
    the returned offset stops before it, so the next tick sees it complete.
    """
    try:
        with path.open("rb") as fh:
            fh.seek(off)
            chunk = fh.read()
    except OSError:
        return [], off
    end = chunk.rfind(b"\n")
    if end == -1:
        return [], off
    events: list[tuple[str, str]] = []
    for raw in chunk[: end + 1].decode("utf-8", "replace").splitlines():
        raw = raw.strip()
        if not raw:
            continue
        try:
            entry = json.loads(raw)
        except json.JSONDecodeError:
            continue
        content = (entry.get("message") or {}).get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if part.get("type") == "toolCall":
                name = part.get("name") or part.get("toolName") or "tool"
                args = part.get("arguments") or part.get("args") or {}
                if isinstance(args, dict):
                    hint = args.get("command") or args.get("path") or args.get("pattern") or ""
                else:
                    hint = ""
                hint = str(hint).splitlines()[0][:160] if str(hint).strip() else ""
                events.append(("tool", f"{name} {hint}".strip()))
            elif part.get("type") == "text" and part.get("text", "").strip():
                text = part["text"].strip()
                events.append(("text", text[:280] + ("…" if len(text) > 280 else "")))
    return events, off + end + 1

def task_status(row: sqlite3.Row, act: dict | None = None) -> dict:
    """Classify a running task for the heartbeat: what state, how long, how far."""
    now = time.time()
    elapsed = int(now - (row["started_at"] or row["created_at"]))
    # pid is None for --systemd tasks; there the process is alive, we just cannot see it.
    alive = True if row["pid"] is None else pid_alive(row["pid"])
    act = act if act is not None else _session_activity(row["session_id"], row["cwd"])
    log_err = _read_log_error(row["log_path"])

    stale = act["age"] is not None and act["age"] > HEARTBEAT_INTERVAL * 3
    if not alive:
        state, detail = "dne", "process is gone"
    elif stale or act["age"] is None:
        # No fresh session activity: only then can the log decide error vs idle.
        if log_err:
            state, detail = "error", log_err[:200]
        elif stale:
            state, detail = "idle", f"no session activity for {int(act['age'])}s"
        else:
            state, detail = "working", "starting"
    else:
        state = "working"
        detail = act["last_text"] or (
            f"last tool: {act['last_tool']}" if act["last_tool"] else "starting"
        )

    out = {
        "task_id": row["task_id"],
        "state": state,
        "elapsed": f"{elapsed // 60}m{elapsed % 60:02d}s",
        "iteration": act["iteration"],
        "last_tool": act["last_tool"] or "-",
        "detail": detail,
        "cwd": row["cwd"],
        "model": row["model"],
    }
    out.update(_task_context(row))
    return out


def pid_alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, owned by someone else
    return True


# ---------------------------------------------------------------- resolution


def capture_artifacts(cwd: str) -> dict | None:
    """Snapshot the tree at resolution time.

    Cheap (three git calls, ~50 ms) and it is the difference between the
    orchestrator reading evidence and re-deriving it by hand on every task.
    Returns None when the cwd is not a git work tree.
    """
    def run(*args: str) -> str:
        try:
            return subprocess.run(
                ["git", "-C", cwd, *args],
                capture_output=True, text=True, timeout=10,
            ).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            return ""

    head = run("log", "-1", "--format=%h %s")
    if not head:
        return None
    return {
        "head": head,
        "branch": run("rev-parse", "--abbrev-ref", "HEAD"),
        "status": run("status", "--porcelain"),
        "diffstat": run("diff", "--stat"),
    }

def run_tests(cwd: str, cmd: str) -> dict:
    """Run the caller's verification command and keep the tail.

    shell=True is deliberate: test commands are written by the trusted
    orchestrator (`pytest -q && ruff check .`), not by the model.
    """
    try:
        p = subprocess.run(
            cmd, shell=True, cwd=cwd, capture_output=True, text=True,
            timeout=TEST_TIMEOUT,
        )
        out = (p.stdout + p.stderr).strip()
        return {"cmd": cmd, "rc": p.returncode, "tail": out[-2000:]}
    except subprocess.TimeoutExpired:
        return {"cmd": cmd, "rc": 124, "tail": f"timed out after {TEST_TIMEOUT}s"}
    except OSError as exc:
        return {"cmd": cmd, "rc": 127, "tail": str(exc)}

FIX_PROMPT = """Your change failed its verification gate.

Command: {cmd}
Exit code: {rc}

Output tail:
---
{tail}
---

Fix the cause, not the test. Do not weaken or delete assertions. Keep the diff
as small as the defect requires. Re-run the command above before you stop."""

def _spawn_fix(row, test_result: dict) -> str | None:
    """Chain one more attempt onto a task whose gate went red."""
    attempt = (row["fix_attempt"] or 0) + 1
    if attempt > (row["max_fixes"] or 0):
        return None
    prompt = FIX_PROMPT.format(
        cmd=test_result["cmd"], rc=test_result["rc"], tail=test_result["tail"]
    )
    return submit(
        prompt,
        row["cwd"],
        model=row["model"],
        provider=row["provider"],
        timeout=DEFAULT_TIMEOUT,
        test_cmd=row["test_cmd"],
        max_fixes=row["max_fixes"],
        fix_attempt=attempt,
        resume_of=row["task_id"],
        template="fix",
        no_commit=True,
    )

def resolve(task_id: str, status: str, *, text=None, usage=None,
            exit_code=None, resolution=None, resolved_by=None) -> bool:
    """
    Write a terminal outcome for a task, exactly once. Returns True if this call
    performed the transition, False if the task was already final.
    """
    artifacts = None
    conn = db()
    try:
        cwd_row = conn.execute(
            "SELECT * FROM tasks WHERE task_id=?", (task_id,)
        ).fetchone()
    finally:
        conn.close()
    if cwd_row is not None:
        artifacts = capture_artifacts(cwd_row["cwd"])

    # A task is only done if its own verification passes. The caller supplies
    # the command; the bridge runs it once, before the terminal write, so every
    # resolution path (callback, session file, watchdog) gets the same gate.
    if status == "done" and cwd_row is not None and cwd_row["test_cmd"]:
        result = run_tests(cwd_row["cwd"], cwd_row["test_cmd"])
        artifacts = {**(artifacts or {}), "tests": result}
        if result["rc"] != 0:
            status = "failed"
            text = f"tests failed (rc={result['rc']})\n{result['tail']}"
            resolution = resolution or "test"
            child = _spawn_fix(cwd_row, result)
            if child:
                text += f"\nauto-fix spawned: {child}"
                artifacts = {**artifacts, "fix_task": child}

    with _lock:
        conn = db()
        try:
            row = conn.execute(
                "SELECT status FROM tasks WHERE task_id=?", (task_id,)
            ).fetchone()
            if row is None:
                return False
            if row["status"] in FINAL:
                return False
            conn.execute(
                """UPDATE tasks SET status=?, finished_at=?, result_text=?,
                       usage_json=?, exit_code=?, resolution=?, resolved_by=?,
                       artifacts_json=?
                   WHERE task_id=?""",
                (
                    status,
                    time.time(),
                    text,
                    json.dumps(usage) if usage else None,
                    exit_code,
                    resolution,
                    resolved_by,
                    json.dumps(artifacts) if artifacts else None,
                    task_id,
                ),
            )
            conn.commit()
        finally:
            conn.close()
    # Wake the orchestrator only on the transition that actually finalised the
    # task. resolve() is idempotent, so a task resolved twice must not notify
    # twice. Fired after the commit and outside the lock, so a slow webhook
    # cannot stall the bridge or another task's resolution.
    wake_async(task_id)
    return True


def handle_callback(payload: dict) -> bool:
    """
    Handle a push callback from the Pi extension.

    The extension reports the run's real status. A `failed` callback carries the
    stop reason so the orchestrator can distinguish a provider error from an
    abort - and, crucially, so a failed run is never recorded as `done`.
    """
    task_id = payload.get("task_id")
    if not task_id:
        return False

    status = payload.get("status", "done")
    if status not in ("done", "failed"):
        status = "failed"

    text = payload.get("text") or ""
    if status == "failed":
        reason = payload.get("error") or payload.get("stop_reason") or "unknown"
        if not text:
            text = f"run failed (stop_reason={payload.get('stop_reason')}): {reason}"

    return resolve(
        task_id,
        status,
        text=text,
        usage=payload.get("usage"),
        exit_code=payload.get("exit"),
        resolution="callback",
        resolved_by="callback",
    )


# ---------------------------------------------------------------- submission


_PASSTHROUGH_PROVIDERS = tuple(
    p.strip()
    for p in os.environ.get("PI_BRIDGE_PASSTHROUGH_PROVIDERS", "").split(",")
    if p.strip()
)


PI_MODELS_JSON = HOME / ".pi" / "agent" / "models.json"

def ensure_provider_placeholder() -> None:
    """
    Guarantee Pi has at least one declared model for the gateway provider.

    Pi refuses a provider that declares zero models ("Unknown provider"), but it
    happily passes an *undeclared* model id through to the server
    ("Using custom model id"). Declaring one placeholder therefore unlocks
    arbitrary model ids - which is what lets an orchestrator inject the model it
    is currently using without hardcoding a model list that the gateway catalog
    would invalidate anyway.
    """
    models_json = PI_MODELS_JSON
    if not models_json.is_file():
        return
    try:
        cfg = json.loads(models_json.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return

    changed = False
    for name, prov in (cfg.get("providers") or {}).items():
        if not isinstance(prov, dict):
            continue
        models = prov.get("models")
        if not models:  # empty or missing -> Pi rejects the whole provider
            base = (prov.get("baseUrl") or "")
            if name in _PASSTHROUGH_PROVIDERS or any(
                p and p in base for p in _PASSTHROUGH_PROVIDERS
            ):
                prov["models"] = [{
                    "id": "passthrough", "name": "Passthrough",
                    "reasoning": True, "contextWindow": 131072, "maxTokens": 32768,
                }]
                changed = True
    if changed:
        try:
            models_json.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
        except OSError:
            pass


def declared_models() -> dict[str, set[str]]:
    """
    Map provider name -> declared model ids, read from Pi's models.json.
    Empty dict means the catalog is unknown (missing or unreadable file), in
    which case callers must not assume a model is unusable.
    """
    try:
        cfg = json.loads(PI_MODELS_JSON.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    out: dict[str, set[str]] = {}
    for name, prov in (cfg.get("providers") or {}).items():
        if isinstance(prov, dict):
            out[name] = {
                m.get("id") for m in (prov.get("models") or [])
                if isinstance(m, dict) and m.get("id")
            }
    return out

def passthrough_provider() -> str | None:
    """First configured passthrough provider that Pi will accept, if any."""
    declared = declared_models()
    return next((p for p in _PASSTHROUGH_PROVIDERS if declared.get(p)), None)

def model_is_resolvable(model: str, provider: str | None) -> bool:
    """
    Whether Pi can resolve `model` given an optional explicit provider.
    With an explicit provider, Pi passes any undeclared id through to the server
    as long as that provider declares at least one model. Without one, the id
    must appear in the declared catalog.
    """
    declared = declared_models()
    if not declared:
        return True
    if provider:
        return bool(declared.get(provider))
    return any(model in ids for ids in declared.values())

def fit_model_to_pi(model: str, provider: str | None) -> tuple[str, str | None, str | None]:
    """
    Return (model, provider, note) adjusted so Pi can actually start.
    `note` is None when nothing changed, "provider" when the passthrough provider
    was attached, "model" when the model fell back to FALLBACK_MODEL,
    "provider-dropped" when an unresolved model forced the provider off (Pi
    rejects `--provider` without `--model`).
    """
    if not model:
        return model, None, "provider-dropped" if provider else None
    if model_is_resolvable(model, provider):
        return model, provider, None
    if not provider:
        passthrough = passthrough_provider()
        if passthrough:
            return model, passthrough, "provider"
    return FALLBACK_MODEL, provider, "model"

HOOKS_DIR = Path(__file__).resolve().parent / "git-hooks"

def no_commit_env() -> dict[str, str]:
    """
    Environment that makes git refuse to commit or push, for one Pi process.

    Git reads GIT_CONFIG_COUNT/GIT_CONFIG_KEY_n/GIT_CONFIG_VALUE_n as extra
    config, so core.hooksPath points at the bridge's blocking hooks without
    touching the target repo or replacing the user's global config.
    """
    base = int(os.environ.get("GIT_CONFIG_COUNT") or "0")
    return {
        "PI_BRIDGE_NO_COMMIT": "1",
        "GIT_CONFIG_COUNT": str(base + 1),
        f"GIT_CONFIG_KEY_{base}": "core.hooksPath",
        f"GIT_CONFIG_VALUE_{base}": str(HOOKS_DIR),
    }


def build_env(task_id: str) -> dict:
    env = os.environ.copy()
    env["PI_BRIDGE_CALLBACK_URL"] = f"http://{CALLBACK_HOST}:{CALLBACK_PORT}{CALLBACK_PATH}"
    env["PI_BRIDGE_TASK_ID"] = task_id
    return env


# A task brief that states only the goal makes Pi explore instead of edit. These
# headers carry the rules that actually stopped that: narrow scope, no invented
# abstractions, and a verification command the caller supplies in the brief.
TEMPLATES = {
    "fix": (
        "Work narrowly. Change only what the defect requires. State the root cause in "
        "one sentence before editing. Do not refactor unrelated code and do not add "
        "abstractions. Verify with the exact command in the brief and paste its output."
    ),
    "feature": (
        "Implement the smallest change that satisfies the request. No scaffolding for "
        "later, no config for a value that never changes, no interface with a single "
        "implementation. Match the surrounding style exactly. Verify with the exact "
        "command in the brief and paste its output."
    ),
    "review": (
        "Review, do not rewrite. Report each defect as file:line, what breaks, and why "
        "it matters, ordered by severity. If you find nothing, say so plainly. Do not "
        "modify files unless the brief explicitly asks you to fix something."
    ),
    "port": (
        "Port behaviour, not code. Name the source and target file for each change. "
        "Preserve observable behaviour exactly and call out anything that cannot be "
        "ported. Verify with the exact command in the brief and paste its output."
    ),
}

PLAN_HINT = (
    "Plan before editing: list the files you will touch and the exact change to each, "
    "then execute that list without re-planning."
)

def submit(
    prompt: str,
    cwd: str,
    *,
    model: str | None = None,
    provider: str | None = None,
    timeout: int = DEFAULT_TIMEOUT,
    extra_args: list[str] | None = None,
    use_systemd: bool = False,
    session: str | None = None,
    allow_alias: bool = False,
    context: dict | None = None,
    no_commit: bool = False,
    resume_of: str | None = None,
    template: str | None = None,
    append_system_prompt: str | None = None,
    plan: bool = False,
    test_cmd: str | None = None,
    max_fixes: int = 0,
    fix_attempt: int = 0,
) -> str:
    """
    Spawn a Pi task and return immediately. Nothing here blocks on Pi.

    `model=None` is the important case: Pi inherits whatever model Hermes is
    using at submit time (see resolve_model). Because the model is resolved at
    spawn, changing the Hermes model affects the next Pi task, not one already
    running - that is the honest semantics of a per-task model pin.
    """
    task_id = uuid.uuid4().hex[:16]
    parent = None
    if resume_of:
        conn = db()
        try:
            parent = conn.execute(
                "SELECT * FROM tasks WHERE task_id=?", (resume_of,)
            ).fetchone()
        finally:
            conn.close()
        if parent is None:
            raise ValueError(f"unknown task: {resume_of}")
        # Pi resolves the project from the session itself and refuses to run
        # when the cwd disagrees, so a resume always runs where the parent ran.
        cwd = parent["cwd"]
        session_id = parent["session_id"]
    else:
        session_id = f"pi-{task_id}"
    cwd = str(Path(cwd).resolve())
    env_session, env_chat, env_thread = _env_route()
    session = session or env_session
    TASKS_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    chosen_model, model_source = resolve_model(
        model, session_id=session, allow_alias=allow_alias
    )
    ensure_provider_placeholder()
    chosen_model, provider, model_note = fit_model_to_pi(chosen_model, provider)
    if model_note:
        model_source = model_note

    if template:
        prefix = TEMPLATES.get(template)
        if prefix is None:
            raise ValueError(
                f"unknown template: {template} (choose from {', '.join(TEMPLATES)})"
            )
        prompt = f"{prefix}\n\n{prompt}"
    sys_prompt = " ".join(
        part for part in (PLAN_HINT if plan else None, append_system_prompt) if part
    )

    prompt_file = TASKS_DIR / f"{task_id}.txt"
    prompt_file.write_text(prompt, encoding="utf-8")
    log_path = LOGS_DIR / f"{task_id}.log"

    block_env = no_commit_env() if no_commit else {}
    if parent is not None:
        # --session resumes an existing session; --session-id is mutually
        # exclusive with it and would start a new one instead.
        args = [PI_BIN, "-p", prompt, "--session", session_id, "--approve"]
    else:
        args = [PI_BIN, "-p", prompt, "--session-id", session_id, "--approve"]
    if chosen_model:
        args += ["--model", chosen_model]
    if provider:
        args += ["--provider", provider]
    if sys_prompt:
        args += ["--append-system-prompt", sys_prompt]
    if extra_args:
        args += extra_args

    if use_systemd:
        # Survives a Hermes restart: Pi becomes its own user unit.
        unit = f"pi-task-{task_id}"
        cmd = [
            "systemd-run", "--user", f"--unit={unit}",
            "--property=Type=oneshot",
            f"--working-directory={cwd}",
            f"--setenv=PI_BRIDGE_CALLBACK_URL={os.environ.get('PI_BRIDGE_CALLBACK_URL') or f'http://{CALLBACK_HOST}:{CALLBACK_PORT}{CALLBACK_PATH}'}",
            f"--setenv=PI_BRIDGE_TASK_ID={task_id}",
            # A transient unit inherits the *user manager's* environment, not the
            # bridge's - and the manager's PATH has no ~/.local/bin. Pi lives there
            # with a `#!/usr/bin/env node` shebang, so it died at startup with
            # `env: 'node': No such file or directory` (exit 127) while the log file
            # stayed empty and the watchdog only noticed at the deadline, hours later.
            *([f"--setenv=PATH={os.environ['PATH']}", f"--setenv=PI_BIN={PI_BIN}"] if os.environ.get("PATH") else []),
            *[f"--setenv={k}={v}" for k, v in block_env.items()],
            *args,
        ]
        with log_path.open("wb") as log:
            subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, cwd=cwd)
        pid = None
        proc = None
    else:
        with log_path.open("wb") as log:
            proc = subprocess.Popen(
                args,
                stdout=log,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                cwd=cwd,
                env={**build_env(task_id), **block_env},
                start_new_session=True,  # detach: survives the caller exiting
            )
        pid = proc.pid

    now = time.time()
    _, origin_chat, origin_thread = active_session_origin(session)
    if not session:
        # active_session_origin fell back to the newest session, which belongs to
        # some other conversation - never deliver this task's notifications there.
        origin_chat, origin_thread = None, None
    if not origin_chat:
        origin_chat, origin_thread = env_chat, env_thread

    conn = db()
    try:
        conn.execute(
            """INSERT INTO tasks (task_id, prompt, cwd, session_id, model, provider,
                   pid, status, created_at, started_at, deadline, log_path, model_source,
                   origin_chat_id, origin_thread_id, context, parent_task_id, test_cmd,
                   max_fixes, fix_attempt)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                task_id, prompt, cwd, session_id, chosen_model, provider, pid,
                "running", now, now, now + timeout, str(log_path), model_source,
                origin_chat, origin_thread,
                json.dumps(context) if context else None,
                resume_of,
                test_cmd,
                max_fixes,
                fix_attempt,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    if proc is not None:
        threading.Thread(
            target=_watch_task_proc,
            args=(proc, task_id, str(log_path)),
            daemon=True,
            name=f"watch-{task_id}",
        ).start()

    return task_id


# ---------------------------------------------------------------- watchdog


REVIEW_PROMPT = """Review the change made by task {parent} in {cwd}.

Original request:
---
{request}
---

Inspect the actual change with `git log -1 --stat` and `git diff HEAD~1` (or
`git status` plus `git diff` if the work is still uncommitted).

Report each defect as file:line, what breaks, and why it matters, ordered by
severity. If you find nothing, say so plainly. Do not modify files."""

def review_task(row: sqlite3.Row, *, model=None, timeout=DEFAULT_TIMEOUT) -> str:
    """Spawn a fresh reviewer for a finished task.

    Deliberately a new session, not `resume_of`: a reviewer that shares the
    implementer's context defends its own reasoning instead of attacking it.
    """
    prompt = REVIEW_PROMPT.format(
        parent=row["task_id"], cwd=row["cwd"], request=row["prompt"]
    )
    return submit(
        prompt,
        row["cwd"],
        model=model,
        timeout=timeout,
        template="review",
        no_commit=True,
    )

def _watch_task_proc(proc: subprocess.Popen, task_id: str, log_path: str) -> None:
    """Detect process termination immediately without waiting for watchdog interval."""
    try:
        exit_code = proc.wait()
    except Exception:
        return
    # Allow 2 seconds for in-flight HTTP callback from hermes-callback.ts
    time.sleep(2)
    conn = db()
    try:
        row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
    finally:
        conn.close()
    if row and row["status"] not in FINAL:
        if not _finalize_from_session(row, "proc-exit"):
            err_text = _read_log_error(log_path) or f"process exited with code {exit_code}"
            resolve(
                task_id, "failed", text=err_text, exit_code=exit_code,
                resolution="proc-watcher", resolved_by="proc-exit",
            )


def _finalize_from_session(row: sqlite3.Row, reason: str) -> bool:
    outcome = read_session_outcome(row["session_id"], row["cwd"])
    if outcome is None:
        log_err = _read_log_error(row["log_path"])
        if log_err:
            return resolve(
                row["task_id"], "failed", text=log_err,
                resolution="log-file", resolved_by=reason,
            )
        return False
    text, usage = outcome
    if text.startswith("__ERROR__"):
        err = text[len("__ERROR__"):].strip()
        if not err and row["log_path"]:
            err = _read_log_error(row["log_path"]) or "run ended with error"
        return resolve(
            row["task_id"], "failed", text=err, usage=usage,
            resolution="session-file", resolved_by=reason,
        )
    return resolve(
        row["task_id"], "done", text=text, usage=usage,
        resolution="session-file", resolved_by=reason,
    )


def watchdog_tick() -> list[str]:
    """
    One pass over in-flight tasks. Closes both push-model gaps. Returns the list
    of task_ids that changed state this tick.
    """
    changed: list[str] = []
    now = time.time()
    conn = db()
    try:
        rows = conn.execute(
            "SELECT * FROM tasks WHERE status='running' ORDER BY created_at"
        ).fetchall()
    finally:
        conn.close()

    for row in rows:
        tid = row["task_id"]

        # Gap 1a: deadline passed -> kill and mark timeout.
        if row["deadline"] and now > row["deadline"]:
            if row["pid"] and pid_alive(row["pid"]):
                try:
                    os.killpg(os.getpgid(row["pid"]), signal.SIGTERM)
                except OSError:
                    try:
                        os.kill(row["pid"], signal.SIGTERM)
                    except OSError:
                        pass
            # Give Pi a moment, then take whatever the session file has.
            time.sleep(1)
            if not _finalize_from_session(row, "watchdog-timeout"):
                resolve(
                    tid, "timeout", text="deadline exceeded; no session result",
                    resolution="watchdog", resolved_by="watchdog-timeout",
                )
            changed.append(tid)
            continue

        # Gap 1b: process is gone but no callback arrived -> resolve from session.
        if row["pid"] and not pid_alive(row["pid"]):
            if _finalize_from_session(row, "watchdog-exit"):
                changed.append(tid)
            else:
                # Process died before reaching a settled result. The session file
                # exists but its last assistant message is not terminal (e.g.
                # stopReason "toolUse": killed mid-tool-call). That is a failure,
                # and the orchestrator should be free to retry it.
                err_text = _read_log_error(row["log_path"]) or "process exited without reaching a settled result"
                resolve(
                    tid, "failed",
                    text=err_text,
                    resolution="watchdog", resolved_by="watchdog-exit",
                )
                changed.append(tid)

    return changed


def reconcile() -> list[str]:
    """
    Startup pass. Re-resolves every task still marked 'running' - the cases
    whose callbacks were lost while this process was down.
    """
    changed: list[str] = []
    conn = db()
    try:
        rows = conn.execute("SELECT * FROM tasks WHERE status='running'").fetchall()
    finally:
        conn.close()
    for row in rows:
        tid = row["task_id"]
        if row["pid"] and pid_alive(row["pid"]):
            continue  # genuinely still running
        if _finalize_from_session(row, "reconcile"):
            changed.append(tid)
        else:
            # Unresolved after a restart: either the process is gone without a
            # settled result, or it outlived our pid bookkeeping. Fail it so the
            # orchestrator can retry rather than wait forever.
            resolve(
                tid, "failed",
                text="unresolved after restart; no settled session result",
                resolution="reconcile", resolved_by="reconcile",
            )
            changed.append(tid)
    return changed

# ---------------------------------------------------------------- reaper

def _proc_env(pid: int) -> dict[str, str]:
    """Read a process environment. Empty dict when /proc/<pid>/environ is unreadable."""
    try:
        raw = Path(f"/proc/{pid}/environ").read_bytes()
    except OSError:
        return {}
    out: dict[str, str] = {}
    for chunk in raw.split(b"\0"):
        if not chunk:
            continue
        key, _, val = chunk.partition(b"=")
        out[key.decode("utf-8", "replace")] = val.decode("utf-8", "replace")
    return out

def _proc_argv0(pid: int) -> str:
    """First element of a process argv. Empty string when unreadable."""
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return ""
    return raw.split(b"\0")[0].decode("utf-8", "replace")

def _proc_cwd(pid: int) -> str | None:
    try:
        return os.readlink(f"/proc/{pid}/cwd")
    except OSError:
        return None

def iter_pi_procs() -> list[tuple[int, str, str]]:
    """(pid, task_id, cwd) for every live bridge-spawned Pi process."""
    found: list[tuple[int, str, str]] = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        pid = int(entry)
        if Path(_proc_argv0(pid)).name != "pi":
            continue
        task_id = _proc_env(pid).get("PI_BRIDGE_TASK_ID")
        cwd = _proc_cwd(pid)
        if task_id and cwd:
            found.append((pid, task_id, cwd))
    return found

def reap_orphans() -> list[int]:
    """SIGTERM Pi processes whose task has no running row and that have gone idle.

    A process is only reaped when all three gates hold: it is a bridge-spawned
    `pi` (PI_BRIDGE_TASK_ID present), its task_id has no `running` row, and its
    session file exists but has not been written for ORPHAN_IDLE seconds. The
    session-file gate is what makes this safe against the spawn/insert race:
    submit() Popen()s Pi before INSERTing the row, so a just-started process has
    no row yet but also has no session file yet, and is skipped.
    """
    conn = db()
    try:
        running = {
            row["task_id"]
            for row in conn.execute("SELECT task_id FROM tasks WHERE status='running'")
        }
    finally:
        conn.close()

    now = time.time()
    reaped: list[int] = []
    for pid, task_id, cwd in iter_pi_procs():
        if task_id in running:
            continue
        session = find_session_file(f"pi-{task_id}", cwd)
        if session is None:
            continue
        try:
            idle = now - session.stat().st_mtime
        except OSError:
            continue
        if idle < ORPHAN_IDLE:
            continue
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            continue
        print(f"[reaper] orphan {task_id} pid={pid} idle={idle:.0f}s", flush=True)
        reaped.append(pid)
    return reaped

def watchdog_loop(stop: threading.Event) -> None:
    while not stop.wait(WATCHDOG_INTERVAL):
        try:
            for tid in watchdog_tick():
                print(f"[watchdog] resolved {tid}", flush=True)
            reap_orphans()
        except Exception as exc:  # never let the watchdog die
            print(f"[watchdog] error: {exc}", flush=True)


_hb_seen: dict[str, tuple] = {}
_hb_sent: dict[str, float] = {}
_stream_off: dict[str, int] = {}

def _hb_should_send(task_id: str, sig: tuple, now: float) -> bool:
    """Send when the snapshot changed, or when the task has been quiet too long."""
    if _hb_seen.get(task_id) != sig:
        return True
    if HEARTBEAT_IDLE <= 0:
        return False
    return now - _hb_sent.get(task_id, 0.0) >= HEARTBEAT_IDLE

def _stream_tick(row: sqlite3.Row, now: float) -> None:
    tid = row["task_id"]
    path = find_session_file(row["session_id"], row["cwd"])
    if not path or not path.is_file():
        return
    if tid not in _stream_off:
        _stream_off[tid] = path.stat().st_size
        return
    events, off = _stream_events(path, _stream_off[tid])
    _stream_off[tid] = off
    if not events:
        sig = (path.stat().st_size,)
        if _hb_should_send(tid, sig, now):
            payload = task_status(row)
            payload["reason"] = "idle"
            payload["delivery_id"] = f"{tid}-hb-{int(now)}"
            _post_signed(HEARTBEAT_URL, payload, "pi-status")
            _hb_seen[tid] = sig
            _hb_sent[tid] = now
        return
    for i, (kind, text) in enumerate(events[:STREAM_MAX]):
        payload = task_status(row)
        payload["state"] = kind
        payload["detail"] = text
        payload["reason"] = "stream"
        payload["delivery_id"] = f"{tid}-st-{int(now)}-{i}"
        _post_signed(HEARTBEAT_URL, payload, "pi-status")
    if len(events) > STREAM_MAX:
        payload = task_status(row)
        payload["state"] = "busy"
        payload["detail"] = f"+{len(events) - STREAM_MAX} more"
        payload["reason"] = "stream"
        payload["delivery_id"] = f"{tid}-st-{int(now)}-x"
        _post_signed(HEARTBEAT_URL, payload, "pi-status")
    _hb_seen[tid] = (path.stat().st_size,)
    _hb_sent[tid] = now

def heartbeat_loop(stop: threading.Event) -> None:
    """Push a status line only when Pi produced something new.

    Pi writes almost nothing to stdout, so the session jsonl is the liveness
    signal: its size plus the last tool/text is the change signature. A fixed
    timer instead repeated the same line every minute for hours.
    """
    while not stop.wait(HEARTBEAT_INTERVAL):
        try:
            conn = db()
            try:
                rows = conn.execute(
                    "SELECT * FROM tasks WHERE status='running' ORDER BY created_at"
                ).fetchall()
            finally:
                conn.close()
            now = time.time()
            alive = set()
            for row in rows:
                tid = row["task_id"]
                alive.add(tid)
                try:
                    if STREAM:
                        _stream_tick(row, now)
                        continue
                    act = _session_activity(row["session_id"], row["cwd"])
                    sig = (act["bytes"], act["iteration"], act["last_tool"], act["last_text"])
                    if not _hb_should_send(tid, sig, now):
                        continue
                    payload = task_status(row, act)
                    payload["reason"] = "activity" if _hb_seen.get(tid) != sig else "idle"
                    payload["delivery_id"] = f"{tid}-hb-{int(now)}"
                    _post_signed(HEARTBEAT_URL, payload, "pi-status")
                    _hb_seen[tid] = sig
                    _hb_sent[tid] = now
                except Exception as exc:
                    # One unparseable session must not silence every other task.
                    print(f"[heartbeat] {tid}: {exc}", flush=True)
            for tid in list(_hb_seen):
                if tid not in alive:
                    _hb_seen.pop(tid, None)
                    _hb_sent.pop(tid, None)
                    _stream_off.pop(tid, None)
        except Exception as exc:  # never let the heartbeat die
            print(f"[heartbeat] error: {exc}", flush=True)


# ---------------------------------------------------------------- HTTP


class Handler(BaseHTTPRequestHandler):
    def _json(self, code: int, obj: dict) -> None:
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        try:
            self._dispatch_post()
        except Exception as exc:
            try:
                self._json(500, {"ok": False, "error": str(exc)})
            except Exception:
                pass

    def _dispatch_post(self):
        n = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(n) if n else b"{}"
        try:
            payload = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            return self._json(400, {"ok": False, "error": "bad json"})
        if not isinstance(payload, dict):
            return self._json(400, {"ok": False, "error": "body must be a json object"})

        if self.path.rstrip("/") == CALLBACK_PATH:
            ok = handle_callback(payload)
            print(f"[callback] task={payload.get('task_id')} accepted={ok}", flush=True)
            return self._json(200, {"ok": True, "accepted": ok})

        if self.path.rstrip("/") == "/submit":
            prompt = payload.get("prompt")
            if not isinstance(prompt, str) or not prompt.strip():
                return self._json(400, {"ok": False, "error": "prompt is required"})
            try:
                timeout = int(payload.get("timeout", DEFAULT_TIMEOUT))
            except (TypeError, ValueError):
                return self._json(400, {"ok": False, "error": "timeout must be an integer"})
            try:
                max_fixes = int(payload.get("max_fixes", 0))
            except (TypeError, ValueError):
                return self._json(400, {"ok": False, "error": "max_fixes must be an integer"})
            if max_fixes < 0:
                return self._json(400, {"ok": False, "error": "max_fixes must be >= 0"})
            try:
                tid = submit(
                    prompt,
                    payload.get("cwd", os.getcwd()),
                    model=payload.get("model"),
                    provider=payload.get("provider"),
                    timeout=timeout,
                    session=payload.get("session_id"),
                    allow_alias=bool(payload.get("allow_alias", False)),
                    context=payload.get("context"),
                    no_commit=bool(payload.get("no_commit", False)),
                    resume_of=payload.get("resume_of"),
                    template=payload.get("template"),
                    append_system_prompt=payload.get("append_system_prompt"),
                    plan=bool(payload.get("plan", False)),
                    test_cmd=payload.get("test_cmd"),
                    max_fixes=max_fixes,
                )
            except ValueError as exc:
                return self._json(400, {"ok": False, "error": str(exc)})
            return self._json(200, {"ok": True, "task_id": tid})

        if self.path.rstrip("/") == "/model":
            model, source = resolve_model(
                payload.get("explicit"),
                session_id=payload.get("session_id"),
                allow_alias=bool(payload.get("allow_alias", False)),
            )
            return self._json(200, {"ok": True, "model": model, "source": source})

        return self._json(404, {"ok": False, "error": "not found"})

    def do_GET(self):
        if self.path.rstrip("/") == "/list":
            conn = db()
            try:
                rows = conn.execute(
                    "SELECT task_id,status,resolution,created_at,finished_at FROM tasks "
                    "ORDER BY created_at DESC LIMIT 100"
                ).fetchall()
            finally:
                conn.close()
            return self._json(200, {"ok": True, "tasks": [dict(r) for r in rows]})

        if self.path.startswith("/status/"):
            tid = self.path.rsplit("/", 1)[-1]
            conn = db()
            try:
                row = conn.execute(
                    "SELECT * FROM tasks WHERE task_id=?", (tid,)
                ).fetchone()
            finally:
                conn.close()
            if not row:
                return self._json(404, {"ok": False, "error": "unknown task"})
            d = dict(row)
            d.pop("prompt", None)  # keep prompts out of status responses
            return self._json(200, {"ok": True, "task": d})

        return self._json(404, {"ok": False, "error": "not found"})

    def log_message(self, *a):  # quiet
        pass


def serve() -> None:
    BRIDGE_DIR.mkdir(parents=True, exist_ok=True)
    changed = reconcile()
    if changed:
        print(f"[startup] reconciled {len(changed)} task(s): {changed}", flush=True)

    stop = threading.Event()
    threading.Thread(target=watchdog_loop, args=(stop,), daemon=True).start()
    if HEARTBEAT_URL:
        threading.Thread(target=heartbeat_loop, args=(stop,), daemon=True).start()
        print(f"[serve] heartbeat every {HEARTBEAT_INTERVAL}s -> {HEARTBEAT_URL}", flush=True)

    httpd = ThreadingHTTPServer((CALLBACK_HOST, CALLBACK_PORT), Handler)
    print(f"[serve] callback  http://{CALLBACK_HOST}:{CALLBACK_PORT}{CALLBACK_PATH}", flush=True)
    print(f"[serve] submit    http://{CALLBACK_HOST}:{CALLBACK_PORT}/submit", flush=True)
    print(f"[serve] state     {DB_PATH}", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        httpd.server_close()


# ---------------------------------------------------------------- CLI


def main() -> int:
    ap = argparse.ArgumentParser(prog="pi_bridge")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("serve")

    s = sub.add_parser("submit")
    s.add_argument("prompt")
    s.add_argument("--cwd", default=os.getcwd())
    s.add_argument("--model", help="explicit model; omit to inherit the orchestrator's current model")
    s.add_argument("--provider")
    s.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    s.add_argument("--systemd", action="store_true", help="run as a user systemd unit")
    s.add_argument("--session", help="orchestrator session id to inherit the model and route "
                                     "notifications from; defaults to $PI_BRIDGE_SESSION_ID "
                                     "(or $HERMES_SESSION_ID)")
    s.add_argument("--allow-alias", action="store_true",
                   help="allow internal alias models like 'hermes'")
    s.add_argument("--no-commit", action="store_true",
                   help="forbid this task from committing or pushing (enforced by git hooks)")
    s.add_argument("--context", help="opaque JSON object echoed in every notification")
    s.add_argument("--resume-of", dest="resume_of", metavar="TASK_ID",
                   help="continue an existing task's Pi session instead of starting fresh")
    s.add_argument("--template", choices=sorted(TEMPLATES),
                   help="prepend a proven brief header (fix|feature|review|port)")
    s.add_argument("--append-system-prompt", dest="append_system_prompt",
                   help="extra instruction appended to Pi's system prompt")
    s.add_argument("--plan", action="store_true",
                   help="make Pi list its intended edits before making them")
    s.add_argument("--test-cmd", dest="test_cmd",
                   help="run this command on completion; a non-zero exit fails the task")
    s.add_argument("--max-fixes", dest="max_fixes", type=int, default=0,
                   help="auto-fix attempts allowed after a red --test-cmd gate")

    rv = sub.add_parser("review", help="spawn a fresh Pi to attack a finished task's change")
    rv.add_argument("task_id")
    rv.add_argument("--model")
    rv.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)

    st = sub.add_parser("status")
    st.add_argument("task_id")

    m = sub.add_parser("model", help="show which model Pi would use, and why")
    m.add_argument("--explicit")
    m.add_argument("--session")
    m.add_argument("--allow-alias", action="store_true")

    pr = sub.add_parser("probe", help="print the heartbeat status for a task, without waiting")
    pr.add_argument("task_id")

    co = sub.add_parser("cost", help="sum token usage per repo")
    co.add_argument("--since", type=float, default=7 * 86400,
                    help="window in seconds (default 7 days)")

    sub.add_parser("list")
    sub.add_parser("reconcile")
    sub.add_parser("watchdog")
    sub.add_parser("reap", help="SIGTERM Pi processes whose task has no running row")

    a = ap.parse_args()

    if a.cmd == "serve":
        serve()
        return 0

    if a.cmd == "model":
        model, source = resolve_model(a.explicit, session_id=a.session,
                                      allow_alias=a.allow_alias)
        live, _ = active_model_from_state(a.session)
        print(json.dumps({
            "resolved_model": model,
            "source": source,
            "active_model": live,
            "config_default": config_default_model(),
            "looks_like_alias": _looks_like_alias(model),
        }, indent=2))
        return 0

    if a.cmd == "submit":
        try:
            ctx = json.loads(a.context) if a.context else None
        except json.JSONDecodeError as exc:
            print(f"--context is not valid JSON: {exc}", file=sys.stderr)
            return 2
        tid = submit(a.prompt, a.cwd, model=a.model, provider=a.provider,
                     timeout=a.timeout, use_systemd=a.systemd,
                     session=a.session, allow_alias=a.allow_alias, context=ctx,
                     no_commit=a.no_commit, resume_of=a.resume_of,
                     template=a.template, append_system_prompt=a.append_system_prompt,
                     plan=a.plan, test_cmd=a.test_cmd,
                     max_fixes=a.max_fixes)
        print(tid)
        return 0

    if a.cmd == "review":
        conn = db()
        try:
            row = conn.execute(
                "SELECT * FROM tasks WHERE task_id=?", (a.task_id,)
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            print(f"unknown task: {a.task_id}", file=sys.stderr)
            return 2
        print(review_task(row, model=a.model, timeout=a.timeout))
        return 0

    if a.cmd == "status":
        conn = db()
        try:
            row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (a.task_id,)).fetchone()
        finally:
            conn.close()
        if not row:
            print("unknown task", file=sys.stderr)
            return 1
        print(json.dumps(dict(row), indent=2, default=str))
        return 0

    if a.cmd == "probe":
        conn = db()
        try:
            row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (a.task_id,)).fetchone()
        finally:
            conn.close()
        if not row:
            print("unknown task", file=sys.stderr)
            return 1
        print(json.dumps(task_status(row), indent=2, default=str))
        return 0

    if a.cmd == "list":
        conn = db()
        try:
            rows = conn.execute(
                "SELECT task_id,status,resolution,created_at,finished_at FROM tasks "
                "ORDER BY created_at DESC LIMIT 50"
            ).fetchall()
        finally:
            conn.close()
        for r in rows:
            print(f"{r['task_id']}  {r['status']:<8} {r['resolution'] or '-':<16} "
                  f"{time.strftime('%H:%M:%S', time.localtime(r['created_at']))}")
        return 0

    if a.cmd == "cost":
        cutoff = time.time() - a.since
        conn = db()
        try:
            rows = conn.execute(
                "SELECT cwd, usage_json FROM tasks "
                "WHERE created_at>=? AND usage_json IS NOT NULL", (cutoff,)
            ).fetchall()
        finally:
            conn.close()
        totals: dict[str, list[int]] = {}
        for r in rows:
            try:
                u = json.loads(r["usage_json"]) or {}
            except ValueError:
                continue
            n = u.get("totalTokens") or (u.get("input", 0) + u.get("output", 0))
            acc = totals.setdefault(r["cwd"], [0, 0])
            acc[0] += int(n or 0)
            acc[1] += 1
        for cwd, (tokens, n) in sorted(totals.items(), key=lambda kv: -kv[1][0]):
            print(f"{tokens:>12,}  {n:>4} task(s)  {cwd}")
        return 0

    if a.cmd in ("reconcile", "watchdog"):
        changed = reconcile() if a.cmd == "reconcile" else watchdog_tick()
        print(json.dumps({"changed": changed}))
        return 0

    if a.cmd == "reap":
        print(json.dumps({"reaped": reap_orphans()}))
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
