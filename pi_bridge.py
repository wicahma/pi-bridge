#!/usr/bin/env python3
"""
Pi ↔ Hermes bridge.

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
  ~/.hermes/pi-bridge/state.db     task state
  ~/.hermes/pi-bridge/tasks/       one prompt file per task
  ~/.hermes/pi-bridge/logs/        per-task stdout/stderr

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
BRIDGE_DIR = Path(os.environ.get("PI_BRIDGE_DIR", HOME / ".hermes" / "pi-bridge"))
TASKS_DIR = BRIDGE_DIR / "tasks"
LOGS_DIR = BRIDGE_DIR / "logs"
DB_PATH = BRIDGE_DIR / "state.db"

CALLBACK_HOST = os.environ.get("PI_BRIDGE_HOST", "127.0.0.1")
CALLBACK_PORT = int(os.environ.get("PI_BRIDGE_PORT", "8799"))
CALLBACK_PATH = "/callback"

# ---------------------------------------------------------------- wake layer

# Optional: notify Hermes (or anything HTTP) that a task reached a terminal
# state, so an idle orchestrator can be woken instead of polled. Entirely
# additive - if this is unconfigured or fails, task resolution is unaffected.
HERMES_WEBHOOK_URL = os.environ.get("PI_BRIDGE_WAKE_URL", "")
HERMES_WEBHOOK_SECRET = os.environ.get("PI_BRIDGE_WAKE_SECRET", "")

# Optional: periodic progress heartbeat. A separate route from the completion
# wake, delivered with deliver_only so it costs no agent run. Unset -> silent.
HERMES_STATUS_URL = os.environ.get("PI_BRIDGE_STATUS_URL", "")
HEARTBEAT_INTERVAL = int(os.environ.get("PI_BRIDGE_HEARTBEAT_INTERVAL", "60"))


def _wake_payload(row: sqlite3.Row) -> dict:
    """Shape the callback payload from the finished task row."""
    origin_chat = row["origin_chat_id"] if "origin_chat_id" in row.keys() else None
    origin_thread = row["origin_thread_id"] if "origin_thread_id" in row.keys() else None
    return {
        "task_id": row["task_id"],
        "status": row["status"],
        "cwd": row["cwd"],
        "model": row["model"],
        "resolution": row["resolution"],
        "resolved_by": row["resolved_by"],
        "result_text": (row["result_text"] or "")[:4000],
        "chat_id": origin_chat or "1274076905",
        "thread_id": origin_thread or "3482164",
    }


def _post_signed(url: str, payload: dict, event: str) -> None:
    """POST a signed JSON body. Never raises into the caller."""
    if not url:
        return
    try:
        body = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if HERMES_WEBHOOK_SECRET:
            sig = hmac.new(
                HERMES_WEBHOOK_SECRET.encode("utf-8"), body, hashlib.sha256
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
    _post_signed(HERMES_WEBHOOK_URL, payload, "pi-task")


def wake_async(task_id: str) -> None:
    """
    Fire the wake notification on a daemon thread.

    Deliberately asynchronous: the task is already resolved by the time this is
    called, so a slow or dead webhook endpoint must not block the bridge, hold a
    request thread, or delay any other task. Failures are logged and dropped.
    """
    if not HERMES_WEBHOOK_URL:
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


# ---------------------------------------------------------------- hermes model

HERMES_HOME = Path(os.environ.get("HERMES_HOME", HOME / ".hermes"))
HERMES_STATE_DB = HERMES_HOME / "state.db"
HERMES_CONFIG = HERMES_HOME / "config.yaml"

# Models that are internal aliases, not real model ids the gateway will accept.
# `hermes` is an alias that routes to whatever the gateway's default is; using it
# as a Pi model id would silently pin Pi to a different model than Hermes.
_ALIAS_PREFIXES = ("hermes", "main-agent", "code-reviewer", "lightweight", "paid-models")


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
    if not HERMES_STATE_DB.is_file():
        return (None, None)
    try:
        conn = sqlite3.connect(f"file:{HERMES_STATE_DB}?mode=ro", uri=True, timeout=10)
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


def active_session_origin(session_id: str | None = None) -> tuple[str | None, str | None, str | None]:
    """
    Returns (session_id, chat_id, thread_id) of the active Hermes session.
    Used to route wake notifications directly back to the requesting chat and topic.
    """
    if not HERMES_STATE_DB.is_file():
        return (None, None, None)
    try:
        conn = sqlite3.connect(f"file:{HERMES_STATE_DB}?mode=ro", uri=True, timeout=10)
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
    if not HERMES_CONFIG.is_file():
        return None
    try:
        import yaml  # optional dependency
        with HERMES_CONFIG.open(encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh) or {}
        m = (cfg.get("model") or {}).get("default")
        return m or None
    except Exception:
        # Minimal fallback that does not require pyyaml.
        try:
            text = HERMES_CONFIG.read_text(encoding="utf-8")
        except OSError:
            return None
        m = re.search(r"^model:\s*\n\s+default:\s*(\S+)", text, re.M)
        return m.group(1) if m else None


def resolve_model(explicit: str | None = None, session_id: str | None = None,
                  allow_alias: bool = False) -> tuple[str, str]:
    """
    Decide which model Pi should run. Returns (model, source).

    Order:
      1. explicit override (--model / HERMES_PI_MODEL)
      2. the model Hermes is currently using (live session)
      3. config.yaml default
      4. 'hermes' (gateway alias)

    Internal aliases are skipped when a concrete model is available, because
    pinning Pi to an alias does not guarantee it follows Hermes.
    """
    if explicit:
        return (explicit, "explicit")

    env_override = os.environ.get("HERMES_PI_MODEL")
    if env_override:
        return (env_override, "env")

    model, _ = active_model_from_state(session_id)
    if model and (allow_alias or not _looks_like_alias(model)):
        return (model, "hermes-session")
    if model:
        # Only an alias was found; prefer a concrete configured default.
        cfg = config_default_model()
        if cfg and not _looks_like_alias(cfg):
            return (cfg, "hermes-config")
        return (model, "hermes-session-alias")

    cfg = config_default_model()
    if cfg:
        return (cfg, "hermes-config")
    return ("hermes", "fallback")


# Where Pi keeps sessions. Pi groups by working directory.
PI_SESSIONS_DIR = Path(
    os.environ.get("PI_CODING_AGENT_SESSION_DIR", HOME / ".pi" / "agent" / "sessions")
)

DEFAULT_TIMEOUT = int(os.environ.get("PI_BRIDGE_TIMEOUT", "7200"))  # 2h
WATCHDOG_INTERVAL = int(os.environ.get("PI_BRIDGE_WATCHDOG_INTERVAL", "30"))

PI_BIN = os.environ.get("PI_BIN", "pi")

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
    model_source  TEXT,                   -- explicit|env|hermes-session|hermes-config|fallback
    origin_chat_id   TEXT,
    origin_thread_id TEXT
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
                        info["last_text"] = part["text"].strip().splitlines()[0][:140]
    except OSError:
        pass
    return info


def task_status(row: sqlite3.Row) -> dict:
    """Classify a running task for the heartbeat: what state, how long, how far."""
    now = time.time()
    elapsed = int(now - (row["started_at"] or row["created_at"]))
    # pid is None for --systemd tasks; there the process is alive, we just cannot see it.
    alive = True if row["pid"] is None else pid_alive(row["pid"])
    act = _session_activity(row["session_id"], row["cwd"])
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

    return {
        "task_id": row["task_id"],
        "state": state,
        "elapsed": f"{elapsed // 60}m{elapsed % 60:02d}s",
        "iteration": act["iteration"],
        "last_tool": act["last_tool"] or "-",
        "detail": detail,
        "cwd": row["cwd"],
        "model": row["model"],
        "chat_id": row["origin_chat_id"] or "1274076905",
        "thread_id": row["origin_thread_id"] or "3482164",
    }


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


def resolve(task_id: str, status: str, *, text=None, usage=None,
            exit_code=None, resolution=None, resolved_by=None) -> bool:
    """
    Write a terminal outcome for a task, exactly once. Returns True if this call
    performed the transition, False if the task was already final.
    """
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
                       usage_json=?, exit_code=?, resolution=?, resolved_by=?
                   WHERE task_id=?""",
                (
                    status,
                    time.time(),
                    text,
                    json.dumps(usage) if usage else None,
                    exit_code,
                    resolution,
                    resolved_by,
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


def ensure_provider_placeholder() -> None:
    """
    Guarantee Pi has at least one declared model for the gateway provider.

    Pi refuses a provider that declares zero models ("Unknown provider"), but it
    happily passes an *undeclared* model id through to the server
    ("Using custom model id"). Declaring one placeholder therefore unlocks
    arbitrary model ids - which is what lets Hermes inject the model it is
    currently using without hardcoding a model list that the 1200-entry gateway
    catalog would invalidate anyway.
    """
    models_json = HOME / ".pi" / "agent" / "models.json"
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
            if "8080" in base or name == "local":
                prov["models"] = [{
                    "id": "hermes", "name": "Passthrough (Hermes)",
                    "reasoning": True, "contextWindow": 131072, "maxTokens": 32768,
                }]
                changed = True
    if changed:
        try:
            models_json.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
        except OSError:
            pass


def build_env(task_id: str) -> dict:
    env = os.environ.copy()
    env["HERMES_CALLBACK_URL"] = f"http://{CALLBACK_HOST}:{CALLBACK_PORT}{CALLBACK_PATH}"
    env["HERMES_TASK_ID"] = task_id
    return env


def submit(
    prompt: str,
    cwd: str,
    *,
    model: str | None = None,
    provider: str | None = None,
    timeout: int = DEFAULT_TIMEOUT,
    extra_args: list[str] | None = None,
    use_systemd: bool = False,
    hermes_session_id: str | None = None,
    allow_alias: bool = False,
) -> str:
    """
    Spawn a Pi task and return immediately. Nothing here blocks on Pi.

    `model=None` is the important case: Pi inherits whatever model Hermes is
    using at submit time (see resolve_model). Because the model is resolved at
    spawn, changing the Hermes model affects the next Pi task, not one already
    running - that is the honest semantics of a per-task model pin.
    """
    task_id = uuid.uuid4().hex[:16]
    session_id = f"pi-{task_id}"
    cwd = str(Path(cwd).resolve())
    TASKS_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    chosen_model, model_source = resolve_model(
        model, session_id=hermes_session_id, allow_alias=allow_alias
    )
    ensure_provider_placeholder()

    prompt_file = TASKS_DIR / f"{task_id}.txt"
    prompt_file.write_text(prompt, encoding="utf-8")
    log_path = LOGS_DIR / f"{task_id}.log"

    args = [PI_BIN, "-p", prompt, "--session-id", session_id, "--approve",
            "--model", chosen_model]
    if provider:
        args += ["--provider", provider]
    if extra_args:
        args += extra_args

    if use_systemd:
        # Survives a Hermes restart: Pi becomes its own user unit.
        unit = f"pi-task-{task_id}"
        cmd = [
            "systemd-run", "--user", f"--unit={unit}",
            "--property=Type=oneshot",
            f"--working-directory={cwd}",
            f"--setenv=HERMES_CALLBACK_URL={os.environ.get('HERMES_CALLBACK_URL') or f'http://{CALLBACK_HOST}:{CALLBACK_PORT}{CALLBACK_PATH}'}",
            f"--setenv=HERMES_TASK_ID={task_id}",
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
                env=build_env(task_id),
                start_new_session=True,  # detach: survives the caller exiting
            )
        pid = proc.pid

    now = time.time()
    _, origin_chat, origin_thread = active_session_origin(hermes_session_id)
    origin_chat = origin_chat or "1274076905"
    origin_thread = origin_thread or "3482164"

    conn = db()
    try:
        conn.execute(
            """INSERT INTO tasks (task_id, prompt, cwd, session_id, model, provider,
                   pid, status, created_at, started_at, deadline, log_path, model_source,
                   origin_chat_id, origin_thread_id)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                task_id, prompt, cwd, session_id, chosen_model, provider, pid,
                "running", now, now, now + timeout, str(log_path), model_source,
                origin_chat, origin_thread,
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


def watchdog_loop(stop: threading.Event) -> None:
    while not stop.wait(WATCHDOG_INTERVAL):
        try:
            for tid in watchdog_tick():
                print(f"[watchdog] resolved {tid}", flush=True)
        except Exception as exc:  # never let the watchdog die
            print(f"[watchdog] error: {exc}", flush=True)


def heartbeat_loop(stop: threading.Event) -> None:
    """Push a status line for every running task, so a long run is never silent."""
    while not stop.wait(HEARTBEAT_INTERVAL):
        try:
            conn = db()
            try:
                rows = conn.execute(
                    "SELECT * FROM tasks WHERE status='running' ORDER BY created_at"
                ).fetchall()
            finally:
                conn.close()
            for row in rows:
                payload = task_status(row)
                payload["delivery_id"] = f"{row['task_id']}-hb-{int(time.time())}"
                _post_signed(HERMES_STATUS_URL, payload, "pi-status")
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
        n = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(n) if n else b"{}"
        try:
            payload = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            return self._json(400, {"ok": False, "error": "bad json"})

        if self.path.rstrip("/") == CALLBACK_PATH:
            ok = handle_callback(payload)
            print(f"[callback] task={payload.get('task_id')} accepted={ok}", flush=True)
            return self._json(200, {"ok": True, "accepted": ok})

        if self.path.rstrip("/") == "/submit":
            tid = submit(
                payload["prompt"],
                payload.get("cwd", os.getcwd()),
                model=payload.get("model"),
                provider=payload.get("provider"),
                timeout=int(payload.get("timeout", DEFAULT_TIMEOUT)),
                hermes_session_id=payload.get("hermes_session_id"),
                allow_alias=bool(payload.get("allow_alias", False)),
            )
            return self._json(200, {"ok": True, "task_id": tid})

        if self.path.rstrip("/") == "/model":
            model, source = resolve_model(
                payload.get("explicit"),
                session_id=payload.get("hermes_session_id"),
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
    if HERMES_STATUS_URL:
        threading.Thread(target=heartbeat_loop, args=(stop,), daemon=True).start()
        print(f"[serve] heartbeat every {HEARTBEAT_INTERVAL}s -> {HERMES_STATUS_URL}", flush=True)

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
    s.add_argument("--model", help="explicit model; omit to inherit the Hermes model")
    s.add_argument("--provider")
    s.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    s.add_argument("--systemd", action="store_true", help="run as a user systemd unit")
    s.add_argument("--hermes-session", help="Hermes session id to inherit the model from")
    s.add_argument("--allow-alias", action="store_true",
                   help="allow internal alias models like 'hermes'")

    st = sub.add_parser("status")
    st.add_argument("task_id")

    m = sub.add_parser("model", help="show which model Pi would use, and why")
    m.add_argument("--explicit")
    m.add_argument("--hermes-session")
    m.add_argument("--allow-alias", action="store_true")

    pr = sub.add_parser("probe", help="print the heartbeat status for a task, without waiting")
    pr.add_argument("task_id")

    sub.add_parser("list")
    sub.add_parser("reconcile")
    sub.add_parser("watchdog")

    a = ap.parse_args()

    if a.cmd == "serve":
        serve()
        return 0

    if a.cmd == "model":
        model, source = resolve_model(a.explicit, session_id=a.hermes_session,
                                      allow_alias=a.allow_alias)
        live, _ = active_model_from_state(a.hermes_session)
        print(json.dumps({
            "resolved_model": model,
            "source": source,
            "hermes_active_model": live,
            "hermes_config_default": config_default_model(),
            "looks_like_alias": _looks_like_alias(model),
        }, indent=2))
        return 0

    if a.cmd == "submit":
        tid = submit(a.prompt, a.cwd, model=a.model, provider=a.provider,
                     timeout=a.timeout, use_systemd=a.systemd,
                     hermes_session_id=a.hermes_session, allow_alias=a.allow_alias)
        print(tid)
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

    if a.cmd in ("reconcile", "watchdog"):
        changed = reconcile() if a.cmd == "reconcile" else watchdog_tick()
        print(json.dumps({"changed": changed}))
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
