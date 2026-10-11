"""pi-* slash commands for the Pi coding-agent bridge.

One command surface, several subcommands, so the whole thing is one plugin
rather than a dozen registrations. Registered names are ``pi-*``:

    pi-status [task_id]   bridge + task state
    pi-model              which model Pi would use, and why
    pi-model-set <id>     pin the model Pi runs with
    pi-model-clear        go back to inheriting the orchestrator's model
    pi-tasks [n]          recent tasks
    pi-wake               wake-layer configuration and reachability
    pi-help               this list

Reads go straight to the bridge's SQLite state and Pi's own config files, so the
commands work whether or not the bridge process is up. Writes touch only Pi's
config (``~/.pi/agent/settings.json``) - never Hermes config.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional

HOME = Path.home()


def _state_dir() -> Path:
    """The bridge's state dir, resolved the same way the engine resolves it."""
    if os.environ.get("PI_BRIDGE_STATE_DIR"):
        return Path(os.environ["PI_BRIDGE_STATE_DIR"])
    if os.environ.get("PI_BRIDGE_DIR"):
        return Path(os.environ["PI_BRIDGE_DIR"])
    xdg = os.environ.get("XDG_STATE_HOME")
    return (Path(xdg) if xdg else HOME / ".local" / "state") / "pi-bridge"


BRIDGE_DIR = _state_dir()
STATE_DB = BRIDGE_DIR / "state.db"
ORCHESTRATOR_DB = os.environ.get("PI_BRIDGE_ORCHESTRATOR_DB")
PI_SETTINGS = HOME / ".pi" / "agent" / "settings.json"
PI_MODELS = HOME / ".pi" / "agent" / "models.json"

BRIDGE_URL = os.environ.get("PI_BRIDGE_URL", "http://127.0.0.1:8799")

_HELP = """pi-* commands (Pi coding-agent bridge)

  pi-status [task_id]   bridge health + task counts (or one task in detail)
  pi-model              show the model Pi would use next, and where it came from
  pi-model-set <id>     pin Pi to a specific model
  pi-model-clear        unpin; Pi inherits the model the orchestrator is using
  pi-tasks [n]          list recent tasks (default 10)
  pi-wake               show wake-layer config and whether the endpoint answers
  pi-help               this list

Pi model resolution order: explicit pin -> env PI_BRIDGE_MODEL -> the model the
orchestrator is currently using -> config default. A pin applies to the NEXT
task, not one already running."""


# ---------------------------------------------------------------- helpers


def _fmt_time(ts) -> str:
    if not ts:
        return "-"
    try:
        return time.strftime("%m-%d %H:%M:%S", time.localtime(float(ts)))
    except (TypeError, ValueError):
        return "-"


def _bridge_reachable() -> tuple[bool, str]:
    try:
        with urllib.request.urlopen(f"{BRIDGE_URL}/list", timeout=3) as r:
            return (r.status == 200, f"HTTP {r.status}")
    except urllib.error.HTTPError as e:
        return (False, f"HTTP {e.code}")
    except Exception as e:
        return (False, type(e).__name__)


def _query(sql: str, args: tuple = ()) -> list:
    if not STATE_DB.is_file():
        return []
    try:
        conn = sqlite3.connect(f"file:{STATE_DB}?mode=ro", uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        try:
            return conn.execute(sql, args).fetchall()
        finally:
            conn.close()
    except sqlite3.Error:
        return []


def _read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _orchestrator_active_model() -> Optional[str]:
    """The model the orchestrator is using right now, from its own state db."""
    if not ORCHESTRATOR_DB:
        return None
    db = Path(ORCHESTRATOR_DB)
    if not db.is_file():
        return None
    try:
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=5)
        try:
            row = conn.execute(
                """SELECT model FROM sessions
                   WHERE model IS NOT NULL AND model != ''
                   ORDER BY (ended_at IS NULL) DESC, last_activity_at DESC
                   LIMIT 1"""
            ).fetchone()
            return row[0] if row else None
        finally:
            conn.close()
    except sqlite3.Error:
        return None


# ---------------------------------------------------------------- commands


def _cmd_status(args: list[str]) -> str:
    ok, detail = _bridge_reachable()
    lines = [f"Bridge: {'UP' if ok else 'DOWN'} ({detail})  {BRIDGE_URL}"]
    try:
        import subprocess
        active = subprocess.run(
            ["systemctl", "--user", "is-active", "pi-bridge.service"],
            capture_output=True, text=True, timeout=5,
        ).stdout.strip()
        lines.append(f"Service: {active or 'unknown'}")
    except Exception:
        pass

    if args:
        tid = args[0]
        rows = _query("SELECT * FROM tasks WHERE task_id=?", (tid,))
        if not rows:
            return "\n".join(lines + [f"No such task: {tid}"])
        r = rows[0]
        lines += [
            "",
            f"task      {r['task_id']}",
            f"status    {r['status']}  ({r['resolution'] or '-'} via {r['resolved_by'] or '-'})",
            f"model     {r['model'] or '-'}  [source={r['model_source'] or '-'}]",
            f"cwd       {r['cwd']}",
            f"created   {_fmt_time(r['created_at'])}",
            f"finished  {_fmt_time(r['finished_at'])}",
            f"result    {(r['result_text'] or '')[:400]}",
        ]
        return "\n".join(lines)

    counts = _query("SELECT status, COUNT(*) n FROM tasks GROUP BY status")
    total = sum(c["n"] for c in counts)
    lines.append("")
    lines.append(f"Tasks: {total} total")
    for c in counts:
        lines.append(f"  {c['status']:<9} {c['n']}")
    running = [r for r in counts if r["status"] == "running"]
    lines.append("")
    lines.append("In flight: " + (str(running[0]["n"]) if running else "0"))
    return "\n".join(lines)


def _cmd_model(args: list[str]) -> str:
    settings = _read_json(PI_SETTINGS)
    models = _read_json(PI_MODELS)

    pinned = settings.get("defaultModel")
    provider = settings.get("defaultProvider")
    active = _orchestrator_active_model()
    env_pin = os.environ.get("PI_BRIDGE_MODEL")

    lines = [
        "Pi model resolution",
        "",
        f"  pinned in settings.json : {pinned or '(none)'}",
        f"  env PI_BRIDGE_MODEL     : {env_pin or '(unset)'}",
        f"  orchestrator active     : {active or '(unknown)'}",
        f"  provider                : {provider or '(unset)'}",
    ]

    # What the bridge would actually pick, in its documented order.
    if env_pin:
        eff, why = env_pin, "env override"
    elif active:
        eff, why = active, "inherits the orchestrator (per task)"
    elif pinned:
        eff, why = pinned, "settings.json default"
    else:
        eff, why = "(unset)", "no model resolved"

    lines += ["", f"  -> next task uses: {eff}", f"     because: {why}"]

    provs = (models.get("providers") or {})
    if provs:
        lines.append("")
        lines.append("Configured providers:")
        for name, cfg in provs.items():
            if not isinstance(cfg, dict):
                continue
            declared = cfg.get("models") or []
            ids = ", ".join(m.get("id", "?") for m in declared if isinstance(m, dict))
            lines.append(f"  {name} -> {cfg.get('baseUrl', '?')}")
            lines.append(f"     declared: {ids or '(none)'}  (undeclared ids pass through)")
    return "\n".join(lines)


def _cmd_model_set(args: list[str]) -> str:
    if not args:
        return "Usage: pi-model-set <model-id>\n\nSee pi-model for the current resolution."
    model = args[0].strip()
    if not model:
        return "Usage: pi-model-set <model-id>"

    settings = _read_json(PI_SETTINGS)
    settings["defaultModel"] = model
    try:
        PI_SETTINGS.parent.mkdir(parents=True, exist_ok=True)
        PI_SETTINGS.write_text(json.dumps(settings, indent=2), encoding="utf-8")
        os.chmod(PI_SETTINGS, 0o600)
    except OSError as e:
        return f"Could not write {PI_SETTINGS}: {e}"

    return (
        f"Pinned Pi model -> {model}\n\n"
        "Applies to the NEXT task. A task already running keeps the model it started with.\n"
        "Note: the bridge resolves the Hermes model per task, so this pin is the fallback\n"
        "used when Hermes' own model cannot be resolved. Use pi-model-clear to unpin."
    )


def _cmd_model_clear(args: list[str]) -> str:
    settings = _read_json(PI_SETTINGS)
    if "defaultModel" in settings:
        settings.pop("defaultModel", None)
        try:
            PI_SETTINGS.write_text(json.dumps(settings, indent=2), encoding="utf-8")
            os.chmod(PI_SETTINGS, 0o600)
        except OSError as e:
            return f"Could not write {PI_SETTINGS}: {e}"
        return "Unpinned Pi model. Pi now inherits whatever model Hermes is using."
    return "No pin was set; Pi already inherits the Hermes model."


def _cmd_tasks(args: list[str]) -> str:
    n = 10
    if args:
        try:
            n = max(1, min(100, int(args[0])))
        except ValueError:
            return "Usage: pi-tasks [count]  (count 1-100)"
    rows = _query(
        "SELECT task_id,status,model,resolution,created_at FROM tasks "
        "ORDER BY created_at DESC LIMIT ?", (n,),
    )
    if not rows:
        return "No tasks recorded yet."
    lines = [f"Recent Pi tasks (last {len(rows)}):", ""]
    for r in rows:
        lines.append(
            f"  {r['task_id']}  {r['status']:<8} {_fmt_time(r['created_at'])}  "
            f"{(r['model'] or '-')[:34]}"
        )
    return "\n".join(lines)


def _bridge_service_env(key: str) -> str:
    """Read an env var from the bridge's systemd unit, falling back to this process.

    The wake URL/secret live on pi-bridge.service, not in the gateway's own
    environment, so reading os.environ alone reports them as unset.
    """
    val = os.environ.get(key, "")
    if val:
        return val
    unit = HOME / ".config" / "systemd" / "user" / "pi-bridge.service"
    try:
        for line in unit.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith(f"Environment={key}="):
                return line.split("=", 2)[2].strip()
    except OSError:
        pass
    return ""


def _cmd_wake(args: list[str]) -> str:
    url = _bridge_service_env("PI_BRIDGE_NOTIFY_URL")
    secret = _bridge_service_env("PI_BRIDGE_NOTIFY_SECRET")
    lines = ["Wake layer", ""]
    if not url:
        lines.append("  disabled (PI_BRIDGE_NOTIFY_URL unset in the bridge service)")
        return "\n".join(lines)

    lines.append(f"  endpoint : {url}")
    lines.append(f"  secret   : {'set' if secret else 'unset'}")

    subs = HOME / ".hermes" / "webhook_subscriptions.json"
    d = _read_json(subs)
    if "pi-done" in d:
        route = d["pi-done"]
        lines += [
            f"  route    : pi-done (deliver={route.get('deliver')}, "
            f"chat={route.get('deliver_extra', {}).get('chat_id', '-')})",
        ]
    else:
        lines.append("  route    : pi-done NOT configured")

    # Probe the endpoint with an empty POST - a 4xx means it is listening and
    # rejecting, which still proves reachability.
    try:
        req = urllib.request.Request(url, data=b"{}", method="POST",
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as r:
            lines.append(f"  reachable: yes (HTTP {r.status})")
    except urllib.error.HTTPError as e:
        lines.append(f"  reachable: yes (HTTP {e.code} - rejected, endpoint alive)")
    except Exception as e:
        lines.append(f"  reachable: NO ({type(e).__name__})")
    return "\n".join(lines)


_SUBCOMMANDS = {
    "status": _cmd_status,
    "model": _cmd_model,
    "model-set": _cmd_model_set,
    "model-clear": _cmd_model_clear,
    "tasks": _cmd_tasks,
    "wake": _cmd_wake,
    "help": lambda _a: _HELP,
}


def _make_handler(fn):
    """Adapt a subcommand function into a raw-args slash handler."""
    def _handler(raw_args: str) -> str:
        return fn((raw_args or "").strip().split())
    return _handler


def _cmd_help(_args: list[str]) -> str:
    return _HELP


# Each command is registered separately so the prefix is literal: /pi-status,
# /pi-model, ... rather than one /pi with subcommands.
_COMMANDS = [
    ("pi-status", _cmd_status, "Bridge health and task state.", "[task_id]"),
    ("pi-model", _cmd_model, "Which model Pi would use next, and why.", ""),
    # args_hint uses brackets, not angle brackets: the Telegram menu builder drops
    # any command whose hint starts with "<" (it assumes selecting it bare would be
    # incomplete), which would hide this from the bot command menu.
    ("pi-model-set", _cmd_model_set, "Pin the model Pi runs with.", "[model-id]"),
    ("pi-model-clear", _cmd_model_clear, "Unpin; Pi inherits the Hermes model.", ""),
    ("pi-tasks", _cmd_tasks, "List recent Pi tasks.", "[count]"),
    ("pi-wake", _cmd_wake, "Wake-layer config and reachability.", ""),
    ("pi-help", _cmd_help, "List all pi-* commands.", ""),
]


def register(ctx) -> None:
    for name, fn, description, hint in _COMMANDS:
        ctx.register_command(name, handler=_make_handler(fn),
                             description=description, args_hint=hint)
