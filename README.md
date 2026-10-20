# pi-bridge

Supervise the [Pi coding agent](https://www.npmjs.com/package/@earendil-works/pi-coding-agent)
from any orchestrator, over HTTP, without ever blocking.

Dispatch a long coding task, return immediately, and learn the outcome from a
signed push callback. The bridge is deliberately **orchestrator-agnostic**: it
stores whatever opaque context you hand it at submit time and echoes it back in
every notification, so it never needs to know what a chat, topic, thread or
session is.

```
orchestrator ──POST /submit──────────────▶ pi-bridge ──spawns──▶ pi
     ▲                                        │
     └──── signed POST: completion ───────────┘
     └──── signed POST: heartbeat ────────────┘
```

## Why

A coding agent run takes minutes to hours. Blocking on it wastes the caller; a
push-only design loses tasks whenever a callback is dropped. This bridge closes
both gaps:

- **Fire-and-forget submit.** `submit()` returns a task id in milliseconds.
- **Push completion.** A signed callback fires when Pi settles, so an idle
  orchestrator wakes instead of polling.
- **Watchdog.** A background tick reconciles tasks whose process died, whose
  callback never arrived, or whose result is only visible in Pi's session file.
- **Heartbeat.** A long run pushes a status line (working / idle / error / gone)
  every interval, so a 60-minute job is never a silent black box.
- **Never blocks on the caller.** Notifications are fire-and-forget on daemon
  threads. A dead webhook endpoint cannot delay a task.

## Requirements

- Python **3.10+** — standard library only, no third-party runtime dependencies.
- The **`pi` CLI** on `PATH` (or point `PI_BIN` at it).
- Optional: a systemd user session, if you want the bridge to outlive your shell.

## Install

```sh
git clone https://github.com/wicahma/pi-bridge && cd pi-bridge
pip install -e .          # provides the `pi-bridge` command
# or run it directly, no install:  ./pi_bridge.py --help
```

Pi's own config is untouched. Gateway-backed setups that serve arbitrary model
ids should set `PI_BRIDGE_PASSTHROUGH_PROVIDERS` — see [Configuration](#configuration).

## Run

```sh
pi-bridge serve           # HTTP API + watchdog + heartbeat
```

A minimal systemd user unit is in
[`integrations/hermes/pi-bridge.service`](integrations/hermes/pi-bridge.service);
it works unchanged for any orchestrator, not just Hermes:

```sh
install -Dm644 integrations/hermes/pi-bridge.service ~/.config/systemd/user/pi-bridge.service
systemctl --user daemon-reload
systemctl --user enable --now pi-bridge
```

## Use

CLI:

```sh
pi-bridge submit "add a /healthz endpoint and a test" --cwd /path/to/repo
# 3f9a1c2e8b7d4a06

pi-bridge status 3f9a1c2e8b7d4a06
pi-bridge probe  3f9a1c2e8b7d4a06     # heartbeat classification, without waiting
pi-bridge list
```

The CLI reads the same `PI_BRIDGE_*` environment as the service. A host that keeps its
configuration in a systemd drop-in
(`~/.config/systemd/user/pi-bridge.service.d/*.conf`) has that configuration applied to the
service only - export the same variables in your shell before a CLI `submit`, or the bridge
falls back to its built-in defaults and cannot inherit the orchestrator's model.
For notifications to reach the right conversation, a submit must know which session
asked. `submit` takes the session from `--session` / `session_id`, else from
`$PI_BRIDGE_SESSION_ID`, else from `$HERMES_SESSION_ID`. The chat and topic come from
that session's row in the orchestrator db; if the db is unavailable they come from
`$PI_BRIDGE_CHAT_ID` / `$PI_BRIDGE_THREAD_ID` (or `$HERMES_SESSION_CHAT_ID` /
`$HERMES_SESSION_THREAD_ID`). With no session and no route env the task is stored
unrouted, and its heartbeat and completion go to the notification default - never to
an unrelated conversation guessed from the db.

HTTP:

```sh
curl -s localhost:8799/submit -d '{"prompt":"...","cwd":"/repo"}'
curl -s localhost:8799/status/3f9a1c2e8b7d4a06
curl -s localhost:8799/list
```

### CLI reference

| Command | Purpose |
| --- | --- |
| `serve` | run the HTTP API, watchdog and heartbeat |
| `submit "<prompt>" [--cwd DIR] [--model M] [--provider P] [--timeout S] [--session ID] [--context JSON] [--no-commit] [--systemd]` | dispatch a task, print its id |
| `status <id>` | current row from `state.db` |
| `model [--explicit M] [--session ID]` | show which model Pi would use, and why |
| `probe <id>` | classify a running task from its session file, without waiting |
| `list` | recent tasks |
| `reconcile` | one watchdog pass, in the foreground |
| `watchdog` | run the watchdog loop alone |

### HTTP API

| Method | Path | Body / result |
| --- | --- | --- |
| `POST` | `/submit` | `{prompt, cwd?, model?, provider?, timeout?, session_id?, allow_alias?, context?, no_commit?, resume_of?, template?, append_system_prompt?, plan?, test_cmd?, max_fixes?}` → `{ok, task_id}` |
| `POST` | `/model` | `{explicit?, session_id?, allow_alias?}` → `{ok, model, source}` |
| `GET` | `/list` | → `{ok, tasks: [...]}` (most recent 100) |
| `GET` | `/status/<id>` | → `{ok, task}`; `404` for an unknown id |
| `POST` | `/callback` | Pi's own settle report (see [`docs/PROTOCOL.md`](docs/PROTOCOL.md)) |

`template` is one of `fix`, `feature`, `review`, `port`. `resume_of` names a
finished task whose Pi session the new task continues. `test_cmd` is run when
the task reports done, and a non-zero exit resolves it as failed; `max_fixes`
(0 = off) chains that many follow-up attempts onto a red gate. An unknown
template or task id answers 400 with the reason.

`context` is an arbitrary JSON object, stored verbatim and echoed back in every
notification for that task. That is the entire integration surface: the bridge
never interprets it.

`--no-commit` (HTTP: `no_commit`) makes the task unable to commit or push. The bridge
points `core.hooksPath` at its own `git-hooks/` for that Pi process through
`GIT_CONFIG_*`, so git itself refuses: a prompt cannot talk Pi out of it, the target
repo is not modified, and the user's global git config is not replaced. The hooks are
only reachable when the flag is set, so a repo's own hooks are never shadowed.

## Configuration

Every knob is an environment variable. Unset means "feature off", so a bare
`serve` is a plain task runner with no outbound network calls.

| Variable | Default | Meaning |
| --- | --- | --- |
| `PI_BRIDGE_STATE_DIR` | `$XDG_STATE_HOME/pi-bridge` | SQLite state, task prompts, logs |
| `PI_BRIDGE_HOST` | `127.0.0.1` | bind address |
| `PI_BRIDGE_PORT` | `8799` | bind port |
| `PI_BRIDGE_NOTIFY_URL` | *(unset)* | POST completion notifications here |
| `PI_BRIDGE_NOTIFY_SECRET` | *(unset)* | HMAC-SHA256 key for those notifications |
| `PI_BRIDGE_HEARTBEAT_URL` | *(unset)* | POST periodic status here |
| `PI_BRIDGE_HEARTBEAT_INTERVAL` | `60` | seconds between heartbeats |
| `PI_BRIDGE_STREAM` | `0` | `1` forwards every new tool call and assistant text block as its own status message instead of one digest per tick |
| `PI_BRIDGE_STREAM_MAX` | `6` | events posted per tick when streaming; the rest collapse into one `+N more` line |
| `PI_BRIDGE_TIMEOUT` | `7200` | default task timeout, seconds |
| `PI_BRIDGE_WATCHDOG_INTERVAL` | `30` | seconds between watchdog ticks |
| `PI_BRIDGE_ORPHAN_IDLE` | `900` | reap a row-less Pi process after this many idle seconds |
| `PI_BRIDGE_MODEL` | *(unset)* | force the model Pi runs |
| `PI_BRIDGE_FALLBACK_MODEL` | *(unset)* | model when nothing else resolves; unset = let Pi decide |
| `PI_BRIDGE_MODEL_ALIASES` | *(unset)* | comma-separated routing aliases that are never used as model ids |
| `PI_BRIDGE_PASSTHROUGH_PROVIDERS` | *(unset)* | providers allowed to serve undeclared model ids |
| `PI_BRIDGE_ORCHESTRATOR_DB` | *(unset)* | orchestrator SQLite db, to inherit its live model; must be set in every process that reads it - the bridge **and** the orchestrator process hosting the plugin |
| `PI_BRIDGE_ORCHESTRATOR_CONFIG` | *(unset)* | orchestrator YAML, for its default model |
| `PI_BRIDGE_DIR` | *(unset)* | legacy alias for `PI_BRIDGE_STATE_DIR` |
| `PI_BIN` | `pi` | pi executable |
| `PI_CODING_AGENT_SESSION_DIR` | `~/.pi/agent/sessions` | where Pi keeps session jsonl |

### Model resolution

Pi is started with `--model` only when a model actually resolves. Order:

1. explicit `--model` / `PI_BRIDGE_MODEL`
2. the orchestrator's live model, read from `PI_BRIDGE_ORCHESTRATOR_DB`
3. the orchestrator's default model, read from `PI_BRIDGE_ORCHESTRATOR_CONFIG`
4. `PI_BRIDGE_FALLBACK_MODEL`, if set
5. otherwise no `--model` at all — Pi uses its own configured default

Values listed in `PI_BRIDGE_MODEL_ALIASES` are skipped whenever a concrete model
is available, because pinning Pi to a routing alias silently pins it to whatever
the gateway default happens to be. If a resolved model is not declared in Pi's
`models.json`, the bridge attaches the first `PI_BRIDGE_PASSTHROUGH_PROVIDERS`
entry that Pi will accept, so an undeclared id still reaches the server.

### Orphan reaping

The watchdog walks tasks that still have a `running` row. A Pi process whose row
is gone — or was never written — is therefore invisible to it, and can sit
sleeping forever. Each watchdog tick also reaps those: a process is `SIGTERM`ed
only when it is named `pi`, carries `PI_BRIDGE_TASK_ID`, has no `running` row,
and has a session file that has not been written for `PI_BRIDGE_ORPHAN_IDLE`
seconds. The session-file gate is what makes it safe: `submit()` starts Pi
before it writes the row, so a just-spawned process has neither yet and is left
alone, while a working one keeps touching its session file. Run it by hand with
`pi_bridge.py reap`.

## Protocol

See [`docs/PROTOCOL.md`](docs/PROTOCOL.md) for the callback and heartbeat
contracts, the signature scheme, and the task lifecycle.

## Integrations

The engine has no orchestrator-specific code. An integration only has to:

1. **Submit** — `POST /submit` with a prompt and working directory.
2. **Receive** — expose an HTTP endpoint that verifies the HMAC signature and
   renders the payload wherever the user lives.
3. *(optional)* **Inherit** — point `PI_BRIDGE_ORCHESTRATOR_DB` at the
   orchestrator's own SQLite state to reuse its live model and its routing keys.

### Implemented

- **[Hermes Agent](https://hermes-agent.nousresearch.com/docs)** — see
  [`integrations/hermes/`](integrations/hermes/README.md): a plugin exposing
  `/pi_*` slash commands, a Pi extension that calls back on settle, two webhook
  route definitions (one mirrored into the session, one `deliver_only`), and the
  systemd unit.
- **Anything with HTTP + HMAC** — see
  [`integrations/generic/`](integrations/generic/README.md): a small stdlib-only
  orchestrator that submits a task and verifies the signed callback. Point the
  bridge's `PI_BRIDGE_NOTIFY_URL` at it.

### Planned / candidate

These follow the same three-part contract; each is a small adapter, not engine
work. Listed as a roadmap, not as shipped support.

| Target | Kind | Integration shape |
| --- | --- | --- |
| ZCode (z.ai) | agent dev environment: CLI, TUI, web, desktop, bot channels | thin client calling `/submit`; a bot-channel receiver for callbacks |
| OpenClaw | autonomous agent platform with a plugin runtime | plugin wrapping `/submit` plus a callback handler |
| Claw Orchestrator | runtime supervising long-lived CLI agent sessions | MCP tool or ACP adapter delegating to `/submit` |
| ACP clients (Zed, JetBrains, Neovim, Emacs) | editor-integrated agents | ACP adapter: prompt → `/submit`, callback → session update |
| n8n and similar workflow engines | automation | HTTP Request node → `/submit`; Webhook node ← signed callback |
| CI (GitHub Actions, Woodpecker, …) | pipelines | a step that `POST`s `/submit` and polls `/status/<id>` |

## Security

- **Local by default.** The server binds `127.0.0.1`; it has no authentication of
  its own, so do not expose it to a network without a proxy that adds one.
- **Signed notifications.** Every outbound notification carries an HMAC-SHA256
  signature over the raw body, keyed by `PI_BRIDGE_NOTIFY_SECRET`. Verify it
  before trusting a payload — the scheme is in
  [`docs/PROTOCOL.md`](docs/PROTOCOL.md).
- **No secret at rest.** The bridge stores only task prompts, logs and status.
  Secrets live in the environment, never in `state.db`.
- **Pi runs unsandboxed.** Tasks execute with the bridge's own privileges. Run
  the service as a user whose access you are willing to grant a coding agent,
  and use Pi's own permission configuration to narrow it further.

## Development

```sh
pip install -e '.[dev]'
python -m pytest -q
```

## Contributing

Issues and pull requests are welcome. Keep the engine dependency-free: if a
change needs a library, it probably belongs in an integration instead.

## License

MIT — see [LICENSE](LICENSE).
