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

## Install

Requires Python 3.10+ and the `pi` CLI on `PATH`.

```sh
git clone <this repo> && cd pi-bridge
pip install -e .          # or just run ./pi_bridge.py directly
```

Pi's own config is untouched; see `PI_BRIDGE_PASSTHROUGH_PROVIDERS` below for
gateway-backed setups that need arbitrary model ids.

## Run

```sh
pi-bridge serve           # HTTP API + watchdog + heartbeat
```

Minimal systemd user unit in `integrations/hermes/pi-bridge.service`.

## Use

```sh
pi-bridge submit "add a /healthz endpoint and a test" --cwd /path/to/repo
# 3f9a1c2e8b7d4a06

pi-bridge status 3f9a1c2e8b7d4a06
pi-bridge probe  3f9a1c2e8b7d4a06     # heartbeat classification, without waiting
pi-bridge list
```

Over HTTP:

```sh
curl -s localhost:8799/submit -d '{"prompt":"...","cwd":"/repo"}'
curl -s localhost:8799/status/3f9a1c2e8b7d4a06
curl -s localhost:8799/list
```

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
| `PI_BRIDGE_TIMEOUT` | `7200` | default task timeout, seconds |
| `PI_BRIDGE_WATCHDOG_INTERVAL` | `30` | seconds between watchdog ticks |
| `PI_BRIDGE_MODEL` | *(unset)* | force the model Pi runs |
| `PI_BRIDGE_FALLBACK_MODEL` | `hermes` | model when nothing else resolves |
| `PI_BRIDGE_MODEL_ALIASES` | `hermes,main-agent,…` | routing aliases never used as model ids |
| `PI_BRIDGE_PASSTHROUGH_PROVIDERS` | `local` | providers allowed undeclared model ids |
| `PI_BRIDGE_ORCHESTRATOR_DB` | *(unset)* | orchestrator SQLite db, to inherit its live model |
| `PI_BRIDGE_ORCHESTRATOR_CONFIG` | *(unset)* | orchestrator YAML, for its default model |
| `PI_BRIDGE_DIR` | *(unset)* | legacy alias for `PI_BRIDGE_STATE_DIR` |
| `PI_BIN` | `pi` | pi executable |
| `PI_CODING_AGENT_SESSION_DIR` | `~/.pi/agent/sessions` | where Pi keeps session jsonl |

Model resolution order: explicit `--model` → `PI_BRIDGE_MODEL` → the
orchestrator's live model → its configured default → `PI_BRIDGE_FALLBACK_MODEL`.
Internal routing aliases are skipped whenever a concrete model is available,
because pinning Pi to an alias silently pins it to whatever the gateway default
happens to be.

## Protocol

See [`docs/PROTOCOL.md`](docs/PROTOCOL.md) for the callback and heartbeat
contracts, the signature scheme, and the task lifecycle.

## Hermes integration

`integrations/hermes/` wires this bridge into
[Hermes Agent](https://hermes-agent.nousresearch.com/docs): a plugin exposing
`/pi_*` slash commands, a Pi extension that calls back on settle, a webhook
route example, and a systemd unit. None of it is required by the engine.

## License

MIT — see [LICENSE](LICENSE).
