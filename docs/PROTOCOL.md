# Protocol

Two directions: the orchestrator drives the bridge over a small HTTP API, and
the bridge pushes signed notifications back out. Nothing in the push direction
is required — leave the URLs unset and the bridge stays silent.

## Signature

Both notification kinds use the same scheme, so any receiver that verifies
GitHub-style webhooks works unchanged.

```
X-Hub-Signature-256: sha256=<hex hmac-sha256 of the raw body, keyed by PI_BRIDGE_NOTIFY_SECRET>
X-GitHub-Event:      <pi-task | pi-status>
X-GitHub-Delivery:   <unique id for this POST>
```

Verify against the **raw bytes** of the body, before any JSON parsing. Compare in
constant time.

`X-GitHub-Delivery` must be unique per POST or an idempotency cache will drop
repeat deliveries. Completion notifications use the task id; heartbeats use
`<task_id>-hb-<epoch>`, so successive ticks of the same task are distinct.

A notification failure is logged and dropped. It never changes a task's state.

## Direction 1 — orchestrator → bridge

| Method | Path | Body | Returns |
| --- | --- | --- | --- |
| `POST` | `/submit` | `prompt`, `cwd`, `model?`, `provider?`, `timeout?`, `session_id?`, `allow_alias?`, `context?` | `{"ok":true,"task_id":"…"}` |
| `GET` | `/status/<task_id>` | — | `{"ok":true,"task":{…}}` (prompt omitted) |
| `GET` | `/list` | — | `{"ok":true,"tasks":[…]}` |
| `POST` | `/model` | `explicit?`, `session_id?`, `allow_alias?` | `{"ok":true,"model":"…","source":"…"}` |
| `POST` | `<callback_path>` | Pi's own settle callback | `{"ok":true,"accepted":bool}` |

`/submit` returns as soon as the Pi process is spawned. It never waits for the
run. The same operations are available as CLI subcommands (`submit`, `status`,
`list`, `model`, `probe`, `reconcile`, `watchdog`, `serve`).

### `context`

An arbitrary JSON object, opaque to the bridge. It is stored with the task and
merged into the top level of every notification for that task, which is how a
receiver routes a message without the bridge knowing anything about routing:

```json
{"prompt": "fix the flaky test", "cwd": "/repo",
 "context": {"chat_id": "…", "thread_id": "…", "run_id": "nightly-42"}}
```

Keys are echoed verbatim; `null` values are dropped.

## Direction 2 — bridge → orchestrator

### Completion — `X-GitHub-Event: pi-task`

Posted once, when a task first reaches a terminal state. Sent on a daemon
thread, so it cannot delay the task or any other.

```json
{
  "task_id": "3f9a1c2e8b7d4a06",
  "status": "done",
  "cwd": "/repo",
  "model": "…",
  "resolution": "callback",
  "resolved_by": "callback",
  "result_text": "…last 4000 chars of Pi's answer…",
  "chat_id": "…",
  "thread_id": "…"
}
```

`status` ∈ `done | failed | timeout | lost`.
`resolution` ∈ `callback | session-file | watchdog | exit | reconcile`, and
`resolved_by` names whichever fired first — useful for telling a clean settle
from a recovery path.

`result_text` is prefixed `__ERROR__` when the outcome came from an error
signature rather than Pi's own final message.

### Heartbeat — `X-GitHub-Event: pi-status`

Posted every `PI_BRIDGE_HEARTBEAT_INTERVAL` seconds for each running task.

```json
{
  "task_id": "3f9a1c2e8b7d4a06",
  "state": "working",
  "elapsed": "12m04s",
  "iteration": 7,
  "last_tool": "read",
  "detail": "now updating the test fixtures",
  "cwd": "/repo",
  "model": "…",
  "delivery_id": "3f9a1c2e8b7d4a06-hb-1776000000",
  "chat_id": "…",
  "thread_id": "…"
}
```

`state` is classified from the task's process and Pi's session file:

| State | Meaning |
| --- | --- |
| `working` | process alive, session file written to within `3 × interval` |
| `idle` | process alive, but no session activity for longer than that |
| `error` | stale or missing session activity **and** the log carries a real error |
| `dne` | the recorded pid is gone |

The liveness signal is Pi's session jsonl, not stdout: Pi writes almost nothing
to stdout while it works. A non-empty log is *not* treated as an error — only a
line starting with `Error`, `ERROR`, `Exception`, `Traceback` or `Fatal` is.
This matters: an unanchored match flags a successful run whose final prose
merely quotes the word "Failed".

## Lifecycle

```
submit ─▶ running ─┬─▶ done      clean settle
                   ├─▶ failed    non-zero exit, or an error signature
                   ├─▶ timeout   deadline passed
                   └─▶ lost      no settled result anywhere
```

Four independent paths can settle a task, and the first to land wins:

1. **callback** — Pi POSTs its own result as it exits.
2. **session-file** — the outcome is read from Pi's session jsonl.
3. **watchdog** — a background tick notices the process is gone.
4. **reconcile** — a startup pass re-derives tasks left in flight by a restart.

Because more than one path can fire, every settle is idempotent: once a task is
terminal, later resolutions are ignored.

The watchdog exists because the callback is best-effort. If Pi is killed, if the
callback is dropped, or if the bridge restarted mid-run, the task still settles
correctly rather than hanging forever.
