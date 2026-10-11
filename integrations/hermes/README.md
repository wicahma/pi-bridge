# Hermes Agent integration

Wires [pi-bridge](../../README.md) into
[Hermes Agent](https://hermes-agent.nousresearch.com/docs). Nothing here is
required by the engine — it is one worked example of an orchestrator that
receives the bridge's push notifications and renders them in a chat.

Three pieces, three runtimes:

| File | Runtime | Role |
| --- | --- | --- |
| `plugin/` | Hermes | `/pi_*` slash commands: submit, status, list, model |
| `hermes-callback.ts` | Pi (extension) | POST Pi's own result to the bridge as it exits |
| `webhook_subscriptions.example.json` | Hermes | two webhook routes: `pi-done` (completion), `pi-status` (heartbeat) |
| `pi-bridge.service` | systemd (user) | runs `pi-bridge serve` with the env below |

## 1. Engine

```sh
install -Dm644 pi-bridge.service ~/.config/systemd/user/pi-bridge.service
systemctl --user daemon-reload && systemctl --user enable --now pi-bridge
```

The unit needs these set for the integration to work — every one is optional to
the engine, but the plugin and the webhook routes assume them:

```
PI_BRIDGE_NOTIFY_URL        http://127.0.0.1:8644/webhooks/pi-done
PI_BRIDGE_HEARTBEAT_URL     http://127.0.0.1:8644/webhooks/pi-status
PI_BRIDGE_NOTIFY_SECRET     <same secret as the pi-done route>
PI_BRIDGE_ORCHESTRATOR_DB   ~/.hermes/state.db
PI_BRIDGE_ORCHESTRATOR_CONFIG ~/.hermes/config.yaml
```

`PI_BRIDGE_ORCHESTRATOR_DB` is what lets a submitted task inherit the model the
current Hermes session is running, and what attaches `chat_id` / `thread_id` to
notifications. Point it elsewhere and both simply stop happening.

## 2. Plugin

```sh
mkdir -p ~/.hermes/plugins/pi-bridge
cp plugin/__init__.py plugin/plugin.yaml ~/.hermes/plugins/pi-bridge/
```

Adds `/pi_submit`, `/pi_status`, `/pi_list`, `/pi_model`. The plugin is a thin
HTTP client — it holds no task state of its own.

## 3. Pi extension

```sh
cp hermes-callback.ts ~/.pi/agent/extensions/
```

Reads `PI_BRIDGE_CALLBACK_URL` and `PI_BRIDGE_TASK_ID` from the environment the
bridge injects into each spawned task, and POSTs the settled result back.

## 4. Webhook routes

Copy `webhook_subscriptions.example.json` into `~/.hermes/webhook_subscriptions.json`
and replace the two `REPLACE_WITH_YOUR_WEBHOOK_SECRET` values with the same
secret as `PI_BRIDGE_NOTIFY_SECRET`.

The routes differ in one important way:

- **`pi-done`** delivers *and* mirrors into the originating session, so a
  finished task wakes the orchestrator agent and it can act on the result.
- **`pi-status`** is `deliver_only: true` — the template becomes the message
  with no agent run and no LLM call. A heartbeat every 60s therefore costs one
  HTTP POST, not one model turn per minute.

That distinction is the whole reason the heartbeat is affordable. Mirroring
status into the session would spawn an agent turn every minute per running task.

## Ordering

The engine does not depend on Hermes. If Hermes is down, the bridge keeps
settling tasks and its notification attempts fail silently — the watchdog and
session-file paths still record every outcome in `state.db`, so nothing is lost.
