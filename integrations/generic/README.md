# Generic HTTP + HMAC integration

A minimal orchestrator for [pi-bridge](../../README.md) — one file, standard
library only, no dependencies. It submits a task and then verifies the bridge's
signed callback. Nothing here is required by the engine; it is the smallest
worked example of the three-part contract, and the proof that the contract is
not tied to any one orchestrator.

## Point a bridge at it

The notification URL is bridge-wide configuration, not a per-task field: the
bridge is *deployed* pointed at this process. Set both variables in the bridge's
environment and restart the service.

```
PI_BRIDGE_NOTIFY_URL        http://127.0.0.1:8901/hook
PI_BRIDGE_NOTIFY_SECRET     <shared secret>
```

The caller reads the same secret from `PI_BRIDGE_NOTIFY_SECRET`, so a single
value on both sides is enough.

## Run it

```sh
python3 integrations/generic/caller.py \
  --cwd /repo \
  --prompt "fix the flaky test"
```

`--listen`, `--path` and `--bridge` default to the values above;
`python3 integrations/generic/caller.py --help` lists every flag.

A successful run prints one line per step, prefixed `[caller] ` so it is easy to
grep:

```
[caller] listening on http://127.0.0.1:8901/hook
[caller] submitted task 3f9a1c2e8b7d4a06
[caller] event=pi-task
[caller] task_id=3f9a1c2e8b7d4a06
[caller] status=done
[caller] result_text=…
```

Exit codes: `0` a verified callback arrived, `2` no callback before `--wait`
elapsed (usually the bridge is not pointed at this address), `1` the submit
itself failed.

## The contract

Same three parts as any other integration: **submit** to `/submit`, **receive**
a POST whose body verifies against `X-Hub-Signature-256`, and optionally
**inherit** the orchestrator's state. See [docs/PROTOCOL.md](../../docs/PROTOCOL.md)
for the signature scheme and the completion and heartbeat payloads.

Unsigned or wrongly signed requests are answered `401` and dropped without being
parsed, so the body is only ever read after its signature checks out.
