/**
 * Hermes callback extension for Pi.
 *
 * Fires once, when the run is truly finished (`agent_settled`), and POSTs a
 * JSON callback to the orchestrator. This is what removes the need for Hermes
 * to hold a blocking listen on Pi for hours.
 *
 * Config via environment variables set by Hermes when it spawns Pi:
 *   HERMES_CALLBACK_URL  e.g. http://127.0.0.1:8799/callback
 *   HERMES_TASK_ID       opaque id Hermes uses to resume the task
 *
 * IMPORTANT: the reported status is derived from the run's actual stop reason.
 * Hardcoding "done" here would report a failed run as a success - and because a
 * failed run's assistant message is usually empty, the orchestrator would see
 * "done" with no output and have nothing to act on.
 */
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

export default function (pi: ExtensionAPI) {
	const url = process.env.HERMES_CALLBACK_URL;
	if (!url) return; // no orchestrator configured -> no-op

	const started = Date.now();
	let settled = false;

	const sendNotification = async (
		status: "done" | "failed",
		stopReason: string | null,
		errorMessage: string | null,
		text: string,
		usage: unknown,
		sessionId: string | null,
	) => {
		if (settled) return;
		settled = true;

		const payload = {
			task_id: process.env.HERMES_TASK_ID ?? null,
			status,
			stop_reason: stopReason,
			error: errorMessage,
			session_id: sessionId,
			duration_s: Math.round((Date.now() - started) / 1000),
			text: (text || errorMessage || "").slice(0, 4000),
			usage,
		};

		try {
			await fetch(url, {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify(payload),
				signal: AbortSignal.timeout(10_000),
			});
		} catch (e) {
			console.error("[hermes-callback] POST failed:", String(e));
		}
	};

	// `agent_settled` is the correct hook: `agent_end` may still be followed by
	// automatic retry, compaction, or queued follow-ups. Notify only when Pi
	// will genuinely not continue on its own.
	pi.on("agent_settled", async (_event, ctx) => {
		// Pull the last assistant message out of the session, best-effort.
		let text = "";
		let usage: unknown = null;
		let stopReason: string | null = null;
		let errorMessage: string | null = null;
		try {
			const branch = ctx.sessionManager.getBranch();
			for (let i = branch.length - 1; i >= 0; i--) {
				const e: any = branch[i];
				const msg = e?.message;
				if (msg?.role === "assistant") {
					const parts = Array.isArray(msg.content) ? msg.content : [];
					text = parts.filter((p: any) => p.type === "text").map((p: any) => p.text).join("");
					usage = msg.usage ?? null;
					stopReason = msg.stopReason ?? null;
					errorMessage = msg.errorMessage ?? null;
					break;
				}
			}
		} catch { /* never let the callback break the run */ }

		// A run that ended on `error` or `aborted` is a failure, not a success.
		// Anything else that is not a clean `stop` is suspicious: report it as
		// failed rather than claiming work that never completed.
		let status: "done" | "failed";
		if (stopReason === "stop") {
			status = "done";
		} else if (stopReason === "error" || stopReason === "aborted") {
			status = "failed";
		} else {
			status = stopReason === null ? "done" : "failed";
		}

		await sendNotification(
			status,
			stopReason,
			errorMessage,
			text,
			usage,
			(ctx as any).sessionId ?? null,
		);
	});

	pi.on("session_shutdown", async (_event, ctx) => {
		if (!settled) {
			await sendNotification(
				"failed",
				"shutdown",
				"session shutdown before agent settled",
				"",
				null,
				(ctx as any).sessionId ?? null,
			);
		}
	});

	process.on("beforeExit", async () => {
		if (!settled) {
			await sendNotification(
				"failed",
				"process_exit",
				"process exited before agent settled",
				"",
				null,
				null,
			);
		}
	});
}
