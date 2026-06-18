import type { Broker } from "./broker.js";
import type { CcDatabase } from "./db.js";

const MS_PER_SEC = 1000;

export interface CleanupOptions {
	intervalSec: number;
}

export interface CleanupHandle {
	stop: () => void;
}

export function startCleanup(
	broker: Broker,
	db: CcDatabase,
	opts: CleanupOptions,
): CleanupHandle {
	let stopped = false;

	function tick(): void {
		if (stopped) return;
		try {
			const now = new Date().toISOString();
			const expired = db.expireInFlight(now);
			for (const messageId of expired) {
				broker.events.emit("message_redelivered", {
					type: "message_redelivered",
					messageId,
					at: now,
				});
			}
			const deleted = db.deleteExpired(now);
			for (const messageId of deleted) {
				broker.events.emit("message_expired", {
					type: "message_expired",
					messageId,
					at: now,
				});
			}
		} catch (e) {
			// daemon 가용성 보호: tick 실패가 process crash 로 이어지지 않도록 격리
			broker.events.emit("cleanup_error", { error: e });
		}
	}

	setImmediate(tick);
	const timer = setInterval(tick, opts.intervalSec * MS_PER_SEC);
	timer.unref();

	return {
		stop: (): void => {
			stopped = true;
			clearInterval(timer);
		},
	};
}
