import type { ApiResponse } from "../protocol/errors.js";
import {
	type AckRequest,
	type AckResponse,
	HTTP_ENDPOINTS,
	type ReadRequest,
	type ReadResponse,
} from "../protocol/http.js";

const DEFAULT_BASE_URL = "http://127.0.0.1:5959";
const DEFAULT_INTERVAL_MS = 5000;
const READ_MAX = 50;
const RECONNECT_BASE_MS = 3000;
const RECONNECT_MAX_MS = 30000;

export interface RunTailOptions {
	baseUrl?: string;
	intervalMs?: number;
	// 모두 테스트 주입용. production 은 default 사용.
	fetchFn?: typeof fetch;
	stdoutWrite?: (chunk: string) => void;
	stderrWrite?: (chunk: string) => void;
	sleep?: (ms: number) => Promise<void>;
	signal?: AbortSignal;
	reconnectBaseMs?: number;
	reconnectMaxMs?: number;
}

function defaultSleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function createSigintSignal(): AbortSignal {
	const ctrl = new AbortController();
	process.once("SIGINT", () => ctrl.abort());
	return ctrl.signal;
}

async function postJson<TReq, TRes>(
	fetchFn: typeof fetch,
	url: string,
	body: TReq,
	signal: AbortSignal,
): Promise<ApiResponse<TRes>> {
	const resp = await fetchFn(url, {
		method: "POST",
		headers: { "content-type": "application/json" },
		body: JSON.stringify(body),
		signal,
	});
	return (await resp.json()) as ApiResponse<TRes>;
}

export async function runTail(
	peerId: string,
	opts: RunTailOptions = {},
): Promise<void> {
	const baseUrl = opts.baseUrl ?? DEFAULT_BASE_URL;
	const intervalMs = opts.intervalMs ?? DEFAULT_INTERVAL_MS;
	const fetchFn = opts.fetchFn ?? fetch;
	const stdoutWrite =
		opts.stdoutWrite ?? ((s: string): void => void process.stdout.write(s));
	const stderrWrite =
		opts.stderrWrite ?? ((s: string): void => void process.stderr.write(s));
	const sleep = opts.sleep ?? defaultSleep;
	const signal = opts.signal ?? createSigintSignal();
	const reconnectBaseMs = opts.reconnectBaseMs ?? RECONNECT_BASE_MS;
	const reconnectMaxMs = opts.reconnectMaxMs ?? RECONNECT_MAX_MS;

	// `head -1` 같은 downstream pipe 가 닫혔을 때 EPIPE 로 process crash 방지
	if (!opts.stdoutWrite) {
		process.stdout.on("error", (e: NodeJS.ErrnoException) => {
			if (e.code === "EPIPE") process.exit(0);
		});
	}

	const readUrl = `${baseUrl}${HTTP_ENDPOINTS.read.path}`;
	const ackUrl = `${baseUrl}${HTTP_ENDPOINTS.ack.path}`;

	// MacBook 잠자기/wake 나 broker 재시작 이후 fetch 가 던지는 network error 는
	// backoff 후 재시도. envelope error (PEER_NOT_FOUND 등) 는 진짜 문제라 그대로 던진다.
	const postWithReconnect = async <TReq, TRes>(
		url: string,
		body: TReq,
	): Promise<ApiResponse<TRes> | null> => {
		let attempt = 0;
		while (!signal.aborted) {
			try {
				return await postJson<TReq, TRes>(fetchFn, url, body, signal);
			} catch (e) {
				if (signal.aborted) return null;
				attempt++;
				const wait = Math.min(reconnectBaseMs * attempt, reconnectMaxMs);
				const msg = e instanceof Error ? e.message : String(e);
				stderrWrite(
					`tail: broker unreachable, reconnecting in ${wait}ms (${msg})\n`,
				);
				await sleep(wait);
			}
		}
		return null;
	};

	while (!signal.aborted) {
		const readReq: ReadRequest = { peerId, max: READ_MAX };
		const readEnv = await postWithReconnect<ReadRequest, ReadResponse>(
			readUrl,
			readReq,
		);
		if (!readEnv) return;

		if (!readEnv.ok) {
			stderrWrite(
				`tail: read failed (${readEnv.error.code}): ${readEnv.error.message}\n`,
			);
			throw new Error(`tail read failed: ${readEnv.error.code}`);
		}

		for (const msg of readEnv.messages) {
			stdoutWrite(`${JSON.stringify(msg)}\n`);
			const ackReq: AckRequest = { peerId, messageId: msg.id };
			const ackEnv = await postWithReconnect<AckRequest, AckResponse>(
				ackUrl,
				ackReq,
			);
			if (!ackEnv) return;
			if (!ackEnv.ok) {
				// ack 실패는 polling 을 멈출 정도는 아님 — visibility timeout 으로 broker 가 재전달함
				stderrWrite(
					`tail: ack failed for ${msg.id} (${ackEnv.error.code}): ${ackEnv.error.message}\n`,
				);
			}
		}

		if (signal.aborted) return;
		await sleep(intervalMs);
	}
}
