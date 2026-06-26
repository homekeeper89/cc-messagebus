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

export interface RunTailOptions {
	baseUrl?: string;
	intervalMs?: number;
	// 모두 테스트 주입용. production 은 default 사용.
	fetchFn?: typeof fetch;
	stdoutWrite?: (chunk: string) => void;
	stderrWrite?: (chunk: string) => void;
	sleep?: (ms: number) => Promise<void>;
	signal?: AbortSignal;
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

	// `head -1` 같은 downstream pipe 가 닫혔을 때 EPIPE 로 process crash 방지
	if (!opts.stdoutWrite) {
		process.stdout.on("error", (e: NodeJS.ErrnoException) => {
			if (e.code === "EPIPE") process.exit(0);
		});
	}

	const readUrl = `${baseUrl}${HTTP_ENDPOINTS.read.path}`;
	const ackUrl = `${baseUrl}${HTTP_ENDPOINTS.ack.path}`;

	while (!signal.aborted) {
		const readReq: ReadRequest = { peerId, max: READ_MAX };
		let readEnv: ApiResponse<ReadResponse>;
		try {
			readEnv = await postJson<ReadRequest, ReadResponse>(
				fetchFn,
				readUrl,
				readReq,
				signal,
			);
		} catch (e) {
			if (signal.aborted) return;
			throw e;
		}

		if (!readEnv.ok) {
			stderrWrite(
				`tail: read failed (${readEnv.error.code}): ${readEnv.error.message}\n`,
			);
			throw new Error(`tail read failed: ${readEnv.error.code}`);
		}

		for (const msg of readEnv.messages) {
			stdoutWrite(`${JSON.stringify(msg)}\n`);
			const ackReq: AckRequest = { peerId, messageId: msg.id };
			let ackEnv: ApiResponse<AckResponse>;
			try {
				ackEnv = await postJson<AckRequest, AckResponse>(
					fetchFn,
					ackUrl,
					ackReq,
					signal,
				);
			} catch (e) {
				if (signal.aborted) return;
				throw e;
			}
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
