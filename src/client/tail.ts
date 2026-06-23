import type { TailEvent } from "../protocol/sse.js";

const DEFAULT_BASE_URL = "http://127.0.0.1:5959";

export interface RunTailOptions {
	baseUrl?: string;
}

export interface ParseSseChunksResult {
	events: TailEvent[];
	rest: string;
}

export function parseSseChunks(buffer: string): ParseSseChunksResult {
	const events: TailEvent[] = [];
	let cursor = 0;
	while (true) {
		const boundary = buffer.indexOf("\n\n", cursor);
		if (boundary === -1) break;
		const block = buffer.slice(cursor, boundary);
		cursor = boundary + 2;
		for (const line of block.split("\n")) {
			if (line.startsWith(":")) continue;
			if (!line.startsWith("data:")) continue;
			const payload = line.slice(5).trimStart();
			if (!payload) continue;
			try {
				events.push(JSON.parse(payload) as TailEvent);
			} catch {
				// ill-formed payload silently dropped; broker emits valid JSON only
			}
		}
	}
	return { events, rest: buffer.slice(cursor) };
}

export async function runTail(
	peerId: string,
	opts: RunTailOptions = {},
): Promise<void> {
	const baseUrl = opts.baseUrl ?? DEFAULT_BASE_URL;
	const url = `${baseUrl}/tail/${encodeURIComponent(peerId)}`;

	const controller = new AbortController();
	const onSigint = (): void => controller.abort();
	process.once("SIGINT", onSigint);

	// `head -1` 같은 downstream pipe 가 닫혔을 때 EPIPE 로 process crash 방지
	process.stdout.on("error", (e: NodeJS.ErrnoException) => {
		if (e.code === "EPIPE") process.exit(0);
	});

	let response: Response;
	try {
		response = await fetch(url, {
			signal: controller.signal,
			headers: { Accept: "text/event-stream" },
		});
	} catch (e) {
		if (controller.signal.aborted) return;
		const msg = e instanceof Error ? e.message : String(e);
		process.stderr.write(`tail: connection failed: ${msg}\n`);
		process.exit(1);
	}

	if (response.status !== 200 || !response.body) {
		const text = await response.text().catch(() => "");
		process.stderr.write(
			`tail: server returned ${response.status}${text ? `: ${text}` : ""}\n`,
		);
		process.exit(1);
	}

	const decoder = new TextDecoder();
	const reader = response.body.getReader();
	let buffer = "";
	try {
		while (true) {
			const { value, done } = await reader.read();
			if (done) break;
			buffer += decoder.decode(value, { stream: true });
			const { events, rest } = parseSseChunks(buffer);
			buffer = rest;
			for (const event of events) {
				process.stdout.write(`${JSON.stringify(event)}\n`);
			}
		}
	} catch (e) {
		if (controller.signal.aborted) return;
		const msg = e instanceof Error ? e.message : String(e);
		process.stderr.write(`tail: stream error: ${msg}\n`);
		process.exit(1);
	} finally {
		process.removeListener("SIGINT", onSigint);
	}
}
