import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { runTail } from "../../src/client/tail.js";

interface RecordedCall {
	url: string;
	body: unknown;
}

interface FetchMock {
	fetchFn: typeof fetch;
	calls: RecordedCall[];
}

type Handler = (call: RecordedCall) => Response | Promise<Response>;

function makeFetchMock(handlers: Handler[]): FetchMock {
	const calls: RecordedCall[] = [];
	let i = 0;
	const fetchFn: typeof fetch = async (input, init) => {
		const url =
			typeof input === "string"
				? input
				: input instanceof URL
					? input.toString()
					: input.url;
		const body = init?.body ? JSON.parse(String(init.body)) : null;
		const call: RecordedCall = { url, body };
		calls.push(call);
		if (i >= handlers.length) {
			throw new Error(`unexpected fetch #${calls.length} to ${url}`);
		}
		const handler = handlers[i++];
		if (!handler) throw new Error("handler missing");
		return handler(call);
	};
	return { fetchFn, calls };
}

function jsonResp(body: unknown, status = 200): Response {
	return new Response(JSON.stringify(body), {
		status,
		headers: { "content-type": "application/json" },
	});
}

function fakeMessage(id: string, overrides: Record<string, unknown> = {}) {
	return {
		id,
		from: "alice",
		to: "bob",
		subject: "hi",
		body: "yo",
		threadId: null,
		sentAt: "2026-06-26T00:00:00Z",
		inFlightUntil: "2026-06-26T00:00:30Z",
		ackedAt: null,
		expiresAt: "2026-07-26T00:00:00Z",
		...overrides,
	};
}

describe("runTail (polling)", () => {
	test("posts /api/read, writes each message line, acks each in order", async () => {
		const ctrl = new AbortController();
		const stdout: string[] = [];
		const m1 = fakeMessage("m1");
		const m2 = fakeMessage("m2", { subject: "second" });

		const { fetchFn, calls } = makeFetchMock([
			() => jsonResp({ ok: true, messages: [m1, m2] }),
			() => jsonResp({ ok: true, ackedAt: "ack-1" }),
			() => jsonResp({ ok: true, ackedAt: "ack-2" }),
			() => {
				ctrl.abort();
				return jsonResp({ ok: true, messages: [] });
			},
		]);

		await runTail("bob", {
			baseUrl: "http://test",
			intervalMs: 1,
			fetchFn,
			stdoutWrite: (s) => stdout.push(s),
			stderrWrite: () => {},
			sleep: async () => {},
			signal: ctrl.signal,
		});

		assert.equal(calls.length, 4, "1 read + 2 ack + 1 final read");
		assert.equal(calls[0]?.url, "http://test/api/read");
		assert.deepEqual(calls[0]?.body, { peerId: "bob", max: 50 });
		assert.equal(calls[1]?.url, "http://test/api/ack");
		assert.deepEqual(calls[1]?.body, { peerId: "bob", messageId: "m1" });
		assert.equal(calls[2]?.url, "http://test/api/ack");
		assert.deepEqual(calls[2]?.body, { peerId: "bob", messageId: "m2" });
		assert.equal(calls[3]?.url, "http://test/api/read");

		assert.equal(stdout.length, 2);
		assert.deepEqual(JSON.parse(stdout[0] ?? ""), m1);
		assert.deepEqual(JSON.parse(stdout[1] ?? ""), m2);
		assert.ok(stdout[0]?.endsWith("\n"), "stdout line terminated by newline");
	});

	test("empty read sleeps for intervalMs and re-polls until aborted", async () => {
		const ctrl = new AbortController();
		const sleepCalls: number[] = [];
		const stdout: string[] = [];

		const { fetchFn, calls } = makeFetchMock([
			() => jsonResp({ ok: true, messages: [] }),
			() => jsonResp({ ok: true, messages: [] }),
		]);

		const sleep = async (ms: number): Promise<void> => {
			sleepCalls.push(ms);
			if (sleepCalls.length >= 2) ctrl.abort();
		};

		await runTail("bob", {
			baseUrl: "http://test",
			intervalMs: 42,
			fetchFn,
			stdoutWrite: (s) => stdout.push(s),
			stderrWrite: () => {},
			sleep,
			signal: ctrl.signal,
		});

		assert.equal(calls.length, 2);
		assert.deepEqual(sleepCalls, [42, 42]);
		assert.equal(stdout.length, 0);
	});

	test("read returning error envelope writes stderr and throws", async () => {
		const ctrl = new AbortController();
		const stderr: string[] = [];

		const { fetchFn } = makeFetchMock([
			() =>
				jsonResp(
					{
						ok: false,
						error: {
							code: "PEER_NOT_FOUND",
							message: "peer 'bob' is not registered",
						},
					},
					404,
				),
		]);

		await assert.rejects(
			runTail("bob", {
				baseUrl: "http://test",
				intervalMs: 1,
				fetchFn,
				stdoutWrite: () => {},
				stderrWrite: (s) => stderr.push(s),
				sleep: async () => {},
				signal: ctrl.signal,
			}),
			/PEER_NOT_FOUND/,
		);

		assert.equal(stderr.length, 1);
		assert.match(stderr[0] ?? "", /read failed.*PEER_NOT_FOUND/);
	});

	test("ack failure logs to stderr but polling continues", async () => {
		const ctrl = new AbortController();
		const stdout: string[] = [];
		const stderr: string[] = [];
		const m1 = fakeMessage("m1");

		const { fetchFn, calls } = makeFetchMock([
			() => jsonResp({ ok: true, messages: [m1] }),
			() =>
				jsonResp(
					{
						ok: false,
						error: {
							code: "MESSAGE_NOT_IN_FLIGHT",
							message: "already acked",
						},
					},
					409,
				),
			() => {
				ctrl.abort();
				return jsonResp({ ok: true, messages: [] });
			},
		]);

		await runTail("bob", {
			baseUrl: "http://test",
			intervalMs: 1,
			fetchFn,
			stdoutWrite: (s) => stdout.push(s),
			stderrWrite: (s) => stderr.push(s),
			sleep: async () => {},
			signal: ctrl.signal,
		});

		assert.equal(calls.length, 3, "read + ack(fail) + next read");
		assert.equal(stdout.length, 1, "message still printed");
		assert.equal(stderr.length, 1);
		assert.match(stderr[0] ?? "", /ack failed for m1.*MESSAGE_NOT_IN_FLIGHT/);
	});

	test("signal pre-aborted skips polling entirely", async () => {
		const ctrl = new AbortController();
		ctrl.abort();
		const stdout: string[] = [];
		const { fetchFn, calls } = makeFetchMock([]);

		await runTail("bob", {
			baseUrl: "http://test",
			intervalMs: 1,
			fetchFn,
			stdoutWrite: (s) => stdout.push(s),
			stderrWrite: () => {},
			sleep: async () => {},
			signal: ctrl.signal,
		});

		assert.equal(calls.length, 0);
		assert.equal(stdout.length, 0);
	});
});
