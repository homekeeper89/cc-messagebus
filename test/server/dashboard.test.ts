import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	after,
	afterEach,
	before,
	beforeEach,
	describe,
	test,
} from "node:test";
import { parseSseChunks } from "../../src/client/tail.js";
import { createServer, type Server } from "../../src/server/index.js";

describe("dashboard html", () => {
	let tmpDir: string;
	let dbPath: string;
	let server: Server;

	before(() => {
		tmpDir = mkdtempSync(join(tmpdir(), "ccmb-dash-html-"));
		dbPath = join(tmpDir, "data.db");
	});

	after(async () => {
		await server?.app.close();
		server?.db.close();
		rmSync(tmpDir, { recursive: true, force: true });
	});

	beforeEach(async () => {
		if (server) {
			await server.app.close();
			server.db.close();
		}
		rmSync(dbPath, { force: true });
		server = createServer({ dbPath });
		await server.app.ready();
	});

	test("GET /dashboard returns 200 + text/html", async () => {
		const res = await server.app.inject({ method: "GET", url: "/dashboard" });
		assert.equal(res.statusCode, 200);
		assert.match(res.headers["content-type"] as string, /text\/html/);
		assert.match(res.body.trim(), /^<!doctype html>/i);
	});

	test("dashboard html wires EventSource('/events')", async () => {
		const res = await server.app.inject({ method: "GET", url: "/dashboard" });
		assert.ok(res.body.includes('new EventSource("/events")'));
	});
});

describe("events SSE", () => {
	let tmpDir: string;
	let dbPath: string;
	let server: Server;
	let baseUrl: string;

	before(() => {
		tmpDir = mkdtempSync(join(tmpdir(), "ccmb-events-"));
		dbPath = join(tmpDir, "data.db");
	});

	after(() => {
		rmSync(tmpDir, { recursive: true, force: true });
	});

	beforeEach(async () => {
		rmSync(dbPath, { force: true });
		server = createServer({
			dbPath,
			host: "127.0.0.1",
			port: 0,
		});
		baseUrl = await server.start();
	});

	afterEach(async () => {
		await server.stop();
	});

	async function readEvent(
		reader: ReadableStreamDefaultReader<Uint8Array>,
		decoder: TextDecoder,
		buffer: { value: string },
		predicate: (event: { type: string }) => boolean,
	): Promise<{ type: string; [k: string]: unknown }> {
		while (true) {
			const { value, done } = await reader.read();
			if (done) throw new Error("stream ended before predicate matched");
			buffer.value += decoder.decode(value, { stream: true });
			const { events, rest } = parseSseChunks(buffer.value);
			buffer.value = rest;
			for (const e of events) {
				const evt = e as { type: string };
				if (predicate(evt)) return evt as { type: string };
			}
		}
	}

	test("GET /events emits session_snapshot + heartbeat on connect", async () => {
		const controller = new AbortController();
		try {
			const res = await fetch(`${baseUrl}/events`, {
				signal: controller.signal,
				headers: { Accept: "text/event-stream" },
			});
			assert.equal(res.status, 200);
			assert.ok(res.body, "expected response body stream");
			const reader = res.body.getReader();
			const decoder = new TextDecoder();
			const buf = { value: "" };

			const snap = await readEvent(
				reader,
				decoder,
				buf,
				(e) => e.type === "session_snapshot",
			);
			assert.deepEqual((snap as { peers: unknown[] }).peers, []);

			const hb = await readEvent(
				reader,
				decoder,
				buf,
				(e) => e.type === "heartbeat",
			);
			assert.equal(hb.type, "heartbeat");
		} finally {
			controller.abort();
		}
	});

	test("GET /events pushes session_registered when peer registers", async () => {
		const controller = new AbortController();
		try {
			const res = await fetch(`${baseUrl}/events`, {
				signal: controller.signal,
				headers: { Accept: "text/event-stream" },
			});
			assert.ok(res.body);
			const reader = res.body.getReader();
			const decoder = new TextDecoder();
			const buf = { value: "" };

			await readEvent(reader, decoder, buf, (e) => e.type === "heartbeat");

			await fetch(`${baseUrl}/api/register`, {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({ topicId: "alice" }),
			});

			const evt = await readEvent(
				reader,
				decoder,
				buf,
				(e) => e.type === "session_registered",
			);
			const peer = (evt as { peer: { topicId: string } }).peer;
			assert.equal(peer.topicId, "alice");
		} finally {
			controller.abort();
		}
	});

	test("GET /events streams message lifecycle (sent → read → acked)", async () => {
		await fetch(`${baseUrl}/api/register`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ topicId: "alice" }),
		});
		await fetch(`${baseUrl}/api/register`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ topicId: "bob" }),
		});

		const controller = new AbortController();
		try {
			const res = await fetch(`${baseUrl}/events`, {
				signal: controller.signal,
				headers: { Accept: "text/event-stream" },
			});
			assert.ok(res.body);
			const reader = res.body.getReader();
			const decoder = new TextDecoder();
			const buf = { value: "" };

			await readEvent(reader, decoder, buf, (e) => e.type === "heartbeat");

			const sendRes = await fetch(`${baseUrl}/api/send`, {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({
					from: "alice",
					to: "bob",
					subject: "ping",
					body: "hi",
				}),
			});
			const sendBody = (await sendRes.json()) as { messageId: string };
			const messageId = sendBody.messageId;

			const sent = await readEvent(
				reader,
				decoder,
				buf,
				(e) => e.type === "message_sent",
			);
			const msg = (sent as { message: { to: string; subject: string } })
				.message;
			assert.equal(msg.to, "bob");
			assert.equal(msg.subject, "ping");

			await fetch(`${baseUrl}/api/read`, {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({ topicId: "bob" }),
			});

			const readEvt = await readEvent(
				reader,
				decoder,
				buf,
				(e) => e.type === "message_read",
			);
			assert.equal((readEvt as { messageId: string }).messageId, messageId);

			await fetch(`${baseUrl}/api/ack`, {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({ topicId: "bob", messageId }),
			});

			const acked = await readEvent(
				reader,
				decoder,
				buf,
				(e) => e.type === "message_acked",
			);
			assert.equal((acked as { messageId: string }).messageId, messageId);
		} finally {
			controller.abort();
		}
	});

	test("late connect snapshot includes existing peers", async () => {
		await fetch(`${baseUrl}/api/register`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ topicId: "alice" }),
		});

		const controller = new AbortController();
		try {
			const res = await fetch(`${baseUrl}/events`, {
				signal: controller.signal,
				headers: { Accept: "text/event-stream" },
			});
			assert.ok(res.body);
			const reader = res.body.getReader();
			const decoder = new TextDecoder();
			const buf = { value: "" };

			const snap = await readEvent(
				reader,
				decoder,
				buf,
				(e) => e.type === "session_snapshot",
			);
			const peers = (snap as { peers: { topicId: string }[] }).peers;
			assert.equal(peers.length, 1);
			assert.equal(peers[0].topicId, "alice");
		} finally {
			controller.abort();
		}
	});
});
