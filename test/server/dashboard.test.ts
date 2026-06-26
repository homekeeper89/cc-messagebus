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
import { parseSseChunks } from "../../src/protocol/sse.js";
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
		assert.ok(res.body.includes('addEventListener("topic_created"'));
		assert.ok(res.body.includes('addEventListener("topic_subscribed"'));
		assert.ok(res.body.includes('addEventListener("topic_unsubscribed"'));
		assert.ok(res.body.includes('addEventListener("topic_message_published"'));
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

	test("GET /events emits session_snapshot on connect", async () => {
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
			assert.deepEqual((snap as { topics: unknown[] }).topics, []);
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

			await readEvent(
				reader,
				decoder,
				buf,
				(e) => e.type === "session_snapshot",
			);

			await fetch(`${baseUrl}/api/register`, {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({ peerId: "alice" }),
			});

			const evt = await readEvent(
				reader,
				decoder,
				buf,
				(e) => e.type === "session_registered",
			);
			const peer = (evt as { peer: { peerId: string } }).peer;
			assert.equal(peer.peerId, "alice");
		} finally {
			controller.abort();
		}
	});

	test("GET /events streams message lifecycle (sent → read → acked)", async () => {
		await fetch(`${baseUrl}/api/register`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ peerId: "alice" }),
		});
		await fetch(`${baseUrl}/api/register`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ peerId: "bob" }),
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

			await readEvent(
				reader,
				decoder,
				buf,
				(e) => e.type === "session_snapshot",
			);

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
			const sentEvent = sent as {
				message: { to: string; subject: string };
				kind: string;
				topicId?: string;
			};
			const msg = sentEvent.message;
			assert.equal(msg.to, "bob");
			assert.equal(msg.subject, "ping");
			assert.equal(
				sentEvent.kind,
				"dm",
				"1:1 send must emit message_sent with kind='dm' (PR-D)",
			);
			assert.equal(
				sentEvent.topicId,
				undefined,
				"DM message_sent must not carry topicId (PR-D)",
			);

			await fetch(`${baseUrl}/api/read`, {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({ peerId: "bob" }),
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
				body: JSON.stringify({ peerId: "bob", messageId }),
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

	test("GET /events streams topic lifecycle (create → subscribe → send → unsubscribe)", async () => {
		await fetch(`${baseUrl}/api/register`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ peerId: "alice" }),
		});
		await fetch(`${baseUrl}/api/register`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ peerId: "bob" }),
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

			await readEvent(
				reader,
				decoder,
				buf,
				(e) => e.type === "session_snapshot",
			);

			await fetch(`${baseUrl}/api/topic_create`, {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({ topicId: "#general", createdBy: "alice" }),
			});

			const created = await readEvent(
				reader,
				decoder,
				buf,
				(e) => e.type === "topic_created",
			);
			const topic = (created as { topic: { topicId: string } }).topic;
			assert.equal(topic.topicId, "#general");

			await fetch(`${baseUrl}/api/topic_subscribe`, {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({ topicId: "#general", peerId: "alice" }),
			});
			const subA = await readEvent(
				reader,
				decoder,
				buf,
				(e) => e.type === "topic_subscribed",
			);
			assert.equal((subA as { peerId: string }).peerId, "alice");

			await fetch(`${baseUrl}/api/topic_subscribe`, {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({ topicId: "#general", peerId: "bob" }),
			});
			const subB = await readEvent(
				reader,
				decoder,
				buf,
				(e) =>
					e.type === "topic_subscribed" &&
					(e as { peerId: string }).peerId === "bob",
			);
			assert.equal((subB as { peerId: string }).peerId, "bob");

			await fetch(`${baseUrl}/api/topic_send`, {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({
					topicId: "#general",
					from: "alice",
					subject: "hello",
					body: "hi all",
				}),
			});

			const published = await readEvent(
				reader,
				decoder,
				buf,
				(e) => e.type === "topic_message_published",
			);
			const pub = published as {
				from: string;
				deliveredTo: string[];
				topicId: string;
			};
			assert.equal(pub.from, "alice");
			assert.equal(pub.topicId, "#general");
			assert.deepEqual(pub.deliveredTo, ["bob"]);

			await fetch(`${baseUrl}/api/topic_unsubscribe`, {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({ topicId: "#general", peerId: "bob" }),
			});
			const unsub = await readEvent(
				reader,
				decoder,
				buf,
				(e) => e.type === "topic_unsubscribed",
			);
			assert.equal((unsub as { peerId: string }).peerId, "bob");
		} finally {
			controller.abort();
		}
	});

	test("late connect snapshot includes existing peers", async () => {
		await fetch(`${baseUrl}/api/register`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ peerId: "alice" }),
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
			const peers = (snap as { peers: { peerId: string }[] }).peers;
			assert.equal(peers.length, 1);
			assert.equal(peers[0].peerId, "alice");
		} finally {
			controller.abort();
		}
	});

	test("late connect snapshot includes existing topics", async () => {
		await fetch(`${baseUrl}/api/register`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ peerId: "alice" }),
		});
		await fetch(`${baseUrl}/api/topic_create`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ topicId: "#general", createdBy: "alice" }),
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
			const topics = (snap as { topics: { topicId: string }[] }).topics;
			assert.equal(topics.length, 1);
			assert.equal(topics[0].topicId, "#general");
		} finally {
			controller.abort();
		}
	});
});
