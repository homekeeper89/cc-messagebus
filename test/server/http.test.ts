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
import { setTimeout as delay } from "node:timers/promises";
import { parseSseChunks } from "../../src/client/tail.js";
import { createServer, type Server } from "../../src/server/index.js";

describe("http", () => {
	let tmpDir: string;
	let dbPath: string;
	let server: Server;

	before(() => {
		tmpDir = mkdtempSync(join(tmpdir(), "ccmb-http-"));
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
		server = createServer({
			dbPath,
			visibilityTimeoutSec: 30,
			ttlDays: 30,
			cleanupIntervalSec: 60,
		});
		await server.app.ready();
	});

	test("POST /api/register returns ok envelope", async () => {
		const res = await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "alice" },
		});
		assert.equal(res.statusCode, 200);
		const body = res.json();
		assert.equal(body.ok, true);
		assert.equal(body.topicId, "alice");
		assert.ok(body.monitorCommand);
	});

	test("POST /api/register duplicate returns 409 + envelope", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "alice" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "alice" },
		});
		assert.equal(res.statusCode, 409);
		const body = res.json();
		assert.equal(body.ok, false);
		assert.equal(body.error.code, "TOPIC_ALREADY_REGISTERED");
	});

	test("POST /api/send empty subject returns 400 + VALIDATION_FAILED", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "bob" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/send",
			payload: { from: "alice", to: "bob", subject: "", body: "x" },
		});
		assert.equal(res.statusCode, 400);
		const body = res.json();
		assert.equal(body.ok, false);
		assert.equal(body.error.code, "VALIDATION_FAILED");
	});

	test("POST /api/read max=300 returns 400", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "bob" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/read",
			payload: { topicId: "bob", max: 300 },
		});
		assert.equal(res.statusCode, 400);
		assert.equal(res.json().error.code, "VALIDATION_FAILED");
	});

	test("POST /api/list_peers empty state returns []", async () => {
		const res = await server.app.inject({
			method: "POST",
			url: "/api/list_peers",
			payload: {},
		});
		assert.equal(res.statusCode, 200);
		const body = res.json();
		assert.equal(body.ok, true);
		assert.deepEqual(body.peers, []);
	});

	test("POST /api/list_peers includes lastActivityAt and orders by it", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/send",
			payload: { from: "alice", to: "alice", subject: "s", body: "b" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "bob" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/list_peers",
			payload: {},
		});
		assert.equal(res.statusCode, 200);
		const body = res.json();
		assert.equal(body.ok, true);
		assert.equal(body.peers.length, 2);
		assert.equal(body.peers[0].topicId, "alice");
		assert.equal(typeof body.peers[0].lastActivityAt, "string");
		assert.equal(body.peers[1].topicId, "bob");
		assert.equal(body.peers[1].lastActivityAt, null);
	});

	test("POST /api/list_channels empty state returns []", async () => {
		const res = await server.app.inject({
			method: "POST",
			url: "/api/list_channels",
			payload: {},
		});
		assert.equal(res.statusCode, 200);
		const body = res.json();
		assert.equal(body.ok, true);
		assert.deepEqual(body.channels, []);
	});

	test("POST /api/list_channels serializes lastPublishedAt as null for unused channel", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/channel_create",
			payload: { channelId: "general", createdBy: "alice" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/list_channels",
			payload: {},
		});
		assert.equal(res.statusCode, 200);
		const body = res.json();
		assert.equal(body.ok, true);
		assert.equal(body.channels.length, 1);
		assert.equal(body.channels[0].channelId, "general");
		assert.equal(body.channels[0].createdBy, "alice");
		assert.equal(body.channels[0].subscriberCount, 0);
		assert.equal(body.channels[0].lastPublishedAt, null);
	});

	test("e2e: register/send/read/ack via HTTP", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "bob" },
		});
		const sendRes = await server.app.inject({
			method: "POST",
			url: "/api/send",
			payload: { from: "alice", to: "bob", subject: "ping", body: "hi" },
		});
		const sendBody = sendRes.json();
		assert.equal(sendRes.statusCode, 200);
		const messageId = sendBody.messageId;

		const readRes = await server.app.inject({
			method: "POST",
			url: "/api/read",
			payload: { topicId: "bob" },
		});
		assert.equal(readRes.statusCode, 200);
		assert.equal(readRes.json().messages.length, 1);

		const ackRes = await server.app.inject({
			method: "POST",
			url: "/api/ack",
			payload: { topicId: "bob", messageId },
		});
		assert.equal(ackRes.statusCode, 200);
		assert.equal(ackRes.json().ok, true);

		const rereadRes = await server.app.inject({
			method: "POST",
			url: "/api/read",
			payload: { topicId: "bob" },
		});
		assert.equal(rereadRes.json().messages.length, 0);
	});

	test("POST /api/unregister unknown topic returns 404 + TOPIC_NOT_FOUND", async () => {
		const res = await server.app.inject({
			method: "POST",
			url: "/api/unregister",
			payload: { topicId: "ghost" },
		});
		assert.equal(res.statusCode, 404);
		const body = res.json();
		assert.equal(body.ok, false);
		assert.equal(body.error.code, "TOPIC_NOT_FOUND");
	});

	test("POST /api/unregister purgeQueue=true drops messages", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "bob" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/send",
			payload: { from: "alice", to: "bob", subject: "s", body: "b" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/unregister",
			payload: { topicId: "bob", purgeQueue: true },
		});
		assert.equal(res.statusCode, 200);
		assert.equal(res.json().purged, true);

		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "bob" },
		});
		const peers = await server.app.inject({
			method: "POST",
			url: "/api/list_peers",
			payload: {},
		});
		const bob = peers
			.json()
			.peers.find((p: { topicId: string }) => p.topicId === "bob");
		assert.equal(bob.queueLength, 0);
	});

	test("POST /api/unregister purgeQueue=false preserves messages", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "bob" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/send",
			payload: { from: "alice", to: "bob", subject: "s", body: "b" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/unregister",
			payload: { topicId: "bob", purgeQueue: false },
		});
		assert.equal(res.statusCode, 200);
		assert.equal(res.json().purged, false);

		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "bob" },
		});
		const peers = await server.app.inject({
			method: "POST",
			url: "/api/list_peers",
			payload: {},
		});
		const bob = peers
			.json()
			.peers.find((p: { topicId: string }) => p.topicId === "bob");
		assert.equal(bob.queueLength, 1);
	});

	test("persistence: data survives db close + reopen", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "bob" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/send",
			payload: { from: "alice", to: "bob", subject: "p", body: "x" },
		});

		await server.app.close();
		server.db.close();

		server = createServer({
			dbPath,
			visibilityTimeoutSec: 30,
			ttlDays: 30,
			cleanupIntervalSec: 60,
		});
		await server.app.ready();

		const peers = await server.app.inject({
			method: "POST",
			url: "/api/list_peers",
			payload: {},
		});
		assert.equal(peers.json().peers.length, 2);

		const read = await server.app.inject({
			method: "POST",
			url: "/api/read",
			payload: { topicId: "bob" },
		});
		assert.equal(read.json().messages.length, 1);
	});

	test("POST /api/channel_create returns ok + channelId", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "alice" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/channel_create",
			payload: { channelId: "general", createdBy: "alice" },
		});
		assert.equal(res.statusCode, 200);
		const body = res.json();
		assert.equal(body.ok, true);
		assert.equal(body.channel.channelId, "general");
		assert.equal(body.channel.createdBy, "alice");
	});

	test("POST /api/channel_create duplicate returns 409 + CHANNEL_ALREADY_EXISTS", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/channel_create",
			payload: { channelId: "general", createdBy: "alice" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/channel_create",
			payload: { channelId: "general", createdBy: "alice" },
		});
		assert.equal(res.statusCode, 409);
		const body = res.json();
		assert.equal(body.ok, false);
		assert.equal(body.error.code, "CHANNEL_ALREADY_EXISTS");
	});

	test("POST /api/channel_create empty channelId returns 400 + VALIDATION_FAILED", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "alice" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/channel_create",
			payload: { channelId: "", createdBy: "alice" },
		});
		assert.equal(res.statusCode, 400);
		assert.equal(res.json().error.code, "VALIDATION_FAILED");
	});

	test("POST /api/channel_subscribe returns ok", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "bob" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/channel_create",
			payload: { channelId: "general", createdBy: "alice" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/channel_subscribe",
			payload: { channelId: "general", topicId: "bob" },
		});
		assert.equal(res.statusCode, 200);
		assert.equal(res.json().ok, true);
	});

	test("POST /api/channel_subscribe duplicate returns 409 + ALREADY_SUBSCRIBED", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "bob" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/channel_create",
			payload: { channelId: "general", createdBy: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/channel_subscribe",
			payload: { channelId: "general", topicId: "bob" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/channel_subscribe",
			payload: { channelId: "general", topicId: "bob" },
		});
		assert.equal(res.statusCode, 409);
		assert.equal(res.json().error.code, "ALREADY_SUBSCRIBED");
	});

	test("POST /api/channel_send fan-out delivers to N-1 subscribers", async () => {
		for (const topicId of ["alice", "bob", "carol"]) {
			await server.app.inject({
				method: "POST",
				url: "/api/register",
				payload: { topicId },
			});
		}
		await server.app.inject({
			method: "POST",
			url: "/api/channel_create",
			payload: { channelId: "general", createdBy: "alice" },
		});
		for (const topicId of ["bob", "carol"]) {
			await server.app.inject({
				method: "POST",
				url: "/api/channel_subscribe",
				payload: { channelId: "general", topicId },
			});
		}
		const res = await server.app.inject({
			method: "POST",
			url: "/api/channel_send",
			payload: {
				channelId: "general",
				from: "alice",
				subject: "hello",
				body: "world",
			},
		});
		assert.equal(res.statusCode, 200);
		const body = res.json();
		assert.equal(body.ok, true);
		assert.ok(body.channelMessageId);
		assert.equal(body.deliveredTo.length, 2);
		assert.ok(body.deliveredTo.includes("bob"));
		assert.ok(body.deliveredTo.includes("carol"));
		assert.ok(!body.deliveredTo.includes("alice"));
	});

	test("POST /api/channel_send unknown channel returns 404 + CHANNEL_NOT_FOUND", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "alice" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/channel_send",
			payload: {
				channelId: "ghost",
				from: "alice",
				subject: "s",
				body: "b",
			},
		});
		assert.equal(res.statusCode, 404);
		assert.equal(res.json().error.code, "CHANNEL_NOT_FOUND");
	});

	test("POST /api/channel_unsubscribe returns ok + unsubscribedAt", async () => {
		for (const topicId of ["alice", "bob"]) {
			await server.app.inject({
				method: "POST",
				url: "/api/register",
				payload: { topicId },
			});
		}
		await server.app.inject({
			method: "POST",
			url: "/api/channel_create",
			payload: { channelId: "general", createdBy: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/channel_subscribe",
			payload: { channelId: "general", topicId: "bob" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/channel_unsubscribe",
			payload: { channelId: "general", topicId: "bob" },
		});
		assert.equal(res.statusCode, 200);
		const body = res.json();
		assert.equal(body.ok, true);
		assert.ok(body.unsubscribedAt);
	});

	test("POST /api/channel_unsubscribe without prior subscribe returns 404 + NOT_SUBSCRIBED", async () => {
		for (const topicId of ["alice", "bob"]) {
			await server.app.inject({
				method: "POST",
				url: "/api/register",
				payload: { topicId },
			});
		}
		await server.app.inject({
			method: "POST",
			url: "/api/channel_create",
			payload: { channelId: "general", createdBy: "alice" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/channel_unsubscribe",
			payload: { channelId: "general", topicId: "bob" },
		});
		assert.equal(res.statusCode, 404);
		assert.equal(res.json().error.code, "NOT_SUBSCRIBED");
	});

	test("POST /api/channel_history returns messages DESC + hasMore=false on small set", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/channel_create",
			payload: { channelId: "general", createdBy: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/channel_send",
			payload: {
				channelId: "general",
				from: "alice",
				subject: "s",
				body: "b",
			},
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/channel_history",
			payload: { channelId: "general" },
		});
		assert.equal(res.statusCode, 200);
		const body = res.json();
		assert.equal(body.ok, true);
		assert.equal(body.messages.length, 1);
		assert.equal(body.hasMore, false);
		assert.ok(body.messages[0].expiresAt);
	});

	test("POST /api/channel_history with out-of-range limit returns 400 VALIDATION_FAILED", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/channel_create",
			payload: { channelId: "general", createdBy: "alice" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/channel_history",
			payload: { channelId: "general", limit: 999 },
		});
		assert.equal(res.statusCode, 400);
	});

	test("POST /api/channel_detail unknown channel returns 404 CHANNEL_NOT_FOUND", async () => {
		const res = await server.app.inject({
			method: "POST",
			url: "/api/channel_detail",
			payload: { channelId: "ghost" },
		});
		assert.equal(res.statusCode, 404);
		const body = res.json();
		assert.equal(body.ok, false);
		assert.equal(body.error.code, "CHANNEL_NOT_FOUND");
	});

	test("POST /api/channel_detail returns subscribers with per-subscriber stats", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { topicId: "bob" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/channel_create",
			payload: { channelId: "general", createdBy: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/channel_subscribe",
			payload: { channelId: "general", topicId: "bob" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/channel_send",
			payload: {
				channelId: "general",
				from: "alice",
				subject: "s",
				body: "b",
			},
		});

		const res = await server.app.inject({
			method: "POST",
			url: "/api/channel_detail",
			payload: { channelId: "general" },
		});
		assert.equal(res.statusCode, 200);
		const body = res.json();
		assert.equal(body.ok, true);
		assert.equal(body.channel.channelId, "general");
		assert.equal(body.channel.createdBy, "alice");
		assert.equal(body.channel.subscribers.length, 1);
		const bob = body.channel.subscribers[0];
		assert.equal(bob.topicId, "bob");
		assert.equal(bob.queueDepth, 1);
		assert.equal(bob.lastReadAt, null);
	});
});

describe("tail SSE", () => {
	let tmpDir: string;
	let dbPath: string;
	let server: Server;
	let baseUrl: string;

	before(() => {
		tmpDir = mkdtempSync(join(tmpdir(), "ccmb-tail-"));
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
			visibilityTimeoutSec: 30,
			ttlDays: 30,
			cleanupIntervalSec: 60,
		});
		baseUrl = await server.start();
	});

	afterEach(async () => {
		await server.stop();
	});

	async function readOneEvent(
		reader: ReadableStreamDefaultReader<Uint8Array>,
		decoder: TextDecoder,
		predicate: (event: { type: string }) => boolean,
	): Promise<{ type: string; [k: string]: unknown }> {
		let buffer = "";
		while (true) {
			const { value, done } = await reader.read();
			if (done) throw new Error("stream ended before predicate matched");
			buffer += decoder.decode(value, { stream: true });
			const { events, rest } = parseSseChunks(buffer);
			buffer = rest;
			for (const e of events) {
				const evt = e as { type: string };
				if (predicate(evt)) return evt as { type: string };
			}
		}
	}

	test("GET /tail/:topicId unknown topic returns 404", async () => {
		const res = await fetch(`${baseUrl}/tail/ghost`);
		assert.equal(res.status, 404);
		const body = (await res.json()) as {
			ok: boolean;
			error: { code: string };
		};
		assert.equal(body.ok, false);
		assert.equal(body.error.code, "TOPIC_NOT_FOUND");
	});

	test("GET /tail/:topicId pushes message_delivered on send", async () => {
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
			const res = await fetch(`${baseUrl}/tail/bob`, {
				signal: controller.signal,
				headers: { Accept: "text/event-stream" },
			});
			assert.equal(res.status, 200);
			assert.ok(res.body, "expected response body stream");
			const reader = res.body.getReader();
			const decoder = new TextDecoder();

			const heartbeat = await readOneEvent(
				reader,
				decoder,
				(e) => e.type === "heartbeat",
			);
			assert.equal(heartbeat.type, "heartbeat");

			await fetch(`${baseUrl}/api/send`, {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({
					from: "alice",
					to: "bob",
					subject: "ping",
					body: "hi",
				}),
			});

			const delivered = await readOneEvent(
				reader,
				decoder,
				(e) => e.type === "message_delivered",
			);
			assert.equal(delivered.type, "message_delivered");
			const msg = (delivered as { message: { to: string; subject: string } })
				.message;
			assert.equal(msg.to, "bob");
			assert.equal(msg.subject, "ping");
		} finally {
			controller.abort();
		}
	});

	test("GET /tail/:topicId pushes message_delivered on channel_send fan-out", async () => {
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
		await fetch(`${baseUrl}/api/channel_create`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ channelId: "ch-tail", createdBy: "alice" }),
		});
		await fetch(`${baseUrl}/api/channel_subscribe`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ channelId: "ch-tail", topicId: "bob" }),
		});

		const controller = new AbortController();
		try {
			const res = await fetch(`${baseUrl}/tail/bob`, {
				signal: controller.signal,
				headers: { Accept: "text/event-stream" },
			});
			assert.equal(res.status, 200);
			assert.ok(res.body, "expected response body stream");
			const reader = res.body.getReader();
			const decoder = new TextDecoder();

			await readOneEvent(reader, decoder, (e) => e.type === "heartbeat");

			await fetch(`${baseUrl}/api/channel_send`, {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({
					channelId: "ch-tail",
					from: "alice",
					subject: "epic-update",
					body: "channel fan-out test",
				}),
			});

			const delivered = await readOneEvent(
				reader,
				decoder,
				(e) => e.type === "message_delivered",
			);
			const msg = (
				delivered as {
					message: { to: string; from: string; subject: string };
				}
			).message;
			assert.equal(msg.to, "bob");
			assert.equal(msg.from, "alice");
			assert.equal(msg.subject, "epic-update");
		} finally {
			controller.abort();
		}
	});

	test("GET /tail/:topicId close marks peer disconnected", async () => {
		await fetch(`${baseUrl}/api/register`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ topicId: "bob" }),
		});

		const controller = new AbortController();
		const res = await fetch(`${baseUrl}/tail/bob`, {
			signal: controller.signal,
			headers: { Accept: "text/event-stream" },
		});
		assert.equal(res.status, 200);
		assert.ok(res.body, "expected response body stream");
		const reader = res.body.getReader();
		const decoder = new TextDecoder();
		await readOneEvent(reader, decoder, (e) => e.type === "heartbeat");

		controller.abort();
		// raw 'close' 가 broker.disconnect 까지 호출하는 데 짧은 tick 필요
		await delay(100);

		const peersRes = await fetch(`${baseUrl}/api/list_peers`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({}),
		});
		const peersBody = (await peersRes.json()) as {
			peers: { topicId: string; status: string }[];
		};
		const bob = peersBody.peers.find((p) => p.topicId === "bob");
		assert.ok(bob, "bob peer must exist");
		assert.equal(bob.status, "disconnected");
	});
});
