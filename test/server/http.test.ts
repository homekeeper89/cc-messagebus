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
			config: { issueRepo: null },
		});
		await server.app.ready();
	});

	test("POST /api/register returns ok envelope", async () => {
		const res = await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "alice" },
		});
		assert.equal(res.statusCode, 200);
		const body = res.json();
		assert.equal(body.ok, true);
		assert.equal(body.peerId, "alice");
		assert.ok(body.monitorCommand);
	});

	test("POST /api/register duplicate returns 409 + envelope", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "alice" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "alice" },
		});
		assert.equal(res.statusCode, 409);
		const body = res.json();
		assert.equal(body.ok, false);
		assert.equal(body.error.code, "PEER_ALREADY_REGISTERED");
	});

	test("POST /api/send empty subject returns 400 + VALIDATION_FAILED", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "bob" },
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
			payload: { peerId: "bob" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/read",
			payload: { peerId: "bob", max: 300 },
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
			payload: { peerId: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/send",
			payload: { from: "alice", to: "alice", subject: "s", body: "b" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "bob" },
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
		assert.equal(body.peers[0].peerId, "alice");
		assert.equal(typeof body.peers[0].lastActivityAt, "string");
		assert.equal(body.peers[1].peerId, "bob");
		assert.equal(body.peers[1].lastActivityAt, null);
	});

	test("POST /api/list_topics empty state returns []", async () => {
		const res = await server.app.inject({
			method: "POST",
			url: "/api/list_topics",
			payload: {},
		});
		assert.equal(res.statusCode, 200);
		const body = res.json();
		assert.equal(body.ok, true);
		assert.deepEqual(body.topics, []);
	});

	test("POST /api/list_topics serializes lastPublishedAt as null for unused topic", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/topic_create",
			payload: { topicId: "general", createdBy: "alice" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/list_topics",
			payload: {},
		});
		assert.equal(res.statusCode, 200);
		const body = res.json();
		assert.equal(body.ok, true);
		assert.equal(body.topics.length, 1);
		assert.equal(body.topics[0].topicId, "general");
		assert.equal(body.topics[0].createdBy, "alice");
		assert.equal(body.topics[0].subscriberCount, 0);
		assert.equal(body.topics[0].lastPublishedAt, null);
	});

	test("e2e: register/send/read/ack via HTTP", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "bob" },
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
			payload: { peerId: "bob" },
		});
		assert.equal(readRes.statusCode, 200);
		assert.equal(readRes.json().messages.length, 1);

		const ackRes = await server.app.inject({
			method: "POST",
			url: "/api/ack",
			payload: { peerId: "bob", messageId },
		});
		assert.equal(ackRes.statusCode, 200);
		assert.equal(ackRes.json().ok, true);

		const rereadRes = await server.app.inject({
			method: "POST",
			url: "/api/read",
			payload: { peerId: "bob" },
		});
		assert.equal(rereadRes.json().messages.length, 0);
	});

	test("POST /api/unregister unknown topic returns 404 + PEER_NOT_FOUND", async () => {
		const res = await server.app.inject({
			method: "POST",
			url: "/api/unregister",
			payload: { peerId: "ghost" },
		});
		assert.equal(res.statusCode, 404);
		const body = res.json();
		assert.equal(body.ok, false);
		assert.equal(body.error.code, "PEER_NOT_FOUND");
	});

	test("POST /api/unregister purgeQueue=true drops messages", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "bob" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/send",
			payload: { from: "alice", to: "bob", subject: "s", body: "b" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/unregister",
			payload: { peerId: "bob", purgeQueue: true },
		});
		assert.equal(res.statusCode, 200);
		assert.equal(res.json().purged, true);

		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "bob" },
		});
		const peers = await server.app.inject({
			method: "POST",
			url: "/api/list_peers",
			payload: {},
		});
		const bob = peers
			.json()
			.peers.find((p: { peerId: string }) => p.peerId === "bob");
		assert.equal(bob.queueLength, 0);
	});

	test("POST /api/unregister purgeQueue=false preserves messages", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "bob" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/send",
			payload: { from: "alice", to: "bob", subject: "s", body: "b" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/unregister",
			payload: { peerId: "bob", purgeQueue: false },
		});
		assert.equal(res.statusCode, 200);
		assert.equal(res.json().purged, false);

		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "bob" },
		});
		const peers = await server.app.inject({
			method: "POST",
			url: "/api/list_peers",
			payload: {},
		});
		const bob = peers
			.json()
			.peers.find((p: { peerId: string }) => p.peerId === "bob");
		assert.equal(bob.queueLength, 1);
	});

	test("persistence: data survives db close + reopen", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "bob" },
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
			payload: { peerId: "bob" },
		});
		assert.equal(read.json().messages.length, 1);
	});

	test("POST /api/topic_create returns ok + topicId", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "alice" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/topic_create",
			payload: { topicId: "general", createdBy: "alice" },
		});
		assert.equal(res.statusCode, 200);
		const body = res.json();
		assert.equal(body.ok, true);
		assert.equal(body.topic.topicId, "general");
		assert.equal(body.topic.createdBy, "alice");
	});

	test("POST /api/topic_create duplicate returns 409 + TOPIC_ALREADY_EXISTS", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/topic_create",
			payload: { topicId: "general", createdBy: "alice" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/topic_create",
			payload: { topicId: "general", createdBy: "alice" },
		});
		assert.equal(res.statusCode, 409);
		const body = res.json();
		assert.equal(body.ok, false);
		assert.equal(body.error.code, "TOPIC_ALREADY_EXISTS");
	});

	test("POST /api/topic_create empty topicId returns 400 + VALIDATION_FAILED", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "alice" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/topic_create",
			payload: { topicId: "", createdBy: "alice" },
		});
		assert.equal(res.statusCode, 400);
		assert.equal(res.json().error.code, "VALIDATION_FAILED");
	});

	test("POST /api/topic_subscribe returns ok", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "bob" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/topic_create",
			payload: { topicId: "general", createdBy: "alice" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/topic_subscribe",
			payload: { topicId: "general", peerId: "bob" },
		});
		assert.equal(res.statusCode, 200);
		assert.equal(res.json().ok, true);
	});

	test("POST /api/topic_subscribe duplicate returns 409 + ALREADY_SUBSCRIBED", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "bob" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/topic_create",
			payload: { topicId: "general", createdBy: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/topic_subscribe",
			payload: { topicId: "general", peerId: "bob" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/topic_subscribe",
			payload: { topicId: "general", peerId: "bob" },
		});
		assert.equal(res.statusCode, 409);
		assert.equal(res.json().error.code, "ALREADY_SUBSCRIBED");
	});

	test("POST /api/topic_send fan-out delivers to N-1 subscribers", async () => {
		for (const peerId of ["alice", "bob", "carol"]) {
			await server.app.inject({
				method: "POST",
				url: "/api/register",
				payload: { peerId },
			});
		}
		await server.app.inject({
			method: "POST",
			url: "/api/topic_create",
			payload: { topicId: "general", createdBy: "alice" },
		});
		for (const peerId of ["bob", "carol"]) {
			await server.app.inject({
				method: "POST",
				url: "/api/topic_subscribe",
				payload: { topicId: "general", peerId },
			});
		}
		const res = await server.app.inject({
			method: "POST",
			url: "/api/topic_send",
			payload: {
				topicId: "general",
				from: "alice",
				subject: "hello",
				body: "world",
			},
		});
		assert.equal(res.statusCode, 200);
		const body = res.json();
		assert.equal(body.ok, true);
		assert.ok(body.topicMessageId);
		assert.equal(body.deliveredTo.length, 2);
		assert.ok(body.deliveredTo.includes("bob"));
		assert.ok(body.deliveredTo.includes("carol"));
		assert.ok(!body.deliveredTo.includes("alice"));
	});

	test("POST /api/topic_send unknown topic returns 404 + TOPIC_NOT_FOUND", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "alice" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/topic_send",
			payload: {
				topicId: "ghost",
				from: "alice",
				subject: "s",
				body: "b",
			},
		});
		assert.equal(res.statusCode, 404);
		assert.equal(res.json().error.code, "TOPIC_NOT_FOUND");
	});

	test("POST /api/topic_unsubscribe returns ok + unsubscribedAt", async () => {
		for (const peerId of ["alice", "bob"]) {
			await server.app.inject({
				method: "POST",
				url: "/api/register",
				payload: { peerId },
			});
		}
		await server.app.inject({
			method: "POST",
			url: "/api/topic_create",
			payload: { topicId: "general", createdBy: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/topic_subscribe",
			payload: { topicId: "general", peerId: "bob" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/topic_unsubscribe",
			payload: { topicId: "general", peerId: "bob" },
		});
		assert.equal(res.statusCode, 200);
		const body = res.json();
		assert.equal(body.ok, true);
		assert.ok(body.unsubscribedAt);
	});

	test("POST /api/topic_unsubscribe without prior subscribe returns 404 + NOT_SUBSCRIBED", async () => {
		for (const peerId of ["alice", "bob"]) {
			await server.app.inject({
				method: "POST",
				url: "/api/register",
				payload: { peerId },
			});
		}
		await server.app.inject({
			method: "POST",
			url: "/api/topic_create",
			payload: { topicId: "general", createdBy: "alice" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/topic_unsubscribe",
			payload: { topicId: "general", peerId: "bob" },
		});
		assert.equal(res.statusCode, 404);
		assert.equal(res.json().error.code, "NOT_SUBSCRIBED");
	});

	test("POST /api/topic_history returns messages DESC + hasMore=false on small set", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/topic_create",
			payload: { topicId: "general", createdBy: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/topic_send",
			payload: {
				topicId: "general",
				from: "alice",
				subject: "s",
				body: "b",
			},
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/topic_history",
			payload: { topicId: "general" },
		});
		assert.equal(res.statusCode, 200);
		const body = res.json();
		assert.equal(body.ok, true);
		assert.equal(body.messages.length, 1);
		assert.equal(body.hasMore, false);
		assert.ok(body.messages[0].expiresAt);
	});

	test("POST /api/topic_history with out-of-range limit returns 400 VALIDATION_FAILED", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/topic_create",
			payload: { topicId: "general", createdBy: "alice" },
		});
		const res = await server.app.inject({
			method: "POST",
			url: "/api/topic_history",
			payload: { topicId: "general", limit: 999 },
		});
		assert.equal(res.statusCode, 400);
	});

	test("POST /api/topic_detail unknown topic returns 404 TOPIC_NOT_FOUND", async () => {
		const res = await server.app.inject({
			method: "POST",
			url: "/api/topic_detail",
			payload: { topicId: "ghost" },
		});
		assert.equal(res.statusCode, 404);
		const body = res.json();
		assert.equal(body.ok, false);
		assert.equal(body.error.code, "TOPIC_NOT_FOUND");
	});

	test("POST /api/topic_detail returns subscribers with per-subscriber stats", async () => {
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/register",
			payload: { peerId: "bob" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/topic_create",
			payload: { topicId: "general", createdBy: "alice" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/topic_subscribe",
			payload: { topicId: "general", peerId: "bob" },
		});
		await server.app.inject({
			method: "POST",
			url: "/api/topic_send",
			payload: {
				topicId: "general",
				from: "alice",
				subject: "s",
				body: "b",
			},
		});

		const res = await server.app.inject({
			method: "POST",
			url: "/api/topic_detail",
			payload: { topicId: "general" },
		});
		assert.equal(res.statusCode, 200);
		const body = res.json();
		assert.equal(body.ok, true);
		assert.equal(body.topic.topicId, "general");
		assert.equal(body.topic.createdBy, "alice");
		assert.equal(body.topic.subscribers.length, 1);
		const bob = body.topic.subscribers[0];
		assert.equal(bob.peerId, "bob");
		assert.equal(bob.queueDepth, 1);
		assert.equal(bob.lastReadAt, null);
	});

	test("POST /api/server_info returns issueRepo + version", async () => {
		const res = await server.app.inject({
			method: "POST",
			url: "/api/server_info",
			payload: {},
		});
		assert.equal(res.statusCode, 200);
		const body = res.json();
		assert.equal(body.ok, true);
		assert.equal(body.issueRepo, null);
		assert.ok(typeof body.version === "string" && body.version.length > 0);
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
});
