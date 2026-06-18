import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, beforeEach, describe, test } from "node:test";
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
});
