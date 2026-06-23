import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, beforeEach, describe, test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { createBroker } from "../../src/server/broker.js";
import { startCleanup } from "../../src/server/cleanup.js";
import { type CcDatabase, openDatabase } from "../../src/server/db.js";

describe("cleanup", () => {
	let tmpDir: string;
	let dbPath: string;
	let db: CcDatabase;

	before(() => {
		tmpDir = mkdtempSync(join(tmpdir(), "ccmb-cleanup-"));
		dbPath = join(tmpDir, "data.db");
	});

	after(() => {
		db?.close();
		rmSync(tmpDir, { recursive: true, force: true });
	});

	beforeEach(() => {
		db?.close();
		rmSync(dbPath, { force: true });
		db = openDatabase(dbPath);
	});

	test("expires in-flight and emits message_redelivered", async () => {
		const broker = createBroker(db, {
			visibilityTimeoutSec: -1,
			ttlDays: 30,
			dashboardUrl: "http://localhost",
		});
		broker.register({ peerId: "alice" });
		broker.register({ peerId: "bob" });
		const sent = broker.send({
			from: "alice",
			to: "bob",
			subject: "s",
			body: "b",
		});
		broker.read({ peerId: "bob" });

		const redelivered: string[] = [];
		broker.events.on("message_redelivered", (e: { messageId: string }) => {
			redelivered.push(e.messageId);
		});

		const handle = startCleanup(broker, db, { intervalSec: 0.05 });
		await delay(150);
		handle.stop();

		assert.ok(
			redelivered.includes(sent.messageId),
			"cleanup should have redelivered the expired in-flight message",
		);
	});

	test("deletes expired TTL rows and emits message_expired", async () => {
		const broker = createBroker(db, {
			visibilityTimeoutSec: 30,
			ttlDays: -1,
			dashboardUrl: "http://localhost",
		});
		broker.register({ peerId: "alice" });
		broker.register({ peerId: "bob" });
		const sent = broker.send({
			from: "alice",
			to: "bob",
			subject: "s",
			body: "b",
		});

		const expired: string[] = [];
		broker.events.on("message_expired", (e: { messageId: string }) => {
			expired.push(e.messageId);
		});

		const handle = startCleanup(broker, db, { intervalSec: 0.05 });
		await delay(150);
		handle.stop();

		assert.ok(
			expired.includes(sent.messageId),
			"cleanup should have deleted the TTL-expired message",
		);
	});

	test("stop() prevents further ticks", async () => {
		const broker = createBroker(db, {
			visibilityTimeoutSec: 30,
			ttlDays: 30,
			dashboardUrl: "http://localhost",
		});
		let ticks = 0;
		broker.events.on("message_redelivered", () => {
			ticks++;
		});
		const handle = startCleanup(broker, db, { intervalSec: 0.05 });
		await delay(100);
		handle.stop();
		const before = ticks;
		await delay(150);
		assert.equal(ticks, before, "no more events after stop");
	});
});
