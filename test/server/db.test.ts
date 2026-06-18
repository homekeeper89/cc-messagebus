import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, beforeEach, describe, test } from "node:test";
import { type CcDatabase, openDatabase } from "../../src/server/db.js";

describe("db", () => {
	let tmpDir: string;
	let dbPath: string;
	let db: CcDatabase;

	before(() => {
		tmpDir = mkdtempSync(join(tmpdir(), "ccmb-db-"));
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

	test("openDatabase creates the parent directory", () => {
		const nestedPath = join(tmpDir, "nested", "deep", "data.db");
		const nested = openDatabase(nestedPath);
		assert.ok(nested);
		nested.close();
	});

	test("registerSession returns peer with status=connected", () => {
		const peer = db.registerSession("saturn", "2026-06-18T00:00:00.000Z");
		assert.equal(peer.topicId, "saturn");
		assert.equal(peer.status, "connected");
		assert.equal(peer.queueLength, 0);
	});

	test("registerSession throws TOPIC_ALREADY_REGISTERED on duplicate connected", () => {
		db.registerSession("saturn", "2026-06-18T00:00:00.000Z");
		assert.throws(
			() => db.registerSession("saturn", "2026-06-18T00:00:01.000Z"),
			/TOPIC_ALREADY_REGISTERED/,
		);
	});

	test("registerSession reactivates disconnected session preserving queue", () => {
		const now = "2026-06-18T00:00:00.000Z";
		db.registerSession("alice", now);
		db.registerSession("bob", now);
		db.insertMessage({
			id: "m1",
			from: "alice",
			to: "bob",
			subject: "s",
			body: "b",
			threadId: null,
			sentAt: now,
			inFlightUntil: null,
			ackedAt: null,
			expiresAt: "2027-06-18T00:00:00.000Z",
		});
		db.markDisconnected("bob", now);
		const reactivated = db.registerSession("bob", "2026-06-18T01:00:00.000Z");
		assert.equal(reactivated.status, "connected");
		assert.equal(reactivated.queueLength, 1);
	});

	test("fetchDeliverable marks rows in-flight and second call returns empty", () => {
		const now = "2026-06-18T00:00:00.000Z";
		const future = "2026-06-18T00:05:00.000Z";
		db.registerSession("bob", now);
		db.insertMessage({
			id: "m1",
			from: "a",
			to: "bob",
			subject: "s",
			body: "b",
			threadId: null,
			sentAt: now,
			inFlightUntil: null,
			ackedAt: null,
			expiresAt: "2027-01-01T00:00:00.000Z",
		});
		const firstFetch = db.fetchDeliverable("bob", 10, now, future);
		assert.equal(firstFetch.length, 1);
		assert.equal(firstFetch[0]?.id, "m1");
		assert.equal(firstFetch[0]?.inFlightUntil, future);

		const secondFetch = db.fetchDeliverable("bob", 10, now, future);
		assert.equal(
			secondFetch.length,
			0,
			"in-flight lock prevents redelivery within visibility window",
		);
	});

	test("ackMessage throws MESSAGE_NOT_IN_FLIGHT on never-read message", () => {
		const now = "2026-06-18T00:00:00.000Z";
		db.registerSession("bob", now);
		db.insertMessage({
			id: "m1",
			from: "a",
			to: "bob",
			subject: "s",
			body: "b",
			threadId: null,
			sentAt: now,
			inFlightUntil: null,
			ackedAt: null,
			expiresAt: "2027-01-01T00:00:00.000Z",
		});
		assert.throws(
			() => db.ackMessage("bob", "m1", now),
			/MESSAGE_NOT_IN_FLIGHT/,
		);
	});

	test("expireInFlight clears in_flight_until for past locks", () => {
		const fetchTime = "2026-06-17T00:00:00.000Z";
		const visibilityEnd = "2026-06-17T00:00:30.000Z";
		const now = "2026-06-18T00:00:00.000Z";
		db.registerSession("bob", fetchTime);
		db.insertMessage({
			id: "m-locked",
			from: "a",
			to: "bob",
			subject: "s",
			body: "b",
			threadId: null,
			sentAt: fetchTime,
			inFlightUntil: null,
			ackedAt: null,
			expiresAt: "2027-01-01T00:00:00.000Z",
		});
		db.fetchDeliverable("bob", 10, fetchTime, visibilityEnd);
		const expired = db.expireInFlight(now);
		assert.deepEqual(expired, ["m-locked"]);

		const refetched = db.fetchDeliverable("bob", 10, now, now);
		assert.equal(
			refetched.length,
			1,
			"row becomes deliverable again after expireInFlight",
		);
	});

	test("deleteExpired removes rows past TTL", () => {
		const now = "2026-06-18T00:00:00.000Z";
		db.registerSession("bob", now);
		db.insertMessage({
			id: "m-dead",
			from: "a",
			to: "bob",
			subject: "s",
			body: "b",
			threadId: null,
			sentAt: "2026-01-01T00:00:00.000Z",
			inFlightUntil: null,
			ackedAt: null,
			expiresAt: "2026-06-01T00:00:00.000Z",
		});
		const deleted = db.deleteExpired(now);
		assert.deepEqual(deleted, ["m-dead"]);
	});
});
