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
		assert.equal(peer.peerId, "saturn");
		assert.equal(peer.status, "connected");
		assert.equal(peer.queueLength, 0);
	});

	test("registerSession throws PEER_ALREADY_REGISTERED on duplicate connected", () => {
		db.registerSession("saturn", "2026-06-18T00:00:00.000Z");
		assert.throws(
			() => db.registerSession("saturn", "2026-06-18T00:00:01.000Z"),
			/PEER_ALREADY_REGISTERED/,
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

describe("topics", () => {
	let tmpDir: string;
	let dbPath: string;
	let db: CcDatabase;

	before(() => {
		tmpDir = mkdtempSync(join(tmpdir(), "ccmb-tp-"));
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

	test("createTopic inserts new topic and rejects duplicates", () => {
		db.createTopic("epic-1", "saturn", "2026-06-19T00:00:00.000Z");
		assert.throws(
			() => db.createTopic("epic-1", "saturn", "2026-06-19T00:00:01.000Z"),
			/TOPIC_ALREADY_EXISTS/,
		);
	});

	test("subscribeTopic throws TOPIC_NOT_FOUND for unknown topic", () => {
		assert.throws(
			() => db.subscribeTopic("nope", "saturn", "2026-06-19T00:00:00.000Z"),
			/TOPIC_NOT_FOUND/,
		);
	});

	test("subscribeTopic throws ALREADY_SUBSCRIBED on duplicate", () => {
		db.createTopic("epic-1", "saturn", "2026-06-19T00:00:00.000Z");
		db.subscribeTopic("epic-1", "carme", "2026-06-19T00:00:01.000Z");
		assert.throws(
			() => db.subscribeTopic("epic-1", "carme", "2026-06-19T00:00:02.000Z"),
			/ALREADY_SUBSCRIBED/,
		);
	});

	test("topicSend fans out to subscribers excluding sender", () => {
		db.createTopic("epic-1", "saturn", "2026-06-19T00:00:00.000Z");
		db.subscribeTopic("epic-1", "saturn", "2026-06-19T00:00:01.000Z");
		db.subscribeTopic("epic-1", "carme", "2026-06-19T00:00:02.000Z");
		db.subscribeTopic("epic-1", "europa", "2026-06-19T00:00:03.000Z");

		const result = db.topicSend({
			topicMessageId: "cm-1",
			topicId: "epic-1",
			from: "saturn",
			subject: "plan-update",
			body: "hello",
			sentAt: "2026-06-19T00:00:04.000Z",
			expiresAt: "2026-07-19T00:00:04.000Z",
			deliveryMessageIds: ["m-1", "m-2"],
		});

		assert.equal(result.deliveredTo.length, 2);
		assert.ok(!result.deliveredTo.includes("saturn"));

		const now = "2026-06-19T00:00:05.000Z";
		const inFlight = "2026-06-19T00:00:35.000Z";
		assert.equal(db.fetchDeliverable("carme", 10, now, inFlight).length, 1);
		assert.equal(db.fetchDeliverable("europa", 10, now, inFlight).length, 1);
		assert.equal(db.fetchDeliverable("saturn", 10, now, inFlight).length, 0);
	});

	test("topicSend throws TOPIC_NOT_FOUND for unknown topic", () => {
		assert.throws(
			() =>
				db.topicSend({
					topicMessageId: "cm-1",
					topicId: "nope",
					from: "saturn",
					subject: "x",
					body: "x",
					sentAt: "2026-06-19T00:00:00.000Z",
					expiresAt: "2026-07-19T00:00:00.000Z",
					deliveryMessageIds: [],
				}),
			/TOPIC_NOT_FOUND/,
		);
	});

	test("topicSend rolls back canonical insert when deliveryMessageIds count mismatches", () => {
		db.createTopic("epic-1", "saturn", "2026-06-19T00:00:00.000Z");
		db.subscribeTopic("epic-1", "carme", "2026-06-19T00:00:01.000Z");
		db.subscribeTopic("epic-1", "europa", "2026-06-19T00:00:02.000Z");

		assert.throws(() =>
			db.topicSend({
				topicMessageId: "cm-1",
				topicId: "epic-1",
				from: "saturn",
				subject: "x",
				body: "x",
				sentAt: "2026-06-19T00:00:03.000Z",
				expiresAt: "2026-07-19T00:00:03.000Z",
				deliveryMessageIds: ["m-1"],
			}),
		);

		const result = db.topicSend({
			topicMessageId: "cm-1",
			topicId: "epic-1",
			from: "saturn",
			subject: "x",
			body: "x",
			sentAt: "2026-06-19T00:00:04.000Z",
			expiresAt: "2026-07-19T00:00:04.000Z",
			deliveryMessageIds: ["m-1", "m-2"],
		});
		assert.equal(result.deliveredTo.length, 2);
	});

	test("openDatabase is idempotent on existing db (backward-compatible ALTER)", () => {
		db.createTopic("epic-1", "saturn", "2026-06-19T00:00:00.000Z");
		db.close();
		const db2 = openDatabase(dbPath);
		assert.ok(db2);
		assert.throws(
			() => db2.createTopic("epic-1", "saturn", "2026-06-19T00:00:01.000Z"),
			/TOPIC_ALREADY_EXISTS/,
		);
		db2.close();
		db = openDatabase(dbPath);
	});

	test("inspectLastActivityAt should return null for pre-migration sessions on reopen", () => {
		db.registerSession("saturn", "2026-06-19T00:00:00.000Z");
		assert.equal(db.inspectLastActivityAt("saturn"), null);
		db.close();

		db = openDatabase(dbPath);
		assert.equal(
			db.inspectLastActivityAt("saturn"),
			null,
			"existing session must have null last_activity_at after backward-compatible ALTER",
		);
	});
});
