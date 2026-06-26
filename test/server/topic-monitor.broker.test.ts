import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, beforeEach, describe, test } from "node:test";
import {
	type Broker,
	BrokerError,
	createBroker,
} from "../../src/server/broker.js";
import { type CcDatabase, openDatabase } from "../../src/server/db.js";

describe("broker topicMonitor", () => {
	let tmpDir: string;
	let dbPath: string;
	let db: CcDatabase;
	let broker: Broker;
	let clockMs: number;

	before(() => {
		tmpDir = mkdtempSync(join(tmpdir(), "ccmb-topic-monitor-"));
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
		// sent_at tie-break flake 회피: monotonic clock 주입 (topics.broker.test 패턴 차용)
		clockMs = Date.parse("2026-01-01T00:00:00.000Z");
		broker = createBroker(db, {
			visibilityTimeoutSec: 30,
			ttlDays: 30,
			dashboardUrl: "http://localhost:5959",
			clock: () => {
				clockMs += 1;
				return new Date(clockMs).toISOString();
			},
		});
	});

	test("first call from null cursor returns all messages and advances cursor", () => {
		broker.register({ peerId: "alice" });
		broker.register({ peerId: "bob" });
		broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
		broker.topicSubscribe({ topicId: "ch-1", peerId: "bob" });
		const m1 = broker.topicSend({
			topicId: "ch-1",
			from: "alice",
			subject: "s1",
			body: "b1",
		});
		const m2 = broker.topicSend({
			topicId: "ch-1",
			from: "alice",
			subject: "s2",
			body: "b2",
		});

		const result = broker.topicMonitor({ topicId: "ch-1", peerId: "bob" });

		assert.equal(result.messages.length, 2);
		assert.equal(result.messages[0]?.topicMessageId, m1.topicMessageId);
		assert.equal(result.messages[1]?.topicMessageId, m2.topicMessageId);
		assert.equal(result.cursor, m2.topicMessageId);
	});

	test("second call from advanced cursor returns only newer messages", () => {
		broker.register({ peerId: "alice" });
		broker.register({ peerId: "bob" });
		broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
		broker.topicSubscribe({ topicId: "ch-1", peerId: "bob" });
		broker.topicSend({
			topicId: "ch-1",
			from: "alice",
			subject: "s1",
			body: "b1",
		});
		broker.topicMonitor({ topicId: "ch-1", peerId: "bob" });

		const m2 = broker.topicSend({
			topicId: "ch-1",
			from: "alice",
			subject: "s2",
			body: "b2",
		});
		const result = broker.topicMonitor({ topicId: "ch-1", peerId: "bob" });

		assert.equal(result.messages.length, 1);
		assert.equal(result.messages[0]?.topicMessageId, m2.topicMessageId);
		assert.equal(result.cursor, m2.topicMessageId);
	});

	test("empty response keeps cursor when no new messages", () => {
		broker.register({ peerId: "alice" });
		broker.register({ peerId: "bob" });
		broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
		broker.topicSubscribe({ topicId: "ch-1", peerId: "bob" });
		const m1 = broker.topicSend({
			topicId: "ch-1",
			from: "alice",
			subject: "s1",
			body: "b1",
		});
		broker.topicMonitor({ topicId: "ch-1", peerId: "bob" });

		const result = broker.topicMonitor({ topicId: "ch-1", peerId: "bob" });

		assert.deepEqual(result.messages, []);
		assert.equal(result.cursor, m1.topicMessageId);
	});

	test("empty topic from null cursor returns empty with null cursor", () => {
		broker.register({ peerId: "alice" });
		broker.register({ peerId: "bob" });
		broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
		broker.topicSubscribe({ topicId: "ch-1", peerId: "bob" });

		const result = broker.topicMonitor({ topicId: "ch-1", peerId: "bob" });

		assert.deepEqual(result.messages, []);
		assert.equal(result.cursor, null);
	});

	test("max parameter caps returned messages", () => {
		broker.register({ peerId: "alice" });
		broker.register({ peerId: "bob" });
		broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
		broker.topicSubscribe({ topicId: "ch-1", peerId: "bob" });
		const m1 = broker.topicSend({
			topicId: "ch-1",
			from: "alice",
			subject: "s1",
			body: "b1",
		});
		broker.topicSend({
			topicId: "ch-1",
			from: "alice",
			subject: "s2",
			body: "b2",
		});
		broker.topicSend({
			topicId: "ch-1",
			from: "alice",
			subject: "s3",
			body: "b3",
		});

		const result = broker.topicMonitor({
			topicId: "ch-1",
			peerId: "bob",
			max: 1,
		});

		assert.equal(result.messages.length, 1);
		assert.equal(result.messages[0]?.topicMessageId, m1.topicMessageId);
		assert.equal(result.cursor, m1.topicMessageId);
	});

	test("max out of range throws VALIDATION_FAILED", () => {
		broker.register({ peerId: "alice" });
		broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
		broker.topicSubscribe({ topicId: "ch-1", peerId: "alice" });

		assert.throws(
			() => broker.topicMonitor({ topicId: "ch-1", peerId: "alice", max: 0 }),
			(err: unknown) =>
				err instanceof BrokerError && err.code === "VALIDATION_FAILED",
		);
		assert.throws(
			() =>
				broker.topicMonitor({ topicId: "ch-1", peerId: "alice", max: 9999 }),
			(err: unknown) =>
				err instanceof BrokerError && err.code === "VALIDATION_FAILED",
		);
	});

	test("throws TOPIC_NOT_FOUND on missing topic", () => {
		broker.register({ peerId: "alice" });

		assert.throws(
			() => broker.topicMonitor({ topicId: "ghost-ch", peerId: "alice" }),
			(err: unknown) =>
				err instanceof BrokerError && err.code === "TOPIC_NOT_FOUND",
		);
	});

	test("throws NOT_SUBSCRIBED when peer is not subscribed", () => {
		broker.register({ peerId: "alice" });
		broker.register({ peerId: "bob" });
		broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });

		assert.throws(
			() => broker.topicMonitor({ topicId: "ch-1", peerId: "bob" }),
			(err: unknown) =>
				err instanceof BrokerError && err.code === "NOT_SUBSCRIBED",
		);
	});

	test("cursor is per-subscriber: bob's monitor does not affect carol's view", () => {
		broker.register({ peerId: "alice" });
		broker.register({ peerId: "bob" });
		broker.register({ peerId: "carol" });
		broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
		broker.topicSubscribe({ topicId: "ch-1", peerId: "bob" });
		broker.topicSubscribe({ topicId: "ch-1", peerId: "carol" });
		broker.topicSend({
			topicId: "ch-1",
			from: "alice",
			subject: "s1",
			body: "b1",
		});
		broker.topicSend({
			topicId: "ch-1",
			from: "alice",
			subject: "s2",
			body: "b2",
		});

		broker.topicMonitor({ topicId: "ch-1", peerId: "bob" });
		const carolResult = broker.topicMonitor({
			topicId: "ch-1",
			peerId: "carol",
		});

		assert.equal(
			carolResult.messages.length,
			2,
			"carol should still see all 2 messages — bob's cursor is independent",
		);
	});
});
