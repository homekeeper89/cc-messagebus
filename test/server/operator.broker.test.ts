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

describe("broker operator RPC", () => {
	let tmpDir: string;
	let dbPath: string;
	let db: CcDatabase;
	let broker: Broker;
	let clockMs: number;

	before(() => {
		tmpDir = mkdtempSync(join(tmpdir(), "ccmb-operator-broker-"));
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

	describe("channelBroadcast", () => {
		test("operator (unregistered sender) can broadcast to subscribers", () => {
			broker.register({ peerId: "alice" });
			broker.register({ peerId: "bob" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "alice" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "bob" });

			const result = broker.channelBroadcast({
				topicId: "ch-1",
				from: "operator",
				subject: "notice",
				body: "system message",
			});

			assert.equal(result.deliveredTo.length, 2);
			assert.ok(result.deliveredTo.includes("alice"));
			assert.ok(result.deliveredTo.includes("bob"));
			assert.ok(result.topicMessageId);
			assert.ok(result.sentAt);
		});

		test("delivers to all subscribers (no sender exclusion when sender is virtual)", () => {
			broker.register({ peerId: "alice" });
			broker.register({ peerId: "bob" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "alice" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "bob" });

			broker.channelBroadcast({
				topicId: "ch-1",
				from: "operator",
				subject: "hi",
				body: "x",
			});

			const aliceRead = broker.read({ peerId: "alice" });
			const bobRead = broker.read({ peerId: "bob" });
			assert.equal(aliceRead.messages.length, 1);
			assert.equal(bobRead.messages.length, 1);
			assert.equal(aliceRead.messages[0]?.from, "operator");
		});

		test("empty topic returns empty deliveredTo", () => {
			broker.register({ peerId: "alice" });
			broker.topicCreate({ topicId: "ch-empty", createdBy: "alice" });

			const result = broker.channelBroadcast({
				topicId: "ch-empty",
				from: "operator",
				subject: "hi",
				body: "nobody",
			});

			assert.deepEqual(result.deliveredTo, []);
			assert.ok(result.topicMessageId);
		});

		test("throws TOPIC_NOT_FOUND on missing topic", () => {
			assert.throws(
				() =>
					broker.channelBroadcast({
						topicId: "ghost-ch",
						from: "operator",
						subject: "hi",
						body: "x",
					}),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "TOPIC_NOT_FOUND",
			);
		});

		test("emits message_sent (kind='topic') per subscriber and topic_message_published", () => {
			broker.register({ peerId: "alice" });
			broker.register({ peerId: "bob" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "alice" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "bob" });

			const fanoutTargets: string[] = [];
			const kinds: string[] = [];
			let publishedEvent: { topicId: string; from: string } | null = null;
			broker.events.on("message_sent", (e) => {
				fanoutTargets.push(e.message.to);
				kinds.push(e.kind);
			});
			broker.events.on("topic_message_published", (e) => {
				publishedEvent = e as { topicId: string; from: string };
			});

			broker.channelBroadcast({
				topicId: "ch-1",
				from: "operator",
				subject: "hi",
				body: "x",
			});

			assert.equal(fanoutTargets.length, 2);
			assert.ok(fanoutTargets.includes("alice"));
			assert.ok(fanoutTargets.includes("bob"));
			assert.ok(kinds.every((k) => k === "topic"));
			assert.ok(publishedEvent);
			const ev = publishedEvent as unknown as {
				topicId: string;
				from: string;
			};
			assert.equal(ev.topicId, "ch-1");
			assert.equal(ev.from, "operator");
		});
	});

	describe("channelDelete", () => {
		test("cascades subscriptions and fanout messages", () => {
			broker.register({ peerId: "alice" });
			broker.register({ peerId: "bob" });
			broker.register({ peerId: "carol" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "alice" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "bob" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "carol" });
			broker.topicSend({
				topicId: "ch-1",
				from: "alice",
				subject: "m1",
				body: "x",
			});
			broker.topicSend({
				topicId: "ch-1",
				from: "alice",
				subject: "m2",
				body: "y",
			});

			const result = broker.channelDelete({
				topicId: "ch-1",
				confirm: "ch-1",
			});

			assert.equal(result.deletedSubs, 3);
			// 2 messages × 2 subscribers (bob, carol; alice 는 sender 제외) = 4
			assert.equal(result.deletedMessages, 4);

			assert.throws(
				() => broker.topicHistory({ topicId: "ch-1" }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "TOPIC_NOT_FOUND",
			);
			assert.equal(broker.read({ peerId: "bob" }).messages.length, 0);
			assert.equal(broker.read({ peerId: "carol" }).messages.length, 0);
		});

		test("throws VALIDATION_FAILED when confirm does not match topicId", () => {
			broker.register({ peerId: "alice" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });

			assert.throws(
				() => broker.channelDelete({ topicId: "ch-1", confirm: "ch-2" }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "VALIDATION_FAILED",
			);
			const detail = broker.topicDetail({ topicId: "ch-1" });
			assert.equal(detail.topic.topicId, "ch-1");
		});

		test("throws TOPIC_NOT_FOUND on missing topic", () => {
			assert.throws(
				() =>
					broker.channelDelete({ topicId: "ghost-ch", confirm: "ghost-ch" }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "TOPIC_NOT_FOUND",
			);
		});

		test("emits topic_deleted event", () => {
			broker.register({ peerId: "alice" });
			broker.register({ peerId: "bob" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "bob" });
			broker.topicSend({
				topicId: "ch-1",
				from: "alice",
				subject: "m1",
				body: "x",
			});

			let event: {
				topicId: string;
				deletedMessages: number;
				deletedSubs: number;
			} | null = null;
			broker.events.on("topic_deleted", (e) => {
				event = e as typeof event;
			});

			broker.channelDelete({ topicId: "ch-1", confirm: "ch-1" });
			assert.ok(event);
			const ev = event as unknown as {
				topicId: string;
				deletedMessages: number;
				deletedSubs: number;
			};
			assert.equal(ev.topicId, "ch-1");
			assert.equal(ev.deletedSubs, 1);
			assert.equal(ev.deletedMessages, 1);
		});
	});

	describe("peerDelete", () => {
		test("removes subscriptions, inbox, and session", () => {
			broker.register({ peerId: "alice" });
			broker.register({ peerId: "bob" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "bob" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "alice" });
			broker.topicSend({
				topicId: "ch-1",
				from: "alice",
				subject: "m1",
				body: "x",
			});

			assert.equal(broker.read({ peerId: "bob" }).messages.length, 1);

			const result = broker.peerDelete({ peerId: "bob", confirm: "bob" });
			assert.equal(result.deletedSubs, 1);

			broker.register({ peerId: "bob" });
			assert.equal(broker.read({ peerId: "bob" }).messages.length, 0);

			const detail = broker.topicDetail({ topicId: "ch-1" });
			assert.equal(detail.topic.subscribers.length, 1);
			assert.equal(detail.topic.subscribers[0]?.peerId, "alice");
		});

		test("counts in-flight messages cancelled", () => {
			broker.register({ peerId: "alice" });
			broker.register({ peerId: "bob" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "bob" });
			broker.topicSend({
				topicId: "ch-1",
				from: "alice",
				subject: "m1",
				body: "x",
			});
			broker.topicSend({
				topicId: "ch-1",
				from: "alice",
				subject: "m2",
				body: "y",
			});

			broker.read({ peerId: "bob" });

			const result = broker.peerDelete({ peerId: "bob", confirm: "bob" });
			assert.equal(result.cancelledInflight, 2);
		});

		test("throws VALIDATION_FAILED when confirm does not match peerId", () => {
			broker.register({ peerId: "alice" });
			assert.throws(
				() => broker.peerDelete({ peerId: "alice", confirm: "bob" }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "VALIDATION_FAILED",
			);
			const peers = broker.listPeers().peers;
			assert.ok(peers.some((p) => p.peerId === "alice"));
		});

		test("throws PEER_NOT_FOUND on missing peer", () => {
			assert.throws(
				() => broker.peerDelete({ peerId: "ghost", confirm: "ghost" }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "PEER_NOT_FOUND",
			);
		});

		test("emits peer_deleted event", () => {
			broker.register({ peerId: "alice" });
			broker.register({ peerId: "bob" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "bob" });

			let event: {
				peerId: string;
				deletedSubs: number;
				cancelledInflight: number;
			} | null = null;
			broker.events.on("peer_deleted", (e) => {
				event = e as typeof event;
			});

			broker.peerDelete({ peerId: "bob", confirm: "bob" });
			assert.ok(event);
			const ev = event as unknown as {
				peerId: string;
				deletedSubs: number;
				cancelledInflight: number;
			};
			assert.equal(ev.peerId, "bob");
			assert.equal(ev.deletedSubs, 1);
			assert.equal(ev.cancelledInflight, 0);
		});
	});
});
