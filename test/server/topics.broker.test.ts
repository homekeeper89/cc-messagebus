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

describe("broker topics", () => {
	let tmpDir: string;
	let dbPath: string;
	let db: CcDatabase;
	let broker: Broker;
	let clockMs: number;

	before(() => {
		tmpDir = mkdtempSync(join(tmpdir(), "ccmb-topics-broker-"));
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
		// topic history pagination 테스트의 sent_at tie-break flake 회피 위해 monotonic clock 주입.
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

	describe("topicCreate", () => {
		test("creates topic and emits topic_created", () => {
			broker.register({ peerId: "alice" });
			let receivedEvent: unknown = null;
			broker.events.on("topic_created", (e) => {
				receivedEvent = e;
			});
			const result = broker.topicCreate({
				topicId: "ch-1",
				createdBy: "alice",
			});
			assert.equal(result.topic.topicId, "ch-1");
			assert.equal(result.topic.createdBy, "alice");
			assert.ok(result.topic.createdAt);
			assert.ok(receivedEvent);
		});

		test("throws TOPIC_ALREADY_EXISTS on duplicate topicId", () => {
			broker.register({ peerId: "alice" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			assert.throws(
				() => broker.topicCreate({ topicId: "ch-1", createdBy: "alice" }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "TOPIC_ALREADY_EXISTS",
			);
		});

		test("throws PEER_NOT_FOUND when creator is not registered", () => {
			assert.throws(
				() => broker.topicCreate({ topicId: "ch-1", createdBy: "ghost" }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "PEER_NOT_FOUND",
			);
		});
	});

	describe("topicSubscribe", () => {
		test("subscribes and emits topic_subscribed", () => {
			broker.register({ peerId: "alice" });
			broker.register({ peerId: "bob" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			let receivedEvent: unknown = null;
			broker.events.on("topic_subscribed", (e) => {
				receivedEvent = e;
			});
			const result = broker.topicSubscribe({
				topicId: "ch-1",
				peerId: "bob",
			});
			assert.ok(result.subscribedAt);
			assert.ok(receivedEvent);
		});

		test("throws TOPIC_NOT_FOUND on missing topic", () => {
			broker.register({ peerId: "alice" });
			assert.throws(
				() => broker.topicSubscribe({ topicId: "ghost-ch", peerId: "alice" }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "TOPIC_NOT_FOUND",
			);
		});

		test("throws ALREADY_SUBSCRIBED on duplicate subscribe", () => {
			broker.register({ peerId: "alice" });
			broker.register({ peerId: "bob" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "bob" });
			assert.throws(
				() => broker.topicSubscribe({ topicId: "ch-1", peerId: "bob" }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "ALREADY_SUBSCRIBED",
			);
		});

		test("throws PEER_NOT_FOUND when subscriber is not registered", () => {
			broker.register({ peerId: "alice" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			assert.throws(
				() => broker.topicSubscribe({ topicId: "ch-1", peerId: "ghost" }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "PEER_NOT_FOUND",
			);
		});
	});

	describe("topicSend", () => {
		test("fan-out delivers to N-1 (sender excluded)", () => {
			broker.register({ peerId: "alice" });
			broker.register({ peerId: "bob" });
			broker.register({ peerId: "carol" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "alice" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "bob" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "carol" });

			const result = broker.topicSend({
				topicId: "ch-1",
				from: "alice",
				subject: "hi",
				body: "epic update",
			});

			assert.equal(
				result.deliveredTo.length,
				2,
				"alice (sender) must be excluded from fan-out",
			);
			assert.ok(result.deliveredTo.includes("bob"));
			assert.ok(result.deliveredTo.includes("carol"));
			assert.ok(!result.deliveredTo.includes("alice"));
		});

		test("sender inbox stays empty after self-published message", () => {
			broker.register({ peerId: "alice" });
			broker.register({ peerId: "bob" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "alice" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "bob" });

			broker.topicSend({
				topicId: "ch-1",
				from: "alice",
				subject: "hi",
				body: "epic update",
			});

			const aliceRead = broker.read({ peerId: "alice" });
			assert.equal(
				aliceRead.messages.length,
				0,
				"sender must not receive their own topic message",
			);
			const bobRead = broker.read({ peerId: "bob" });
			assert.equal(bobRead.messages.length, 1);
			assert.equal(bobRead.messages[0]?.from, "alice");
		});

		test("sender not subscribed still fan-out to all subscribers", () => {
			broker.register({ peerId: "alice" });
			broker.register({ peerId: "bob" });
			broker.register({ peerId: "carol" });
			broker.register({ peerId: "dave" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "bob" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "carol" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "dave" });

			const result = broker.topicSend({
				topicId: "ch-1",
				from: "alice",
				subject: "hi",
				body: "from outsider",
			});

			assert.equal(result.deliveredTo.length, 3);
		});

		test("empty topic returns empty deliveredTo", () => {
			broker.register({ peerId: "alice" });
			broker.topicCreate({ topicId: "ch-empty", createdBy: "alice" });

			const result = broker.topicSend({
				topicId: "ch-empty",
				from: "alice",
				subject: "hi",
				body: "nobody listening",
			});

			assert.deepEqual(result.deliveredTo, []);
			assert.ok(result.topicMessageId);
		});

		test("only sender subscribed returns empty deliveredTo", () => {
			broker.register({ peerId: "alice" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "alice" });

			const result = broker.topicSend({
				topicId: "ch-1",
				from: "alice",
				subject: "hi",
				body: "lonely",
			});

			assert.deepEqual(result.deliveredTo, []);
		});

		test("throws TOPIC_NOT_FOUND on missing topic", () => {
			broker.register({ peerId: "alice" });
			assert.throws(
				() =>
					broker.topicSend({
						topicId: "ghost-ch",
						from: "alice",
						subject: "hi",
						body: "x",
					}),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "TOPIC_NOT_FOUND",
			);
		});

		test("throws PEER_NOT_FOUND when sender is not registered", () => {
			broker.register({ peerId: "alice" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			assert.throws(
				() =>
					broker.topicSend({
						topicId: "ch-1",
						from: "ghost",
						subject: "hi",
						body: "x",
					}),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "PEER_NOT_FOUND",
			);
		});

		test("topic_message_published event matches return value", () => {
			broker.register({ peerId: "alice" });
			broker.register({ peerId: "bob" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "bob" });

			let receivedEvent: {
				topicMessageId: string;
				deliveredTo: string[];
				sentAt: string;
			} | null = null;
			broker.events.on("topic_message_published", (e) => {
				receivedEvent = e;
			});

			const result = broker.topicSend({
				topicId: "ch-1",
				from: "alice",
				subject: "hi",
				body: "x",
			});

			assert.ok(receivedEvent);
			const event = receivedEvent as unknown as {
				topicMessageId: string;
				deliveredTo: string[];
				sentAt: string;
			};
			assert.equal(event.topicMessageId, result.topicMessageId);
			assert.deepEqual(event.deliveredTo, result.deliveredTo);
			assert.equal(event.sentAt, result.sentAt);
		});

		test("fan-out emits message_sent per subscriber copy", () => {
			broker.register({ peerId: "alice" });
			broker.register({ peerId: "bob" });
			broker.register({ peerId: "carol" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "bob" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "carol" });

			const received: string[] = [];
			const kinds: string[] = [];
			const topicIds: (string | undefined)[] = [];
			broker.events.on("message_sent", (e) => {
				received.push(e.message.to);
				kinds.push(e.kind);
				topicIds.push(e.topicId);
			});

			broker.topicSend({
				topicId: "ch-1",
				from: "alice",
				subject: "hi",
				body: "x",
			});

			assert.equal(
				received.length,
				2,
				"two subscribers must each get a message_sent",
			);
			assert.ok(received.includes("bob"));
			assert.ok(received.includes("carol"));
			assert.ok(
				!received.includes("alice"),
				"sender must not receive own message_sent",
			);
			assert.ok(
				kinds.every((k) => k === "topic"),
				"fanout message_sent events must carry kind='topic' (PR-D)",
			);
			assert.ok(
				topicIds.every((t) => t === "ch-1"),
				"fanout message_sent events must carry topicId for dashboard routing (PR-D)",
			);
		});
	});

	describe("topicUnsubscribe", () => {
		test("unsubscribes and emits topic_unsubscribed", () => {
			broker.register({ peerId: "alice" });
			broker.register({ peerId: "bob" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "bob" });
			let receivedEvent: unknown = null;
			broker.events.on("topic_unsubscribed", (e) => {
				receivedEvent = e;
			});
			const result = broker.topicUnsubscribe({
				topicId: "ch-1",
				peerId: "bob",
			});
			assert.ok(result.unsubscribedAt);
			assert.ok(receivedEvent);
		});

		test("throws TOPIC_NOT_FOUND on missing topic", () => {
			broker.register({ peerId: "alice" });
			assert.throws(
				() =>
					broker.topicUnsubscribe({
						topicId: "ghost-ch",
						peerId: "alice",
					}),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "TOPIC_NOT_FOUND",
			);
		});

		test("throws NOT_SUBSCRIBED when subscription does not exist", () => {
			broker.register({ peerId: "alice" });
			broker.register({ peerId: "bob" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			assert.throws(
				() => broker.topicUnsubscribe({ topicId: "ch-1", peerId: "bob" }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "NOT_SUBSCRIBED",
			);
		});

		test("throws PEER_NOT_FOUND when peerId is not registered", () => {
			broker.register({ peerId: "alice" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			assert.throws(
				() => broker.topicUnsubscribe({ topicId: "ch-1", peerId: "ghost" }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "PEER_NOT_FOUND",
			);
		});

		test("already-delivered messages remain ackable after unsubscribe", () => {
			broker.register({ peerId: "alice" });
			broker.register({ peerId: "bob" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "bob" });
			broker.topicSend({
				topicId: "ch-1",
				from: "alice",
				subject: "hi",
				body: "before unsubscribe",
			});

			broker.topicUnsubscribe({ topicId: "ch-1", peerId: "bob" });

			const bobRead = broker.read({ peerId: "bob" });
			assert.equal(
				bobRead.messages.length,
				1,
				"prior message must still be in bob's inbox after unsubscribe",
			);
			const msgId = bobRead.messages[0]?.id;
			assert.ok(msgId);
			const ackResult = broker.ack({ peerId: "bob", messageId: msgId });
			assert.ok(ackResult.ackedAt);
		});

		test("post-unsubscribe send does not deliver to former subscriber", () => {
			broker.register({ peerId: "alice" });
			broker.register({ peerId: "bob" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "bob" });
			broker.topicUnsubscribe({ topicId: "ch-1", peerId: "bob" });

			const result = broker.topicSend({
				topicId: "ch-1",
				from: "alice",
				subject: "hi",
				body: "after unsubscribe",
			});
			assert.deepEqual(result.deliveredTo, []);
		});
	});

	describe("topicHistory", () => {
		test("returns canonical messages in DESC order with derived expiresAt", () => {
			broker.register({ peerId: "alice" });
			broker.register({ peerId: "bob" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "bob" });
			broker.topicSend({
				topicId: "ch-1",
				from: "alice",
				subject: "first",
				body: "m1",
			});
			broker.topicSend({
				topicId: "ch-1",
				from: "alice",
				subject: "second",
				body: "m2",
			});

			const result = broker.topicHistory({ topicId: "ch-1" });
			assert.equal(result.messages.length, 2);
			assert.equal(result.hasMore, false);
			assert.equal(result.messages[0]?.subject, "second", "DESC: newest first");
			assert.equal(result.messages[1]?.subject, "first");
			assert.ok(
				result.messages[0]?.expiresAt,
				"expiresAt must be derived from sentAt + ttlDays",
			);
		});

		test("paginates with hasMore via limit + 1 detection", () => {
			broker.register({ peerId: "alice" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			for (let i = 0; i < 5; i++) {
				broker.topicSend({
					topicId: "ch-1",
					from: "alice",
					subject: `s${i}`,
					body: `m${i}`,
				});
			}

			const page1 = broker.topicHistory({ topicId: "ch-1", limit: 2 });
			assert.equal(page1.messages.length, 2);
			assert.equal(page1.hasMore, true);

			const cursor = page1.messages[1]?.sentAt;
			assert.ok(cursor);
			const page2 = broker.topicHistory({
				topicId: "ch-1",
				limit: 2,
				beforeSentAt: cursor,
			});
			assert.equal(page2.messages.length, 2);
			assert.equal(page2.hasMore, true);

			const cursor2 = page2.messages[1]?.sentAt;
			assert.ok(cursor2);
			const page3 = broker.topicHistory({
				topicId: "ch-1",
				limit: 2,
				beforeSentAt: cursor2,
			});
			assert.equal(page3.messages.length, 1);
			assert.equal(page3.hasMore, false);
		});

		test("throws TOPIC_NOT_FOUND on missing topic", () => {
			assert.throws(
				() => broker.topicHistory({ topicId: "ghost-ch" }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "TOPIC_NOT_FOUND",
			);
		});

		test("empty topic returns empty messages and hasMore=false", () => {
			broker.register({ peerId: "alice" });
			broker.topicCreate({ topicId: "ch-empty", createdBy: "alice" });
			const result = broker.topicHistory({ topicId: "ch-empty" });
			assert.deepEqual(result.messages, []);
			assert.equal(result.hasMore, false);
		});

		test("rejects out-of-range limit", () => {
			broker.register({ peerId: "alice" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			assert.throws(
				() => broker.topicHistory({ topicId: "ch-1", limit: 0 }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "VALIDATION_FAILED",
			);
			assert.throws(
				() => broker.topicHistory({ topicId: "ch-1", limit: 999 }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "VALIDATION_FAILED",
			);
		});
	});

	describe("topicArchive", () => {
		test("sets archivedAt and emits topic_archived", () => {
			broker.register({ peerId: "alice" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			let receivedEvent: { topicId: string; archivedAt: string } | null = null;
			broker.events.on("topic_archived", (e) => {
				receivedEvent = e as { topicId: string; archivedAt: string };
			});

			const result = broker.topicArchive({ topicId: "ch-1" });
			assert.ok(result.archivedAt);
			assert.ok(receivedEvent);
			const event = receivedEvent as unknown as {
				topicId: string;
				archivedAt: string;
			};
			assert.equal(event.topicId, "ch-1");
			assert.equal(event.archivedAt, result.archivedAt);
		});

		test("list_topics returns archivedAt after archive", () => {
			broker.register({ peerId: "alice" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			broker.topicCreate({ topicId: "ch-2", createdBy: "alice" });
			broker.topicArchive({ topicId: "ch-1" });

			const list = broker.listTopics();
			const ch1 = list.topics.find((t) => t.topicId === "ch-1");
			const ch2 = list.topics.find((t) => t.topicId === "ch-2");
			assert.ok(ch1?.archivedAt, "archived topic must expose archivedAt");
			assert.equal(
				ch2?.archivedAt,
				null,
				"non-archived topic must have archivedAt=null",
			);
		});

		test("send still works on archived topic (hidden-only semantics)", () => {
			broker.register({ peerId: "alice" });
			broker.register({ peerId: "bob" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			broker.topicSubscribe({ topicId: "ch-1", peerId: "bob" });
			broker.topicArchive({ topicId: "ch-1" });

			const result = broker.topicSend({
				topicId: "ch-1",
				from: "alice",
				subject: "hi",
				body: "after archive",
			});
			assert.equal(
				result.deliveredTo.length,
				1,
				"archive must not block fan-out (UI-hidden only)",
			);
			assert.ok(result.deliveredTo.includes("bob"));
		});

		test("throws TOPIC_NOT_FOUND on missing topic", () => {
			assert.throws(
				() => broker.topicArchive({ topicId: "ghost-ch" }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "TOPIC_NOT_FOUND",
			);
		});

		test("re-archive is idempotent (no throw)", () => {
			broker.register({ peerId: "alice" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			broker.topicArchive({ topicId: "ch-1" });
			const second = broker.topicArchive({ topicId: "ch-1" });
			assert.ok(
				second.archivedAt,
				"re-archive must return archivedAt without throwing",
			);
		});
	});

	describe("topicUnarchive", () => {
		test("clears archivedAt and emits topic_unarchived", () => {
			broker.register({ peerId: "alice" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			broker.topicArchive({ topicId: "ch-1" });
			let receivedEvent: { topicId: string; at: string } | null = null;
			broker.events.on("topic_unarchived", (e) => {
				receivedEvent = e as { topicId: string; at: string };
			});

			const result = broker.topicUnarchive({ topicId: "ch-1" });
			assert.ok(result.unarchivedAt);
			assert.ok(receivedEvent);
			const event = receivedEvent as unknown as {
				topicId: string;
				at: string;
			};
			assert.equal(event.topicId, "ch-1");
			assert.equal(event.at, result.unarchivedAt);

			const list = broker.listTopics();
			const ch1 = list.topics.find((t) => t.topicId === "ch-1");
			assert.equal(
				ch1?.archivedAt,
				null,
				"archivedAt must be null after unarchive",
			);
		});

		test("throws TOPIC_NOT_FOUND on missing topic", () => {
			assert.throws(
				() => broker.topicUnarchive({ topicId: "ghost-ch" }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "TOPIC_NOT_FOUND",
			);
		});

		test("unarchive on non-archived topic is idempotent (no throw)", () => {
			broker.register({ peerId: "alice" });
			broker.topicCreate({ topicId: "ch-1", createdBy: "alice" });
			const result = broker.topicUnarchive({ topicId: "ch-1" });
			assert.ok(
				result.unarchivedAt,
				"unarchive on already-active topic must return unarchivedAt without throwing",
			);
		});
	});
});
