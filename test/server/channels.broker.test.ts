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

describe("broker channels", () => {
	let tmpDir: string;
	let dbPath: string;
	let db: CcDatabase;
	let broker: Broker;
	let clockMs: number;

	before(() => {
		tmpDir = mkdtempSync(join(tmpdir(), "ccmb-channels-broker-"));
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
		// channel history pagination 테스트의 sent_at tie-break flake 회피 위해 monotonic clock 주입.
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

	describe("channelCreate", () => {
		test("creates channel and emits channel_created", () => {
			broker.register({ topicId: "alice" });
			let receivedEvent: unknown = null;
			broker.events.on("channel_created", (e) => {
				receivedEvent = e;
			});
			const result = broker.channelCreate({
				channelId: "ch-1",
				createdBy: "alice",
			});
			assert.equal(result.channel.channelId, "ch-1");
			assert.equal(result.channel.createdBy, "alice");
			assert.ok(result.channel.createdAt);
			assert.ok(receivedEvent);
		});

		test("throws CHANNEL_ALREADY_EXISTS on duplicate channelId", () => {
			broker.register({ topicId: "alice" });
			broker.channelCreate({ channelId: "ch-1", createdBy: "alice" });
			assert.throws(
				() => broker.channelCreate({ channelId: "ch-1", createdBy: "alice" }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "CHANNEL_ALREADY_EXISTS",
			);
		});

		test("throws TOPIC_NOT_FOUND when creator is not registered", () => {
			assert.throws(
				() => broker.channelCreate({ channelId: "ch-1", createdBy: "ghost" }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "TOPIC_NOT_FOUND",
			);
		});
	});

	describe("channelSubscribe", () => {
		test("subscribes and emits channel_subscribed", () => {
			broker.register({ topicId: "alice" });
			broker.register({ topicId: "bob" });
			broker.channelCreate({ channelId: "ch-1", createdBy: "alice" });
			let receivedEvent: unknown = null;
			broker.events.on("channel_subscribed", (e) => {
				receivedEvent = e;
			});
			const result = broker.channelSubscribe({
				channelId: "ch-1",
				topicId: "bob",
			});
			assert.ok(result.subscribedAt);
			assert.ok(receivedEvent);
		});

		test("throws CHANNEL_NOT_FOUND on missing channel", () => {
			broker.register({ topicId: "alice" });
			assert.throws(
				() =>
					broker.channelSubscribe({ channelId: "ghost-ch", topicId: "alice" }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "CHANNEL_NOT_FOUND",
			);
		});

		test("throws ALREADY_SUBSCRIBED on duplicate subscribe", () => {
			broker.register({ topicId: "alice" });
			broker.register({ topicId: "bob" });
			broker.channelCreate({ channelId: "ch-1", createdBy: "alice" });
			broker.channelSubscribe({ channelId: "ch-1", topicId: "bob" });
			assert.throws(
				() => broker.channelSubscribe({ channelId: "ch-1", topicId: "bob" }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "ALREADY_SUBSCRIBED",
			);
		});

		test("throws TOPIC_NOT_FOUND when subscriber is not registered", () => {
			broker.register({ topicId: "alice" });
			broker.channelCreate({ channelId: "ch-1", createdBy: "alice" });
			assert.throws(
				() => broker.channelSubscribe({ channelId: "ch-1", topicId: "ghost" }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "TOPIC_NOT_FOUND",
			);
		});
	});

	describe("channelSend", () => {
		test("fan-out delivers to N-1 (sender excluded)", () => {
			broker.register({ topicId: "alice" });
			broker.register({ topicId: "bob" });
			broker.register({ topicId: "carol" });
			broker.channelCreate({ channelId: "ch-1", createdBy: "alice" });
			broker.channelSubscribe({ channelId: "ch-1", topicId: "alice" });
			broker.channelSubscribe({ channelId: "ch-1", topicId: "bob" });
			broker.channelSubscribe({ channelId: "ch-1", topicId: "carol" });

			const result = broker.channelSend({
				channelId: "ch-1",
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
			broker.register({ topicId: "alice" });
			broker.register({ topicId: "bob" });
			broker.channelCreate({ channelId: "ch-1", createdBy: "alice" });
			broker.channelSubscribe({ channelId: "ch-1", topicId: "alice" });
			broker.channelSubscribe({ channelId: "ch-1", topicId: "bob" });

			broker.channelSend({
				channelId: "ch-1",
				from: "alice",
				subject: "hi",
				body: "epic update",
			});

			const aliceRead = broker.read({ topicId: "alice" });
			assert.equal(
				aliceRead.messages.length,
				0,
				"sender must not receive their own channel message",
			);
			const bobRead = broker.read({ topicId: "bob" });
			assert.equal(bobRead.messages.length, 1);
			assert.equal(bobRead.messages[0]?.from, "alice");
		});

		test("sender not subscribed still fan-out to all subscribers", () => {
			broker.register({ topicId: "alice" });
			broker.register({ topicId: "bob" });
			broker.register({ topicId: "carol" });
			broker.register({ topicId: "dave" });
			broker.channelCreate({ channelId: "ch-1", createdBy: "alice" });
			broker.channelSubscribe({ channelId: "ch-1", topicId: "bob" });
			broker.channelSubscribe({ channelId: "ch-1", topicId: "carol" });
			broker.channelSubscribe({ channelId: "ch-1", topicId: "dave" });

			const result = broker.channelSend({
				channelId: "ch-1",
				from: "alice",
				subject: "hi",
				body: "from outsider",
			});

			assert.equal(result.deliveredTo.length, 3);
		});

		test("empty channel returns empty deliveredTo", () => {
			broker.register({ topicId: "alice" });
			broker.channelCreate({ channelId: "ch-empty", createdBy: "alice" });

			const result = broker.channelSend({
				channelId: "ch-empty",
				from: "alice",
				subject: "hi",
				body: "nobody listening",
			});

			assert.deepEqual(result.deliveredTo, []);
			assert.ok(result.channelMessageId);
		});

		test("only sender subscribed returns empty deliveredTo", () => {
			broker.register({ topicId: "alice" });
			broker.channelCreate({ channelId: "ch-1", createdBy: "alice" });
			broker.channelSubscribe({ channelId: "ch-1", topicId: "alice" });

			const result = broker.channelSend({
				channelId: "ch-1",
				from: "alice",
				subject: "hi",
				body: "lonely",
			});

			assert.deepEqual(result.deliveredTo, []);
		});

		test("throws CHANNEL_NOT_FOUND on missing channel", () => {
			broker.register({ topicId: "alice" });
			assert.throws(
				() =>
					broker.channelSend({
						channelId: "ghost-ch",
						from: "alice",
						subject: "hi",
						body: "x",
					}),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "CHANNEL_NOT_FOUND",
			);
		});

		test("throws TOPIC_NOT_FOUND when sender is not registered", () => {
			broker.register({ topicId: "alice" });
			broker.channelCreate({ channelId: "ch-1", createdBy: "alice" });
			assert.throws(
				() =>
					broker.channelSend({
						channelId: "ch-1",
						from: "ghost",
						subject: "hi",
						body: "x",
					}),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "TOPIC_NOT_FOUND",
			);
		});

		test("channel_message_published event matches return value", () => {
			broker.register({ topicId: "alice" });
			broker.register({ topicId: "bob" });
			broker.channelCreate({ channelId: "ch-1", createdBy: "alice" });
			broker.channelSubscribe({ channelId: "ch-1", topicId: "bob" });

			let receivedEvent: {
				channelMessageId: string;
				deliveredTo: string[];
				sentAt: string;
			} | null = null;
			broker.events.on("channel_message_published", (e) => {
				receivedEvent = e;
			});

			const result = broker.channelSend({
				channelId: "ch-1",
				from: "alice",
				subject: "hi",
				body: "x",
			});

			assert.ok(receivedEvent);
			const event = receivedEvent as unknown as {
				channelMessageId: string;
				deliveredTo: string[];
				sentAt: string;
			};
			assert.equal(event.channelMessageId, result.channelMessageId);
			assert.deepEqual(event.deliveredTo, result.deliveredTo);
			assert.equal(event.sentAt, result.sentAt);
		});

		test("fan-out emits message_sent per subscriber copy", () => {
			broker.register({ topicId: "alice" });
			broker.register({ topicId: "bob" });
			broker.register({ topicId: "carol" });
			broker.channelCreate({ channelId: "ch-1", createdBy: "alice" });
			broker.channelSubscribe({ channelId: "ch-1", topicId: "bob" });
			broker.channelSubscribe({ channelId: "ch-1", topicId: "carol" });

			const received: string[] = [];
			broker.events.on("message_sent", (e) => {
				received.push(e.message.to);
			});

			broker.channelSend({
				channelId: "ch-1",
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
		});
	});

	describe("channelUnsubscribe", () => {
		test("unsubscribes and emits channel_unsubscribed", () => {
			broker.register({ topicId: "alice" });
			broker.register({ topicId: "bob" });
			broker.channelCreate({ channelId: "ch-1", createdBy: "alice" });
			broker.channelSubscribe({ channelId: "ch-1", topicId: "bob" });
			let receivedEvent: unknown = null;
			broker.events.on("channel_unsubscribed", (e) => {
				receivedEvent = e;
			});
			const result = broker.channelUnsubscribe({
				channelId: "ch-1",
				topicId: "bob",
			});
			assert.ok(result.unsubscribedAt);
			assert.ok(receivedEvent);
		});

		test("throws CHANNEL_NOT_FOUND on missing channel", () => {
			broker.register({ topicId: "alice" });
			assert.throws(
				() =>
					broker.channelUnsubscribe({
						channelId: "ghost-ch",
						topicId: "alice",
					}),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "CHANNEL_NOT_FOUND",
			);
		});

		test("throws NOT_SUBSCRIBED when subscription does not exist", () => {
			broker.register({ topicId: "alice" });
			broker.register({ topicId: "bob" });
			broker.channelCreate({ channelId: "ch-1", createdBy: "alice" });
			assert.throws(
				() => broker.channelUnsubscribe({ channelId: "ch-1", topicId: "bob" }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "NOT_SUBSCRIBED",
			);
		});

		test("throws TOPIC_NOT_FOUND when topicId is not registered", () => {
			broker.register({ topicId: "alice" });
			broker.channelCreate({ channelId: "ch-1", createdBy: "alice" });
			assert.throws(
				() =>
					broker.channelUnsubscribe({ channelId: "ch-1", topicId: "ghost" }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "TOPIC_NOT_FOUND",
			);
		});

		test("already-delivered messages remain ackable after unsubscribe", () => {
			broker.register({ topicId: "alice" });
			broker.register({ topicId: "bob" });
			broker.channelCreate({ channelId: "ch-1", createdBy: "alice" });
			broker.channelSubscribe({ channelId: "ch-1", topicId: "bob" });
			broker.channelSend({
				channelId: "ch-1",
				from: "alice",
				subject: "hi",
				body: "before unsubscribe",
			});

			broker.channelUnsubscribe({ channelId: "ch-1", topicId: "bob" });

			const bobRead = broker.read({ topicId: "bob" });
			assert.equal(
				bobRead.messages.length,
				1,
				"prior message must still be in bob's inbox after unsubscribe",
			);
			const msgId = bobRead.messages[0]?.id;
			assert.ok(msgId);
			const ackResult = broker.ack({ topicId: "bob", messageId: msgId });
			assert.ok(ackResult.ackedAt);
		});

		test("post-unsubscribe send does not deliver to former subscriber", () => {
			broker.register({ topicId: "alice" });
			broker.register({ topicId: "bob" });
			broker.channelCreate({ channelId: "ch-1", createdBy: "alice" });
			broker.channelSubscribe({ channelId: "ch-1", topicId: "bob" });
			broker.channelUnsubscribe({ channelId: "ch-1", topicId: "bob" });

			const result = broker.channelSend({
				channelId: "ch-1",
				from: "alice",
				subject: "hi",
				body: "after unsubscribe",
			});
			assert.deepEqual(result.deliveredTo, []);
		});
	});

	describe("channelHistory", () => {
		test("returns canonical messages in DESC order with derived expiresAt", () => {
			broker.register({ topicId: "alice" });
			broker.register({ topicId: "bob" });
			broker.channelCreate({ channelId: "ch-1", createdBy: "alice" });
			broker.channelSubscribe({ channelId: "ch-1", topicId: "bob" });
			broker.channelSend({
				channelId: "ch-1",
				from: "alice",
				subject: "first",
				body: "m1",
			});
			broker.channelSend({
				channelId: "ch-1",
				from: "alice",
				subject: "second",
				body: "m2",
			});

			const result = broker.channelHistory({ channelId: "ch-1" });
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
			broker.register({ topicId: "alice" });
			broker.channelCreate({ channelId: "ch-1", createdBy: "alice" });
			for (let i = 0; i < 5; i++) {
				broker.channelSend({
					channelId: "ch-1",
					from: "alice",
					subject: `s${i}`,
					body: `m${i}`,
				});
			}

			const page1 = broker.channelHistory({ channelId: "ch-1", limit: 2 });
			assert.equal(page1.messages.length, 2);
			assert.equal(page1.hasMore, true);

			const cursor = page1.messages[1]?.sentAt;
			assert.ok(cursor);
			const page2 = broker.channelHistory({
				channelId: "ch-1",
				limit: 2,
				beforeSentAt: cursor,
			});
			assert.equal(page2.messages.length, 2);
			assert.equal(page2.hasMore, true);

			const cursor2 = page2.messages[1]?.sentAt;
			assert.ok(cursor2);
			const page3 = broker.channelHistory({
				channelId: "ch-1",
				limit: 2,
				beforeSentAt: cursor2,
			});
			assert.equal(page3.messages.length, 1);
			assert.equal(page3.hasMore, false);
		});

		test("throws CHANNEL_NOT_FOUND on missing channel", () => {
			assert.throws(
				() => broker.channelHistory({ channelId: "ghost-ch" }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "CHANNEL_NOT_FOUND",
			);
		});

		test("empty channel returns empty messages and hasMore=false", () => {
			broker.register({ topicId: "alice" });
			broker.channelCreate({ channelId: "ch-empty", createdBy: "alice" });
			const result = broker.channelHistory({ channelId: "ch-empty" });
			assert.deepEqual(result.messages, []);
			assert.equal(result.hasMore, false);
		});

		test("rejects out-of-range limit", () => {
			broker.register({ topicId: "alice" });
			broker.channelCreate({ channelId: "ch-1", createdBy: "alice" });
			assert.throws(
				() => broker.channelHistory({ channelId: "ch-1", limit: 0 }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "VALIDATION_FAILED",
			);
			assert.throws(
				() => broker.channelHistory({ channelId: "ch-1", limit: 999 }),
				(err: unknown) =>
					err instanceof BrokerError && err.code === "VALIDATION_FAILED",
			);
		});
	});
});
