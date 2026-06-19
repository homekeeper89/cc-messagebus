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
		broker = createBroker(db, {
			visibilityTimeoutSec: 30,
			ttlDays: 30,
			dashboardUrl: "http://localhost:5959",
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
	});
});
