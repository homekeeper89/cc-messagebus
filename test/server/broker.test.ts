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

describe("broker", () => {
	let tmpDir: string;
	let dbPath: string;
	let db: CcDatabase;
	let broker: Broker;

	before(() => {
		tmpDir = mkdtempSync(join(tmpdir(), "ccmb-broker-"));
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

	test("register returns monitorCommand and emits session_registered", () => {
		let receivedEvent: unknown = null;
		broker.events.on("session_registered", (e) => {
			receivedEvent = e;
		});
		const result = broker.register({ topicId: "alice" });
		assert.equal(result.topicId, "alice");
		assert.equal(result.monitorCommand, "cc-messagebus tail alice");
		assert.equal(result.dashboardUrl, "http://localhost:5959");
		assert.ok(receivedEvent);
	});

	test("register throws BrokerError on connected duplicate", () => {
		broker.register({ topicId: "alice" });
		assert.throws(
			() => broker.register({ topicId: "alice" }),
			(err: unknown) =>
				err instanceof BrokerError && err.code === "TOPIC_ALREADY_REGISTERED",
		);
	});

	test("send: target not registered throws PEER_NOT_FOUND", () => {
		broker.register({ topicId: "alice" });
		assert.throws(
			() =>
				broker.send({
					from: "alice",
					to: "ghost",
					subject: "hi",
					body: "x",
				}),
			(err: unknown) =>
				err instanceof BrokerError && err.code === "PEER_NOT_FOUND",
		);
	});

	test("send to disconnected target still queues", () => {
		broker.register({ topicId: "alice" });
		broker.register({ topicId: "bob" });
		broker.disconnect("bob");
		const result = broker.send({
			from: "alice",
			to: "bob",
			subject: "hi",
			body: "x",
		});
		assert.ok(result.messageId);
		const peers = broker.listPeers().peers;
		const bob = peers.find((p) => p.topicId === "bob");
		assert.equal(bob?.queueLength, 1);
		assert.equal(bob?.status, "disconnected");
	});

	test("end-to-end: register/send/read/ack flow", () => {
		broker.register({ topicId: "alice" });
		broker.register({ topicId: "bob" });
		const sent = broker.send({
			from: "alice",
			to: "bob",
			subject: "ping",
			body: "hello",
		});
		const read = broker.read({ topicId: "bob" });
		assert.equal(read.messages.length, 1);
		assert.equal(read.messages[0]?.id, sent.messageId);
		assert.equal(read.messages[0]?.subject, "ping");
		assert.ok(read.messages[0]?.inFlightUntil);

		const acked = broker.ack({ topicId: "bob", messageId: sent.messageId });
		assert.ok(acked.ackedAt);

		const reread = broker.read({ topicId: "bob" });
		assert.equal(reread.messages.length, 0);
	});

	test("ack: already-acked message throws MESSAGE_NOT_IN_FLIGHT", () => {
		broker.register({ topicId: "alice" });
		broker.register({ topicId: "bob" });
		const sent = broker.send({
			from: "alice",
			to: "bob",
			subject: "s",
			body: "b",
		});
		broker.read({ topicId: "bob" });
		broker.ack({ topicId: "bob", messageId: sent.messageId });
		assert.throws(
			() => broker.ack({ topicId: "bob", messageId: sent.messageId }),
			(err: unknown) =>
				err instanceof BrokerError && err.code === "MESSAGE_NOT_IN_FLIGHT",
		);
	});

	test("ack: unknown messageId throws MESSAGE_NOT_FOUND", () => {
		broker.register({ topicId: "bob" });
		assert.throws(
			() => broker.ack({ topicId: "bob", messageId: "nope" }),
			(err: unknown) =>
				err instanceof BrokerError && err.code === "MESSAGE_NOT_FOUND",
		);
	});

	test("read without ack: short visibility timeout causes redelivery", () => {
		const shortBroker = createBroker(db, {
			visibilityTimeoutSec: -1,
			ttlDays: 30,
			dashboardUrl: "http://localhost:5959",
		});
		shortBroker.register({ topicId: "alice" });
		shortBroker.register({ topicId: "bob" });
		shortBroker.send({ from: "alice", to: "bob", subject: "s", body: "b" });
		const first = shortBroker.read({ topicId: "bob" });
		assert.equal(first.messages.length, 1);
		const second = shortBroker.read({ topicId: "bob" });
		assert.equal(
			second.messages.length,
			1,
			"expired in-flight window allows immediate redelivery",
		);
	});

	test("unregister: throws TOPIC_NOT_FOUND for unknown topic", () => {
		assert.throws(
			() => broker.unregister("ghost", { topicId: "ghost" }),
			(err: unknown) =>
				err instanceof BrokerError && err.code === "TOPIC_NOT_FOUND",
		);
	});

	test("unregister: purgeQueue=false preserves messages", () => {
		broker.register({ topicId: "alice" });
		broker.register({ topicId: "bob" });
		broker.send({ from: "alice", to: "bob", subject: "s", body: "b" });
		const result = broker.unregister("bob", {
			topicId: "bob",
			purgeQueue: false,
		});
		assert.equal(result.purged, false);
		broker.register({ topicId: "bob" });
		const peers = broker.listPeers().peers;
		const bob = peers.find((p) => p.topicId === "bob");
		assert.equal(bob?.queueLength, 1, "queued message survives unregister");
	});

	test("unregister: purgeQueue=true drops queued messages", () => {
		broker.register({ topicId: "alice" });
		broker.register({ topicId: "bob" });
		broker.send({ from: "alice", to: "bob", subject: "s", body: "b" });
		const result = broker.unregister("bob", {
			topicId: "bob",
			purgeQueue: true,
		});
		assert.equal(result.purged, true);
		broker.register({ topicId: "bob" });
		const peers = broker.listPeers().peers;
		const bob = peers.find((p) => p.topicId === "bob");
		assert.equal(bob?.queueLength, 0, "queue purged");
	});

	test("unregister: emits session_disconnected", () => {
		broker.register({ topicId: "alice" });
		let received: unknown = null;
		broker.events.on("session_disconnected", (e) => {
			received = e;
		});
		broker.unregister("alice", { topicId: "alice" });
		assert.ok(received);
	});

	test("listener throw does not crash broker.emit", () => {
		broker.events.on("session_registered", () => {
			throw new Error("listener fail");
		});
		assert.doesNotThrow(() => broker.register({ topicId: "alice" }));
	});

	describe("last_activity_at tracking", () => {
		const FIXED_NOW = "2026-06-20T00:00:00.000Z";
		let activityBroker: Broker;

		beforeEach(() => {
			activityBroker = createBroker(db, {
				visibilityTimeoutSec: 30,
				ttlDays: 30,
				dashboardUrl: "http://localhost:5959",
				clock: () => FIXED_NOW,
			});
		});

		test("send_should_update_last_activity_at_for_sender", () => {
			activityBroker.register({ topicId: "alice" });
			activityBroker.register({ topicId: "bob" });
			activityBroker.send({
				from: "alice",
				to: "bob",
				subject: "s",
				body: "b",
			});
			assert.equal(db.inspectLastActivityAt("alice"), FIXED_NOW);
		});

		test("read_should_update_last_activity_at_for_reader_even_when_empty", () => {
			activityBroker.register({ topicId: "bob" });
			const result = activityBroker.read({ topicId: "bob" });
			assert.equal(result.messages.length, 0);
			assert.equal(
				db.inspectLastActivityAt("bob"),
				FIXED_NOW,
				"polling read counts as activity",
			);
		});

		test("ack_should_update_last_activity_at_for_acker", () => {
			activityBroker.register({ topicId: "alice" });
			activityBroker.register({ topicId: "bob" });
			const sent = activityBroker.send({
				from: "alice",
				to: "bob",
				subject: "s",
				body: "b",
			});
			activityBroker.read({ topicId: "bob" });
			activityBroker.ack({ topicId: "bob", messageId: sent.messageId });
			assert.equal(db.inspectLastActivityAt("bob"), FIXED_NOW);
		});

		test("channelSubscribe_should_update_last_activity_at_for_subscriber", () => {
			activityBroker.register({ topicId: "alice" });
			activityBroker.channelCreate({
				channelId: "epic-1",
				createdBy: "alice",
			});
			activityBroker.register({ topicId: "bob" });
			activityBroker.channelSubscribe({
				channelId: "epic-1",
				topicId: "bob",
			});
			assert.equal(db.inspectLastActivityAt("bob"), FIXED_NOW);
		});

		test("channelSend_should_update_last_activity_at_for_sender_only", () => {
			activityBroker.register({ topicId: "alice" });
			activityBroker.channelCreate({
				channelId: "epic-1",
				createdBy: "alice",
			});
			activityBroker.register({ topicId: "bob" });
			activityBroker.channelSubscribe({
				channelId: "epic-1",
				topicId: "bob",
			});

			const bobBefore = db.inspectLastActivityAt("bob");
			activityBroker.channelSend({
				channelId: "epic-1",
				from: "alice",
				subject: "s",
				body: "b",
			});
			assert.equal(db.inspectLastActivityAt("alice"), FIXED_NOW);
			assert.equal(
				db.inspectLastActivityAt("bob"),
				bobBefore,
				"subscriber receiving fan-out is not 'activity'",
			);
		});

		test("register_should_not_update_last_activity_at", () => {
			activityBroker.register({ topicId: "alice" });
			assert.equal(
				db.inspectLastActivityAt("alice"),
				null,
				"registration is bookkeeping, not activity",
			);
		});

		test("channelCreate_should_not_update_last_activity_at", () => {
			activityBroker.register({ topicId: "alice" });
			activityBroker.channelCreate({
				channelId: "epic-1",
				createdBy: "alice",
			});
			assert.equal(db.inspectLastActivityAt("alice"), null);
		});

		test("channelUnsubscribe_should_not_update_last_activity_at", () => {
			activityBroker.register({ topicId: "alice" });
			activityBroker.channelCreate({
				channelId: "epic-1",
				createdBy: "alice",
			});
			activityBroker.register({ topicId: "bob" });
			activityBroker.channelSubscribe({
				channelId: "epic-1",
				topicId: "bob",
			});
			const bobAfterSubscribe = db.inspectLastActivityAt("bob");
			activityBroker.channelUnsubscribe({
				channelId: "epic-1",
				topicId: "bob",
			});
			assert.equal(
				db.inspectLastActivityAt("bob"),
				bobAfterSubscribe,
				"unsubscribe must not bump activity",
			);
		});
	});
});
