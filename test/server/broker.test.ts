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
		const result = broker.register({ peerId: "alice" });
		assert.equal(result.peerId, "alice");
		assert.equal(result.monitorCommand, "cc-messagebus tail alice");
		assert.equal(result.dashboardUrl, "http://localhost:5959");
		assert.ok(receivedEvent);
	});

	test("register throws BrokerError on connected duplicate", () => {
		broker.register({ peerId: "alice" });
		assert.throws(
			() => broker.register({ peerId: "alice" }),
			(err: unknown) =>
				err instanceof BrokerError && err.code === "PEER_ALREADY_REGISTERED",
		);
	});

	test("send: target not registered throws PEER_NOT_FOUND", () => {
		broker.register({ peerId: "alice" });
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
		broker.register({ peerId: "alice" });
		broker.register({ peerId: "bob" });
		broker.disconnect("bob");
		const result = broker.send({
			from: "alice",
			to: "bob",
			subject: "hi",
			body: "x",
		});
		assert.ok(result.messageId);
		const peers = broker.listPeers().peers;
		const bob = peers.find((p) => p.peerId === "bob");
		assert.equal(bob?.queueLength, 1);
		assert.equal(bob?.status, "disconnected");
	});

	test("end-to-end: register/send/read/ack flow", () => {
		broker.register({ peerId: "alice" });
		broker.register({ peerId: "bob" });
		const sent = broker.send({
			from: "alice",
			to: "bob",
			subject: "ping",
			body: "hello",
		});
		const read = broker.read({ peerId: "bob" });
		assert.equal(read.messages.length, 1);
		assert.equal(read.messages[0]?.id, sent.messageId);
		assert.equal(read.messages[0]?.subject, "ping");
		assert.ok(read.messages[0]?.inFlightUntil);

		const acked = broker.ack({ peerId: "bob", messageId: sent.messageId });
		assert.ok(acked.ackedAt);

		const reread = broker.read({ peerId: "bob" });
		assert.equal(reread.messages.length, 0);
	});

	test("ack: already-acked message throws MESSAGE_NOT_IN_FLIGHT", () => {
		broker.register({ peerId: "alice" });
		broker.register({ peerId: "bob" });
		const sent = broker.send({
			from: "alice",
			to: "bob",
			subject: "s",
			body: "b",
		});
		broker.read({ peerId: "bob" });
		broker.ack({ peerId: "bob", messageId: sent.messageId });
		assert.throws(
			() => broker.ack({ peerId: "bob", messageId: sent.messageId }),
			(err: unknown) =>
				err instanceof BrokerError && err.code === "MESSAGE_NOT_IN_FLIGHT",
		);
	});

	test("ack: unknown messageId throws MESSAGE_NOT_FOUND", () => {
		broker.register({ peerId: "bob" });
		assert.throws(
			() => broker.ack({ peerId: "bob", messageId: "nope" }),
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
		shortBroker.register({ peerId: "alice" });
		shortBroker.register({ peerId: "bob" });
		shortBroker.send({ from: "alice", to: "bob", subject: "s", body: "b" });
		const first = shortBroker.read({ peerId: "bob" });
		assert.equal(first.messages.length, 1);
		const second = shortBroker.read({ peerId: "bob" });
		assert.equal(
			second.messages.length,
			1,
			"expired in-flight window allows immediate redelivery",
		);
	});

	test("unregister: throws PEER_NOT_FOUND for unknown topic", () => {
		assert.throws(
			() => broker.unregister("ghost", { peerId: "ghost" }),
			(err: unknown) =>
				err instanceof BrokerError && err.code === "PEER_NOT_FOUND",
		);
	});

	test("unregister: purgeQueue=false preserves messages", () => {
		broker.register({ peerId: "alice" });
		broker.register({ peerId: "bob" });
		broker.send({ from: "alice", to: "bob", subject: "s", body: "b" });
		const result = broker.unregister("bob", {
			peerId: "bob",
			purgeQueue: false,
		});
		assert.equal(result.purged, false);
		broker.register({ peerId: "bob" });
		const peers = broker.listPeers().peers;
		const bob = peers.find((p) => p.peerId === "bob");
		assert.equal(bob?.queueLength, 1, "queued message survives unregister");
	});

	test("unregister: purgeQueue=true drops queued messages", () => {
		broker.register({ peerId: "alice" });
		broker.register({ peerId: "bob" });
		broker.send({ from: "alice", to: "bob", subject: "s", body: "b" });
		const result = broker.unregister("bob", {
			peerId: "bob",
			purgeQueue: true,
		});
		assert.equal(result.purged, true);
		broker.register({ peerId: "bob" });
		const peers = broker.listPeers().peers;
		const bob = peers.find((p) => p.peerId === "bob");
		assert.equal(bob?.queueLength, 0, "queue purged");
	});

	test("unregister: emits session_disconnected", () => {
		broker.register({ peerId: "alice" });
		let received: unknown = null;
		broker.events.on("session_disconnected", (e) => {
			received = e;
		});
		broker.unregister("alice", { peerId: "alice" });
		assert.ok(received);
	});

	test("listener throw does not crash broker.emit", () => {
		broker.events.on("session_registered", () => {
			throw new Error("listener fail");
		});
		assert.doesNotThrow(() => broker.register({ peerId: "alice" }));
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
			activityBroker.register({ peerId: "alice" });
			activityBroker.register({ peerId: "bob" });
			activityBroker.send({
				from: "alice",
				to: "bob",
				subject: "s",
				body: "b",
			});
			assert.equal(db.inspectLastActivityAt("alice"), FIXED_NOW);
		});

		test("read_should_update_last_activity_at_for_reader_even_when_empty", () => {
			activityBroker.register({ peerId: "bob" });
			const result = activityBroker.read({ peerId: "bob" });
			assert.equal(result.messages.length, 0);
			assert.equal(
				db.inspectLastActivityAt("bob"),
				FIXED_NOW,
				"polling read counts as activity",
			);
		});

		test("ack_should_update_last_activity_at_for_acker", () => {
			activityBroker.register({ peerId: "alice" });
			activityBroker.register({ peerId: "bob" });
			const sent = activityBroker.send({
				from: "alice",
				to: "bob",
				subject: "s",
				body: "b",
			});
			activityBroker.read({ peerId: "bob" });
			activityBroker.ack({ peerId: "bob", messageId: sent.messageId });
			assert.equal(db.inspectLastActivityAt("bob"), FIXED_NOW);
		});

		test("topicSubscribe_should_update_last_activity_at_for_subscriber", () => {
			activityBroker.register({ peerId: "alice" });
			activityBroker.topicCreate({
				topicId: "epic-1",
				createdBy: "alice",
			});
			activityBroker.register({ peerId: "bob" });
			activityBroker.topicSubscribe({
				topicId: "epic-1",
				peerId: "bob",
			});
			assert.equal(db.inspectLastActivityAt("bob"), FIXED_NOW);
		});

		test("topicSend_should_update_last_activity_at_for_sender_only", () => {
			activityBroker.register({ peerId: "alice" });
			activityBroker.topicCreate({
				topicId: "epic-1",
				createdBy: "alice",
			});
			activityBroker.register({ peerId: "bob" });
			activityBroker.topicSubscribe({
				topicId: "epic-1",
				peerId: "bob",
			});

			const bobBefore = db.inspectLastActivityAt("bob");
			activityBroker.topicSend({
				topicId: "epic-1",
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
			activityBroker.register({ peerId: "alice" });
			assert.equal(
				db.inspectLastActivityAt("alice"),
				null,
				"registration is bookkeeping, not activity",
			);
		});

		test("topicCreate_should_not_update_last_activity_at", () => {
			activityBroker.register({ peerId: "alice" });
			activityBroker.topicCreate({
				topicId: "epic-1",
				createdBy: "alice",
			});
			assert.equal(db.inspectLastActivityAt("alice"), null);
		});

		test("topicUnsubscribe_should_not_update_last_activity_at", () => {
			activityBroker.register({ peerId: "alice" });
			activityBroker.topicCreate({
				topicId: "epic-1",
				createdBy: "alice",
			});
			activityBroker.register({ peerId: "bob" });
			activityBroker.topicSubscribe({
				topicId: "epic-1",
				peerId: "bob",
			});
			const bobAfterSubscribe = db.inspectLastActivityAt("bob");
			activityBroker.topicUnsubscribe({
				topicId: "epic-1",
				peerId: "bob",
			});
			assert.equal(
				db.inspectLastActivityAt("bob"),
				bobAfterSubscribe,
				"unsubscribe must not bump activity",
			);
		});
	});

	describe("list_peers ordering and lastActivityAt exposure", () => {
		const FIXED_NOW = "2026-06-20T00:00:00.000Z";

		test("listPeers exposes lastActivityAt as null for newly registered peer", () => {
			const fixedBroker = createBroker(db, {
				visibilityTimeoutSec: 30,
				ttlDays: 30,
				dashboardUrl: "http://localhost:5959",
				clock: () => FIXED_NOW,
			});
			fixedBroker.register({ peerId: "alice" });
			const peers = fixedBroker.listPeers().peers;
			const alice = peers.find((p) => p.peerId === "alice");
			assert.equal(alice?.lastActivityAt, null);
		});

		test("listPeers exposes lastActivityAt after send", () => {
			const fixedBroker = createBroker(db, {
				visibilityTimeoutSec: 30,
				ttlDays: 30,
				dashboardUrl: "http://localhost:5959",
				clock: () => FIXED_NOW,
			});
			fixedBroker.register({ peerId: "alice" });
			fixedBroker.register({ peerId: "bob" });
			fixedBroker.send({
				from: "alice",
				to: "bob",
				subject: "s",
				body: "b",
			});
			const peers = fixedBroker.listPeers().peers;
			const alice = peers.find((p) => p.peerId === "alice");
			assert.equal(alice?.lastActivityAt, FIXED_NOW);
		});

		test("listPeers orders by last_activity_at DESC with NULLS last", () => {
			let now = "2026-06-20T00:00:00.000Z";
			const mutableBroker = createBroker(db, {
				visibilityTimeoutSec: 30,
				ttlDays: 30,
				dashboardUrl: "http://localhost:5959",
				clock: () => now,
			});
			mutableBroker.register({ peerId: "charlie" });
			mutableBroker.register({ peerId: "alice" });
			mutableBroker.register({ peerId: "bob" });
			now = "2026-06-20T00:00:01.000Z";
			mutableBroker.send({
				from: "alice",
				to: "alice",
				subject: "s",
				body: "b",
			});
			now = "2026-06-20T00:00:02.000Z";
			mutableBroker.send({
				from: "bob",
				to: "bob",
				subject: "s",
				body: "b",
			});
			const peerIds = mutableBroker.listPeers().peers.map((p) => p.peerId);
			assert.deepEqual(peerIds, ["bob", "alice", "charlie"]);
		});

		test("listPeers tie-breaks by connected_at ASC when both last_activity_at are NULL", () => {
			let now = "2026-06-20T00:00:00.000Z";
			const mutableBroker = createBroker(db, {
				visibilityTimeoutSec: 30,
				ttlDays: 30,
				dashboardUrl: "http://localhost:5959",
				clock: () => now,
			});
			mutableBroker.register({ peerId: "zulu" });
			now = "2026-06-20T00:00:01.000Z";
			mutableBroker.register({ peerId: "alpha" });
			const peerIds = mutableBroker.listPeers().peers.map((p) => p.peerId);
			assert.deepEqual(peerIds, ["zulu", "alpha"]);
		});
	});

	describe("list_topics", () => {
		test("returns empty array when no topics exist", () => {
			const res = broker.listTopics();
			assert.deepEqual(res.topics, []);
		});

		test("returns topic with subscriberCount=0 and lastPublishedAt=null when no subscribers and no messages", () => {
			let now = "2026-06-20T00:00:00.000Z";
			const fixedBroker = createBroker(db, {
				visibilityTimeoutSec: 30,
				ttlDays: 30,
				dashboardUrl: "http://localhost:5959",
				clock: () => now,
			});
			fixedBroker.register({ peerId: "alice" });
			now = "2026-06-20T00:00:01.000Z";
			fixedBroker.topicCreate({ topicId: "general", createdBy: "alice" });
			const res = fixedBroker.listTopics();
			assert.equal(res.topics.length, 1);
			assert.equal(res.topics[0].topicId, "general");
			assert.equal(res.topics[0].createdBy, "alice");
			assert.equal(res.topics[0].createdAt, "2026-06-20T00:00:01.000Z");
			assert.equal(res.topics[0].subscriberCount, 0);
			assert.equal(res.topics[0].lastPublishedAt, null);
		});

		test("counts distinct subscribers across multiple subscriptions", () => {
			broker.register({ peerId: "alice" });
			broker.register({ peerId: "bob" });
			broker.register({ peerId: "carol" });
			broker.topicCreate({ topicId: "ch1", createdBy: "alice" });
			broker.topicSubscribe({ topicId: "ch1", peerId: "alice" });
			broker.topicSubscribe({ topicId: "ch1", peerId: "bob" });
			broker.topicSubscribe({ topicId: "ch1", peerId: "carol" });
			const res = broker.listTopics();
			assert.equal(res.topics.length, 1);
			assert.equal(res.topics[0].subscriberCount, 3);
		});

		test("orders topics by lastPublishedAt DESC with NULLS last, tie-breaks by createdAt ASC", () => {
			let now = "2026-06-20T00:00:00.000Z";
			const mutableBroker = createBroker(db, {
				visibilityTimeoutSec: 30,
				ttlDays: 30,
				dashboardUrl: "http://localhost:5959",
				clock: () => now,
			});
			mutableBroker.register({ peerId: "alice" });
			mutableBroker.register({ peerId: "bob" });
			now = "2026-06-20T00:00:01.000Z";
			mutableBroker.topicCreate({
				topicId: "older-silent",
				createdBy: "alice",
			});
			now = "2026-06-20T00:00:02.000Z";
			mutableBroker.topicCreate({
				topicId: "newer-silent",
				createdBy: "alice",
			});
			now = "2026-06-20T00:00:03.000Z";
			mutableBroker.topicCreate({
				topicId: "old-active",
				createdBy: "alice",
			});
			mutableBroker.topicSubscribe({
				topicId: "old-active",
				peerId: "bob",
			});
			now = "2026-06-20T00:00:04.000Z";
			mutableBroker.topicCreate({
				topicId: "new-active",
				createdBy: "alice",
			});
			mutableBroker.topicSubscribe({
				topicId: "new-active",
				peerId: "bob",
			});
			now = "2026-06-20T00:00:05.000Z";
			mutableBroker.topicSend({
				topicId: "old-active",
				from: "alice",
				subject: "s",
				body: "b",
			});
			now = "2026-06-20T00:00:06.000Z";
			mutableBroker.topicSend({
				topicId: "new-active",
				from: "alice",
				subject: "s",
				body: "b",
			});
			const ids = mutableBroker.listTopics().topics.map((c) => c.topicId);
			assert.deepEqual(ids, [
				"new-active",
				"old-active",
				"older-silent",
				"newer-silent",
			]);
		});
	});

	describe("topic_detail", () => {
		test("topicDetail_should_throw_TOPIC_NOT_FOUND_when_topic_missing", () => {
			assert.throws(
				() => broker.topicDetail({ topicId: "ghost" }),
				(e: unknown) =>
					e instanceof BrokerError && e.code === "TOPIC_NOT_FOUND",
			);
		});

		test("topicDetail_should_return_empty_subscribers_when_none_subscribed", () => {
			broker.register({ peerId: "alice" });
			broker.topicCreate({ topicId: "empty", createdBy: "alice" });
			const res = broker.topicDetail({ topicId: "empty" });
			assert.equal(res.topic.topicId, "empty");
			assert.equal(res.topic.createdBy, "alice");
			assert.deepEqual(res.topic.subscribers, []);
		});

		test("topicDetail_should_list_subscribers_ordered_by_subscribed_at_ASC", () => {
			let now = "2026-06-20T00:00:00.000Z";
			db.close();
			rmSync(dbPath, { force: true });
			db = openDatabase(dbPath);
			const mutableBroker = createBroker(db, {
				visibilityTimeoutSec: 30,
				ttlDays: 30,
				dashboardUrl: "http://localhost:5959",
				clock: () => now,
			});

			mutableBroker.register({ peerId: "alice" });
			mutableBroker.register({ peerId: "bob" });
			mutableBroker.register({ peerId: "carol" });
			mutableBroker.topicCreate({ topicId: "ch1", createdBy: "alice" });

			now = "2026-06-20T00:00:01.000Z";
			mutableBroker.topicSubscribe({ topicId: "ch1", peerId: "bob" });
			now = "2026-06-20T00:00:02.000Z";
			mutableBroker.topicSubscribe({ topicId: "ch1", peerId: "carol" });

			const res = mutableBroker.topicDetail({ topicId: "ch1" });
			const ids = res.topic.subscribers.map((s) => s.peerId);
			assert.deepEqual(ids, ["bob", "carol"]);
		});

		test("topicDetail_should_compute_queueDepth_and_lastReadAt_per_subscriber", () => {
			let now = "2026-06-20T00:00:00.000Z";
			db.close();
			rmSync(dbPath, { force: true });
			db = openDatabase(dbPath);
			const mutableBroker = createBroker(db, {
				visibilityTimeoutSec: 30,
				ttlDays: 30,
				dashboardUrl: "http://localhost:5959",
				clock: () => now,
			});

			mutableBroker.register({ peerId: "alice" });
			mutableBroker.register({ peerId: "bob" });
			mutableBroker.register({ peerId: "carol" });
			mutableBroker.topicCreate({ topicId: "ch1", createdBy: "alice" });
			mutableBroker.topicSubscribe({ topicId: "ch1", peerId: "bob" });
			mutableBroker.topicSubscribe({ topicId: "ch1", peerId: "carol" });

			now = "2026-06-20T00:00:10.000Z";
			mutableBroker.topicSend({
				topicId: "ch1",
				from: "alice",
				subject: "m1",
				body: "b1",
			});
			now = "2026-06-20T00:00:11.000Z";
			mutableBroker.topicSend({
				topicId: "ch1",
				from: "alice",
				subject: "m2",
				body: "b2",
			});

			// bob: reads and acks m1 only — queueDepth=1, lastReadAt = ack time of m1
			now = "2026-06-20T00:00:20.000Z";
			const bobMsgs = mutableBroker.read({ peerId: "bob" }).messages;
			const bobM1 = bobMsgs.find((m) => m.subject === "m1");
			assert.ok(bobM1);
			now = "2026-06-20T00:00:21.000Z";
			mutableBroker.ack({ peerId: "bob", messageId: bobM1.id });

			// carol: no acks — queueDepth=2, lastReadAt=null
			const res = mutableBroker.topicDetail({ topicId: "ch1" });
			const bob = res.topic.subscribers.find((s) => s.peerId === "bob");
			const carol = res.topic.subscribers.find((s) => s.peerId === "carol");
			assert.ok(bob);
			assert.ok(carol);
			assert.equal(bob.queueDepth, 1);
			assert.equal(bob.lastReadAt, "2026-06-20T00:00:21.000Z");
			assert.equal(carol.queueDepth, 2);
			assert.equal(carol.lastReadAt, null);
		});

		test("topicDetail_should_handle_many_subscribers_in_single_query", () => {
			broker.register({ peerId: "alice" });
			broker.topicCreate({ topicId: "wide", createdBy: "alice" });
			const subscribers = Array.from({ length: 50 }, (_, i) => `peer-${i}`);
			for (const t of subscribers) {
				broker.register({ peerId: t });
				broker.topicSubscribe({ topicId: "wide", peerId: t });
			}
			const res = broker.topicDetail({ topicId: "wide" });
			assert.equal(res.topic.subscribers.length, 50);
			for (const s of res.topic.subscribers) {
				assert.equal(s.queueDepth, 0);
				assert.equal(s.lastReadAt, null);
			}
		});

		test("topicDetail_should_only_count_topic_origin_messages_not_direct_sends", () => {
			broker.register({ peerId: "alice" });
			broker.register({ peerId: "bob" });
			broker.topicCreate({ topicId: "ch1", createdBy: "alice" });
			broker.topicSubscribe({ topicId: "ch1", peerId: "bob" });
			// direct send (not topic) — must NOT count toward queueDepth
			broker.send({ from: "alice", to: "bob", subject: "direct", body: "d" });

			const res = broker.topicDetail({ topicId: "ch1" });
			const bob = res.topic.subscribers.find((s) => s.peerId === "bob");
			assert.ok(bob);
			assert.equal(bob.queueDepth, 0);
		});
	});

	describe("peersClean", () => {
		test("removes peers whose pid is dead (ESRCH)", () => {
			broker.register({ peerId: "alive", pid: process.pid });
			broker.register({ peerId: "dead", pid: 999999 });

			const res = broker.peersClean();

			assert.equal(res.cleaned.length, 1);
			assert.equal(res.cleaned[0].peerId, "dead");
			assert.equal(res.cleaned[0].pid, 999999);
			const peers = broker.listPeers().peers.map((p) => p.peerId);
			assert.ok(peers.includes("alive"));
			assert.ok(!peers.includes("dead"));
		});

		test("keeps peers with null pid (never registered pid)", () => {
			broker.register({ peerId: "nopid" });

			const res = broker.peersClean();

			assert.equal(res.cleaned.length, 0);
			const peers = broker.listPeers().peers.map((p) => p.peerId);
			assert.ok(peers.includes("nopid"));
		});

		test("emits session_disconnected for cleaned peers", () => {
			broker.register({ peerId: "dead", pid: 999999 });
			const events: string[] = [];
			broker.events.on("session_disconnected", (e) => {
				events.push((e as { peerId: string }).peerId);
			});

			broker.peersClean();

			assert.deepEqual(events, ["dead"]);
		});

		test("returns empty when no peers are dead", () => {
			broker.register({ peerId: "alive", pid: process.pid });

			const res = broker.peersClean();

			assert.equal(res.cleaned.length, 0);
		});

		test("removes disconnected peers regardless of pid", () => {
			broker.register({ peerId: "gone", pid: process.pid });
			broker.disconnect("gone");
			broker.register({ peerId: "gone-nopid" });
			broker.disconnect("gone-nopid");

			const res = broker.peersClean();

			const cleanedIds = res.cleaned.map((c) => c.peerId).sort();
			assert.deepEqual(cleanedIds, ["gone", "gone-nopid"]);
			const peers = broker.listPeers().peers.map((p) => p.peerId);
			assert.ok(!peers.includes("gone"));
			assert.ok(!peers.includes("gone-nopid"));
		});
	});

	describe("listDmConversations", () => {
		test("returns empty when no DMs sent", () => {
			assert.deepEqual(broker.listDmConversations(), { conversations: [] });
		});

		test("groups messages by unordered peer pair", () => {
			let tick = 0;
			const clockedBroker = createBroker(db, {
				visibilityTimeoutSec: 30,
				ttlDays: 30,
				dashboardUrl: "http://localhost:5959",
				clock: () => new Date(2026, 0, 1, 0, 0, tick++).toISOString(),
			});
			clockedBroker.register({ peerId: "alice" });
			clockedBroker.register({ peerId: "bob" });
			clockedBroker.register({ peerId: "carol" });

			clockedBroker.send({
				from: "alice",
				to: "bob",
				subject: "hi",
				body: "1",
			});
			clockedBroker.send({
				from: "bob",
				to: "alice",
				subject: "re",
				body: "2",
			});
			clockedBroker.send({
				from: "alice",
				to: "carol",
				subject: "yo",
				body: "3",
			});

			const { conversations } = clockedBroker.listDmConversations();
			assert.equal(conversations.length, 2);
			const abPair = conversations.find(
				(c) => c.peerA === "alice" && c.peerB === "bob",
			);
			assert.ok(abPair, "alice↔bob pair present");
			assert.equal(abPair?.messageCount, 2);
			assert.equal(abPair?.lastFrom, "bob");
			assert.equal(abPair?.lastSubject, "re");
		});

		test("orders conversations by lastSentAt DESC", () => {
			let tick = 0;
			const clockedBroker = createBroker(db, {
				visibilityTimeoutSec: 30,
				ttlDays: 30,
				dashboardUrl: "http://localhost:5959",
				clock: () => new Date(2026, 0, 1, 0, 0, tick++).toISOString(),
			});
			clockedBroker.register({ peerId: "alice" });
			clockedBroker.register({ peerId: "bob" });
			clockedBroker.register({ peerId: "carol" });

			clockedBroker.send({
				from: "alice",
				to: "bob",
				subject: "old",
				body: "",
			});
			clockedBroker.send({
				from: "alice",
				to: "carol",
				subject: "new",
				body: "",
			});

			const { conversations } = clockedBroker.listDmConversations();
			assert.equal(conversations[0]?.lastSubject, "new");
			assert.equal(conversations[1]?.lastSubject, "old");
		});

		test("excludes topic fanout messages", () => {
			broker.register({ peerId: "alice" });
			broker.register({ peerId: "bob" });
			broker.topicCreate({ topicId: "t1", createdBy: "alice" });
			broker.topicSubscribe({ topicId: "t1", peerId: "bob" });
			broker.topicSend({
				topicId: "t1",
				from: "alice",
				subject: "topicmsg",
				body: "",
			});

			assert.deepEqual(broker.listDmConversations(), { conversations: [] });
		});
	});
});
