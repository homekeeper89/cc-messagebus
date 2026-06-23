import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, beforeEach, describe, test } from "node:test";
import Database from "better-sqlite3";
import { openDatabase } from "../../src/server/db.js";

const V1_SCHEMA_DDL = `
CREATE TABLE sessions (
	topic_id TEXT PRIMARY KEY,
	connected_at TEXT NOT NULL,
	last_seen_at TEXT NOT NULL,
	status TEXT NOT NULL,
	last_activity_at TEXT
);

CREATE TABLE channels (
	id TEXT PRIMARY KEY,
	created_at TEXT NOT NULL,
	created_by TEXT NOT NULL
);

CREATE TABLE channel_subscriptions (
	channel_id TEXT NOT NULL,
	subscriber_topic_id TEXT NOT NULL,
	subscribed_at TEXT NOT NULL,
	PRIMARY KEY (channel_id, subscriber_topic_id)
);

CREATE TABLE channel_messages (
	id TEXT PRIMARY KEY,
	channel_id TEXT NOT NULL,
	from_topic_id TEXT NOT NULL,
	subject TEXT NOT NULL,
	body TEXT NOT NULL,
	sent_at TEXT NOT NULL
);

CREATE TABLE messages (
	id TEXT PRIMARY KEY,
	from_topic TEXT NOT NULL,
	to_topic TEXT NOT NULL,
	subject TEXT NOT NULL,
	body TEXT NOT NULL,
	thread_id TEXT,
	sent_at TEXT NOT NULL,
	in_flight_until TEXT,
	acked_at TEXT,
	expires_at TEXT NOT NULL,
	channel_message_id TEXT REFERENCES channel_messages(id) ON DELETE SET NULL
);

CREATE INDEX idx_channel_messages_channel_sent ON channel_messages (channel_id, sent_at);
CREATE INDEX idx_channel_subscriptions_subscriber ON channel_subscriptions (subscriber_topic_id);
CREATE INDEX idx_messages_delivery ON messages (to_topic, acked_at, in_flight_until);
CREATE INDEX idx_messages_expires ON messages (expires_at);
`;

function seedV1Database(dbPath: string): void {
	const raw = new Database(dbPath);
	raw.pragma("foreign_keys = ON");
	raw.exec(V1_SCHEMA_DDL);
	raw.pragma("user_version = 1");

	raw
		.prepare(
			"INSERT INTO sessions (topic_id, connected_at, last_seen_at, status, last_activity_at) VALUES (?, ?, ?, ?, ?)",
		)
		.run(
			"alice",
			"2026-06-19T00:00:00.000Z",
			"2026-06-19T00:00:01.000Z",
			"connected",
			"2026-06-19T00:00:02.000Z",
		);

	raw
		.prepare(
			"INSERT INTO channels (id, created_at, created_by) VALUES (?, ?, ?)",
		)
		.run("#general", "2026-06-19T00:00:00.000Z", "alice");

	raw
		.prepare(
			"INSERT INTO channel_subscriptions (channel_id, subscriber_topic_id, subscribed_at) VALUES (?, ?, ?)",
		)
		.run("#general", "alice", "2026-06-19T00:00:01.000Z");

	raw
		.prepare(
			"INSERT INTO channel_messages (id, channel_id, from_topic_id, subject, body, sent_at) VALUES (?, ?, ?, ?, ?, ?)",
		)
		.run(
			"cm-1",
			"#general",
			"alice",
			"hello",
			"world",
			"2026-06-19T00:00:02.000Z",
		);

	raw
		.prepare(
			"INSERT INTO messages (id, from_topic, to_topic, subject, body, thread_id, sent_at, in_flight_until, acked_at, expires_at, channel_message_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		)
		.run(
			"m-1",
			"alice",
			"bob",
			"dm-sub",
			"dm-body",
			null,
			"2026-06-19T00:00:03.000Z",
			null,
			null,
			"2027-06-19T00:00:03.000Z",
			"cm-1",
		);

	raw.close();
}

function tableHasColumn(
	raw: Database.Database,
	table: string,
	column: string,
): boolean {
	const cols = raw
		.prepare<[], { name: string }>(`PRAGMA table_info(${table})`)
		.all();
	return cols.some((c) => c.name === column);
}

function tableExists(raw: Database.Database, table: string): boolean {
	const row = raw
		.prepare<[string], { name: string } | undefined>(
			"SELECT name FROM sqlite_master WHERE type='table' AND name=?",
		)
		.get(table);
	return row !== undefined;
}

describe("migration v1 → v2", () => {
	let tmpDir: string;
	let dbPath: string;

	before(() => {
		tmpDir = mkdtempSync(join(tmpdir(), "ccmb-migrate-"));
	});

	after(() => {
		rmSync(tmpDir, { recursive: true, force: true });
	});

	beforeEach(() => {
		dbPath = join(tmpDir, `data-${Date.now()}-${Math.random()}.db`);
	});

	test("test_migration_v1_to_v2_should_rename_columns_and_preserve_data", () => {
		seedV1Database(dbPath);

		const db = openDatabase(dbPath);
		db.close();

		const raw = new Database(dbPath, { readonly: true });

		assert.ok(
			tableHasColumn(raw, "sessions", "peer_id"),
			"sessions.peer_id must exist after migration",
		);
		assert.equal(
			tableHasColumn(raw, "sessions", "topic_id"),
			false,
			"sessions.topic_id must be removed",
		);

		assert.ok(tableExists(raw, "topics"), "topics table must exist");
		assert.equal(
			tableExists(raw, "channels"),
			false,
			"channels table must be renamed",
		);
		assert.ok(
			tableExists(raw, "topic_subscriptions"),
			"topic_subscriptions table must exist",
		);
		assert.equal(
			tableExists(raw, "channel_subscriptions"),
			false,
			"channel_subscriptions must be renamed",
		);
		assert.ok(
			tableExists(raw, "topic_messages"),
			"topic_messages table must exist",
		);

		assert.ok(tableHasColumn(raw, "topic_subscriptions", "subscriber_peer_id"));
		assert.ok(tableHasColumn(raw, "topic_messages", "from_peer_id"));
		assert.ok(tableHasColumn(raw, "messages", "from_peer"));
		assert.ok(tableHasColumn(raw, "messages", "to_peer"));
		assert.ok(tableHasColumn(raw, "messages", "topic_message_id"));

		const session = raw
			.prepare<[], { peer_id: string; status: string }>(
				"SELECT peer_id, status FROM sessions",
			)
			.get();
		assert.equal(session?.peer_id, "alice");
		assert.equal(session?.status, "connected");

		const topic = raw
			.prepare<[], { id: string; created_by: string }>(
				"SELECT id, created_by FROM topics",
			)
			.get();
		assert.equal(topic?.id, "#general");
		assert.equal(topic?.created_by, "alice");

		const sub = raw
			.prepare<[], { topic_id: string; subscriber_peer_id: string }>(
				"SELECT topic_id, subscriber_peer_id FROM topic_subscriptions",
			)
			.get();
		assert.equal(sub?.topic_id, "#general");
		assert.equal(sub?.subscriber_peer_id, "alice");

		const topicMsg = raw
			.prepare<[], { id: string; topic_id: string; from_peer_id: string }>(
				"SELECT id, topic_id, from_peer_id FROM topic_messages",
			)
			.get();
		assert.equal(topicMsg?.id, "cm-1");
		assert.equal(topicMsg?.topic_id, "#general");
		assert.equal(topicMsg?.from_peer_id, "alice");

		const dm = raw
			.prepare<
				[],
				{ from_peer: string; to_peer: string; topic_message_id: string }
			>("SELECT from_peer, to_peer, topic_message_id FROM messages")
			.get();
		assert.equal(dm?.from_peer, "alice");
		assert.equal(dm?.to_peer, "bob");
		assert.equal(dm?.topic_message_id, "cm-1");

		const userVersion = raw.pragma("user_version", { simple: true });
		assert.equal(userVersion, 2);

		raw.close();
	});

	test("test_migration_should_be_idempotent_on_v2_database", () => {
		const db = openDatabase(dbPath);
		db.registerSession("alice", "2026-06-19T00:00:00.000Z");
		db.createTopic("#general", "alice", "2026-06-19T00:00:01.000Z");
		db.close();

		const db2 = openDatabase(dbPath);
		assert.ok(db2);
		const peer = db2.registerSession("bob", "2026-06-19T00:01:00.000Z");
		assert.equal(peer.peerId, "bob");
		db2.close();

		const raw = new Database(dbPath, { readonly: true });
		const userVersion = raw.pragma("user_version", { simple: true });
		assert.equal(userVersion, 2);
		raw.close();
	});
});
