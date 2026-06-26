import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import Database from "better-sqlite3";
import type {
	IsoTimestamp,
	MessageDto,
	MessageId,
	PeerDto,
	PeerId,
	TopicId,
	TopicSummaryDto,
} from "../protocol/http.js";
import { SessionStatus } from "../protocol/http.js";

export type DbErrorCode =
	| "PEER_ALREADY_REGISTERED"
	| "MESSAGE_NOT_FOUND"
	| "MESSAGE_NOT_IN_FLIGHT"
	| "TOPIC_NOT_FOUND"
	| "TOPIC_ALREADY_EXISTS"
	| "ALREADY_SUBSCRIBED"
	| "NOT_SUBSCRIBED";

export class DbError extends Error {
	constructor(public code: DbErrorCode) {
		super(code);
		this.name = "DbError";
	}
}

const SCHEMA_DDL = `
CREATE TABLE IF NOT EXISTS sessions (
	peer_id TEXT PRIMARY KEY,
	connected_at TEXT NOT NULL,
	last_seen_at TEXT NOT NULL,
	status TEXT NOT NULL,
	last_activity_at TEXT
);

CREATE TABLE IF NOT EXISTS topics (
	id TEXT PRIMARY KEY,
	created_at TEXT NOT NULL,
	created_by TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS topic_subscriptions (
	topic_id TEXT NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
	subscriber_peer_id TEXT NOT NULL,
	subscribed_at TEXT NOT NULL,
	PRIMARY KEY (topic_id, subscriber_peer_id)
);

CREATE TABLE IF NOT EXISTS topic_messages (
	id TEXT PRIMARY KEY,
	topic_id TEXT NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
	from_peer_id TEXT NOT NULL,
	subject TEXT NOT NULL,
	body TEXT NOT NULL,
	sent_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
	id TEXT PRIMARY KEY,
	from_peer TEXT NOT NULL,
	to_peer TEXT NOT NULL,
	subject TEXT NOT NULL,
	body TEXT NOT NULL,
	thread_id TEXT,
	sent_at TEXT NOT NULL,
	in_flight_until TEXT,
	acked_at TEXT,
	expires_at TEXT NOT NULL,
	topic_message_id TEXT REFERENCES topic_messages(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_messages_delivery
	ON messages (to_peer, acked_at, in_flight_until);

CREATE INDEX IF NOT EXISTS idx_messages_expires
	ON messages (expires_at);

CREATE INDEX IF NOT EXISTS idx_topic_messages_topic_sent
	ON topic_messages (topic_id, sent_at);

CREATE INDEX IF NOT EXISTS idx_topic_subscriptions_subscriber
	ON topic_subscriptions (subscriber_peer_id);
`;

const MIGRATE_V1_TO_V2_SQL = `
ALTER TABLE sessions RENAME COLUMN topic_id TO peer_id;
ALTER TABLE messages RENAME COLUMN from_topic TO from_peer;
ALTER TABLE messages RENAME COLUMN to_topic TO to_peer;
ALTER TABLE messages RENAME COLUMN channel_message_id TO topic_message_id;
ALTER TABLE channels RENAME TO topics;
ALTER TABLE channel_subscriptions RENAME TO topic_subscriptions;
ALTER TABLE topic_subscriptions RENAME COLUMN channel_id TO topic_id;
ALTER TABLE topic_subscriptions RENAME COLUMN subscriber_topic_id TO subscriber_peer_id;
ALTER TABLE channel_messages RENAME TO topic_messages;
ALTER TABLE topic_messages RENAME COLUMN channel_id TO topic_id;
ALTER TABLE topic_messages RENAME COLUMN from_topic_id TO from_peer_id;
DROP INDEX IF EXISTS idx_channel_messages_channel_sent;
DROP INDEX IF EXISTS idx_channel_subscriptions_subscriber;
DROP INDEX IF EXISTS idx_messages_delivery;
CREATE INDEX IF NOT EXISTS idx_topic_messages_topic_sent ON topic_messages (topic_id, sent_at);
CREATE INDEX IF NOT EXISTS idx_topic_subscriptions_subscriber ON topic_subscriptions (subscriber_peer_id);
CREATE INDEX IF NOT EXISTS idx_messages_delivery ON messages (to_peer, acked_at, in_flight_until);
`;

// SQLite cannot ALTER an existing FK clause. v3 rebuilds messages so that
// topic_message_id cascades on topic_messages delete (was SET NULL in v2).
const MIGRATE_V2_TO_V3_SQL = `
CREATE TABLE messages_new (
	id TEXT PRIMARY KEY,
	from_peer TEXT NOT NULL,
	to_peer TEXT NOT NULL,
	subject TEXT NOT NULL,
	body TEXT NOT NULL,
	thread_id TEXT,
	sent_at TEXT NOT NULL,
	in_flight_until TEXT,
	acked_at TEXT,
	expires_at TEXT NOT NULL,
	topic_message_id TEXT REFERENCES topic_messages(id) ON DELETE CASCADE
);
INSERT INTO messages_new (id, from_peer, to_peer, subject, body, thread_id, sent_at, in_flight_until, acked_at, expires_at, topic_message_id)
	SELECT id, from_peer, to_peer, subject, body, thread_id, sent_at, in_flight_until, acked_at, expires_at, topic_message_id FROM messages;
DROP TABLE messages;
ALTER TABLE messages_new RENAME TO messages;
CREATE INDEX idx_messages_delivery ON messages (to_peer, acked_at, in_flight_until);
CREATE INDEX idx_messages_expires ON messages (expires_at);
`;

interface SessionRow {
	peer_id: string;
	connected_at: string;
	last_seen_at: string;
	status: string;
	last_activity_at: string | null;
}

interface MessageRow {
	id: string;
	from_peer: string;
	to_peer: string;
	subject: string;
	body: string;
	thread_id: string | null;
	sent_at: string;
	in_flight_until: string | null;
	acked_at: string | null;
	expires_at: string;
	topic_message_id: string | null;
}

interface TopicRow {
	id: string;
	created_at: string;
	created_by: string;
}

export interface TopicSendInput {
	topicMessageId: string;
	topicId: TopicId;
	from: PeerId;
	subject: string;
	body: string;
	sentAt: IsoTimestamp;
	expiresAt: IsoTimestamp;
	deliveryMessageIds: string[];
}

export interface TopicMessageRow {
	id: string;
	topic_id: string;
	from_peer_id: string;
	subject: string;
	body: string;
	sent_at: string;
}

export interface TopicDetailRow {
	topicId: TopicId;
	createdBy: PeerId;
	createdAt: IsoTimestamp;
	subscribers: Array<{
		peerId: PeerId;
		subscribedAt: IsoTimestamp;
		queueDepth: number;
		lastReadAt: IsoTimestamp | null;
	}>;
}

function rowToPeer(row: SessionRow, queueLength: number): PeerDto {
	return {
		peerId: row.peer_id,
		status: row.status as PeerDto["status"],
		connectedAt: row.connected_at,
		lastSeenAt: row.last_seen_at,
		lastActivityAt: row.last_activity_at,
		queueLength,
	};
}

function rowToMessage(row: MessageRow): MessageDto {
	return {
		id: row.id,
		from: row.from_peer,
		to: row.to_peer,
		subject: row.subject,
		body: row.body,
		threadId: row.thread_id,
		sentAt: row.sent_at,
		inFlightUntil: row.in_flight_until,
		ackedAt: row.acked_at,
		expiresAt: row.expires_at,
	};
}

function detectV1(db: Database.Database): boolean {
	const sessionsCols = db
		.prepare<[], { name: string }>("PRAGMA table_info(sessions)")
		.all();
	if (sessionsCols.length === 0) return false;
	return sessionsCols.some((c) => c.name === "topic_id");
}

function migrateV1ToV2(db: Database.Database): void {
	db.pragma("foreign_keys = OFF");
	try {
		const tx = db.transaction(() => {
			db.exec(MIGRATE_V1_TO_V2_SQL);
			db.pragma("user_version = 2");
		});
		tx();
	} finally {
		db.pragma("foreign_keys = ON");
	}
}

function migrateV2ToV3(db: Database.Database): void {
	db.pragma("foreign_keys = OFF");
	try {
		const tx = db.transaction(() => {
			db.exec(MIGRATE_V2_TO_V3_SQL);
			db.pragma("user_version = 3");
		});
		tx();
	} finally {
		db.pragma("foreign_keys = ON");
	}
}

export function openDatabase(dbPath: string) {
	mkdirSync(dirname(dbPath), { recursive: true });
	const db = new Database(dbPath);
	db.pragma("journal_mode = WAL");
	db.pragma("foreign_keys = ON");

	const isFreshDb =
		db
			.prepare<[], { name: string } | undefined>(
				"SELECT name FROM sqlite_master WHERE type='table' AND name='sessions'",
			)
			.get() === undefined;

	if (detectV1(db)) {
		migrateV1ToV2(db);
	}

	db.exec(SCHEMA_DDL);

	if (isFreshDb) {
		db.pragma("user_version = 3");
	}

	let userVersion = db.pragma("user_version", { simple: true }) as number;
	if (userVersion < 2) {
		db.pragma("user_version = 2");
		userVersion = 2;
	}
	if (userVersion === 2) {
		migrateV2ToV3(db);
		userVersion = 3;
	}

	const stmtGetSession = db.prepare<[string], SessionRow>(
		"SELECT * FROM sessions WHERE peer_id = ?",
	);
	const stmtInsertSession = db.prepare<[string, string, string, string]>(
		"INSERT INTO sessions (peer_id, connected_at, last_seen_at, status) VALUES (?, ?, ?, ?)",
	);
	const stmtReactivateSession = db.prepare<[string, string, string, string]>(
		"UPDATE sessions SET status = ?, connected_at = ?, last_seen_at = ? WHERE peer_id = ?",
	);
	const stmtDeleteSession = db.prepare<[string]>(
		"DELETE FROM sessions WHERE peer_id = ?",
	);
	const stmtPurgeQueue = db.prepare<[string]>(
		"DELETE FROM messages WHERE to_peer = ?",
	);
	const stmtMarkDisconnected = db.prepare<[string, string, string]>(
		"UPDATE sessions SET status = ?, last_seen_at = ? WHERE peer_id = ?",
	);
	const stmtTouchLastSeen = db.prepare<[string, string]>(
		"UPDATE sessions SET last_seen_at = ? WHERE peer_id = ?",
	);
	const stmtUpdateLastActivity = db.prepare<[string, string]>(
		"UPDATE sessions SET last_activity_at = ? WHERE peer_id = ?",
	);
	const stmtSelectLastActivity = db.prepare<
		[string],
		{ last_activity_at: string | null }
	>("SELECT last_activity_at FROM sessions WHERE peer_id = ?");
	const stmtListSessionsWithQueue = db.prepare<
		[],
		SessionRow & { queue_length: number }
	>(
		`SELECT s.*, COALESCE(q.cnt, 0) AS queue_length
		 FROM sessions s
		 LEFT JOIN (
		   SELECT to_peer, COUNT(*) AS cnt
		   FROM messages
		   WHERE acked_at IS NULL
		   GROUP BY to_peer
		 ) q ON q.to_peer = s.peer_id
		 ORDER BY s.last_activity_at DESC NULLS LAST, s.connected_at ASC`,
	);
	const stmtCountQueue = db.prepare<[string], { c: number }>(
		"SELECT COUNT(*) AS c FROM messages WHERE to_peer = ? AND acked_at IS NULL",
	);
	const stmtInsertMessage = db.prepare<
		[string, string, string, string, string, string | null, string, string]
	>(
		"INSERT INTO messages (id, from_peer, to_peer, subject, body, thread_id, sent_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
	);
	const stmtSelectDeliverable = db.prepare<
		[string, string, number],
		MessageRow
	>(
		`SELECT * FROM messages
		 WHERE to_peer = ?
		   AND acked_at IS NULL
		   AND (in_flight_until IS NULL OR in_flight_until <= ?)
		 ORDER BY sent_at ASC
		 LIMIT ?`,
	);
	const stmtSetInFlight = db.prepare<[string, string]>(
		"UPDATE messages SET in_flight_until = ? WHERE id = ?",
	);
	const stmtGetMessage = db.prepare<[string, string], MessageRow>(
		"SELECT * FROM messages WHERE id = ? AND to_peer = ?",
	);
	const stmtAckMessage = db.prepare<[string, string]>(
		"UPDATE messages SET acked_at = ?, in_flight_until = NULL WHERE id = ?",
	);
	const stmtSelectExpiredInFlight = db.prepare<[string], { id: string }>(
		"SELECT id FROM messages WHERE in_flight_until IS NOT NULL AND in_flight_until <= ? AND acked_at IS NULL",
	);
	const stmtClearInFlight = db.prepare<[string]>(
		"UPDATE messages SET in_flight_until = NULL WHERE id = ?",
	);
	const stmtSelectExpired = db.prepare<[string], { id: string }>(
		"SELECT id FROM messages WHERE expires_at <= ?",
	);
	const stmtDeleteMessage = db.prepare<[string]>(
		"DELETE FROM messages WHERE id = ?",
	);
	const stmtInsertTopic = db.prepare<[string, string, string]>(
		"INSERT INTO topics (id, created_at, created_by) VALUES (?, ?, ?)",
	);
	const stmtGetTopic = db.prepare<[string], TopicRow>(
		"SELECT * FROM topics WHERE id = ?",
	);
	const stmtInsertTopicSubscription = db.prepare<[string, string, string]>(
		"INSERT INTO topic_subscriptions (topic_id, subscriber_peer_id, subscribed_at) VALUES (?, ?, ?)",
	);
	const stmtDeleteTopicSubscription = db.prepare<[string, string]>(
		"DELETE FROM topic_subscriptions WHERE topic_id = ? AND subscriber_peer_id = ?",
	);
	const stmtListTopicHistoryAll = db.prepare<[string, number], TopicMessageRow>(
		`SELECT id, topic_id, from_peer_id, subject, body, sent_at
		 FROM topic_messages
		 WHERE topic_id = ?
		 ORDER BY sent_at DESC, id DESC
		 LIMIT ?`,
	);
	const stmtListTopicHistoryBefore = db.prepare<
		[string, string, number],
		TopicMessageRow
	>(
		`SELECT id, topic_id, from_peer_id, subject, body, sent_at
		 FROM topic_messages
		 WHERE topic_id = ? AND sent_at < ?
		 ORDER BY sent_at DESC, id DESC
		 LIMIT ?`,
	);
	const stmtListTopicSubscribers = db.prepare<
		[string],
		{ subscriber_peer_id: string }
	>("SELECT subscriber_peer_id FROM topic_subscriptions WHERE topic_id = ?");
	const stmtListTopicSummaries = db.prepare<
		[],
		{
			id: string;
			created_by: string;
			created_at: string;
			subscriber_count: number;
			last_published_at: string | null;
		}
	>(
		`SELECT t.id, t.created_by, t.created_at,
		        COALESCE(COUNT(DISTINCT s.subscriber_peer_id), 0) AS subscriber_count,
		        MAX(tm.sent_at) AS last_published_at
		 FROM topics t
		 LEFT JOIN topic_subscriptions s ON s.topic_id = t.id
		 LEFT JOIN topic_messages tm ON tm.topic_id = t.id
		 GROUP BY t.id
		 ORDER BY MAX(tm.sent_at) DESC NULLS LAST, t.created_at ASC`,
	);
	const stmtListTopicSubscribersWithStats = db.prepare<
		[string],
		{
			subscriber_peer_id: string;
			subscribed_at: string;
			queue_depth: number;
			last_read_at: string | null;
		}
	>(
		`SELECT s.subscriber_peer_id,
		        s.subscribed_at,
		        COALESCE(SUM(CASE WHEN m.topic_message_id IS NOT NULL AND m.acked_at IS NULL THEN 1 ELSE 0 END), 0) AS queue_depth,
		        MAX(CASE WHEN m.topic_message_id IS NOT NULL THEN m.acked_at END) AS last_read_at
		 FROM topic_subscriptions s
		 LEFT JOIN topic_messages tm ON tm.topic_id = s.topic_id
		 LEFT JOIN messages m
		        ON m.topic_message_id = tm.id
		       AND m.to_peer = s.subscriber_peer_id
		 WHERE s.topic_id = ?
		 GROUP BY s.subscriber_peer_id, s.subscribed_at
		 ORDER BY s.subscribed_at ASC, s.subscriber_peer_id ASC`,
	);
	const stmtInsertTopicMessage = db.prepare<
		[string, string, string, string, string, string]
	>(
		"INSERT INTO topic_messages (id, topic_id, from_peer_id, subject, body, sent_at) VALUES (?, ?, ?, ?, ?, ?)",
	);
	const stmtInsertMessageWithTopic = db.prepare<
		[
			string,
			string,
			string,
			string,
			string,
			string | null,
			string,
			string,
			string,
		]
	>(
		"INSERT INTO messages (id, from_peer, to_peer, subject, body, thread_id, sent_at, expires_at, topic_message_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
	);
	const stmtDeleteTopic = db.prepare<[string]>(
		"DELETE FROM topics WHERE id = ?",
	);
	const stmtCountTopicSubs = db.prepare<[string], { c: number }>(
		"SELECT COUNT(*) AS c FROM topic_subscriptions WHERE topic_id = ?",
	);
	// topic 삭제 전에 cascade 될 fanout messages 수를 미리 계산.
	const stmtCountTopicFanoutMessages = db.prepare<[string], { c: number }>(
		"SELECT COUNT(*) AS c FROM messages WHERE topic_message_id IN (SELECT id FROM topic_messages WHERE topic_id = ?)",
	);
	const stmtDeleteSubsForPeer = db.prepare<[string]>(
		"DELETE FROM topic_subscriptions WHERE subscriber_peer_id = ?",
	);
	const stmtCountPeerInflightInbox = db.prepare<[string], { c: number }>(
		"SELECT COUNT(*) AS c FROM messages WHERE to_peer = ? AND acked_at IS NULL AND in_flight_until IS NOT NULL",
	);
	const stmtDeletePeerInbox = db.prepare<[string]>(
		"DELETE FROM messages WHERE to_peer = ?",
	);

	function registerSession(peerId: PeerId, now: IsoTimestamp): PeerDto {
		const existing = stmtGetSession.get(peerId);
		if (existing) {
			if (existing.status === SessionStatus.CONNECTED) {
				throw new DbError("PEER_ALREADY_REGISTERED");
			}
			stmtReactivateSession.run(SessionStatus.CONNECTED, now, now, peerId);
		} else {
			stmtInsertSession.run(peerId, now, now, SessionStatus.CONNECTED);
		}
		return {
			peerId,
			status: SessionStatus.CONNECTED,
			connectedAt: now,
			lastSeenAt: now,
			lastActivityAt: null,
			queueLength: stmtCountQueue.get(peerId)?.c ?? 0,
		};
	}

	const unregisterTx = db.transaction(
		(peerId: PeerId, purge: boolean): { purged: boolean } => {
			stmtDeleteSession.run(peerId);
			if (purge) stmtPurgeQueue.run(peerId);
			return { purged: purge };
		},
	);

	function markDisconnected(peerId: PeerId, now: IsoTimestamp): void {
		stmtMarkDisconnected.run(SessionStatus.DISCONNECTED, now, peerId);
	}

	function touchLastSeen(peerId: PeerId, now: IsoTimestamp): void {
		stmtTouchLastSeen.run(now, peerId);
	}

	function updateLastActivity(peerId: PeerId, now: IsoTimestamp): void {
		stmtUpdateLastActivity.run(now, peerId);
	}

	function inspectLastActivityAt(peerId: PeerId): string | null {
		const row = stmtSelectLastActivity.get(peerId);
		return row?.last_activity_at ?? null;
	}

	function getSession(peerId: PeerId): PeerDto | null {
		const row = stmtGetSession.get(peerId);
		if (!row) return null;
		return rowToPeer(row, stmtCountQueue.get(peerId)?.c ?? 0);
	}

	function listSessions(): PeerDto[] {
		const rows = stmtListSessionsWithQueue.all();
		return rows.map((r) => rowToPeer(r, r.queue_length));
	}

	function insertMessage(msg: MessageDto): void {
		stmtInsertMessage.run(
			msg.id,
			msg.from,
			msg.to,
			msg.subject,
			msg.body,
			msg.threadId ?? null,
			msg.sentAt,
			msg.expiresAt,
		);
	}

	const fetchDeliverableTx = db.transaction(
		(
			peerId: PeerId,
			max: number,
			now: IsoTimestamp,
			inFlightUntil: IsoTimestamp,
		): MessageDto[] => {
			const rows = stmtSelectDeliverable.all(peerId, now, max);
			return rows.map((row) => {
				stmtSetInFlight.run(inFlightUntil, row.id);
				return { ...rowToMessage(row), inFlightUntil };
			});
		},
	);

	function ackMessage(
		peerId: PeerId,
		messageId: MessageId,
		now: IsoTimestamp,
	): IsoTimestamp {
		const row = stmtGetMessage.get(messageId, peerId);
		if (!row) throw new DbError("MESSAGE_NOT_FOUND");
		if (row.in_flight_until === null)
			throw new DbError("MESSAGE_NOT_IN_FLIGHT");
		stmtAckMessage.run(now, messageId);
		return now;
	}

	function expireInFlight(now: IsoTimestamp): MessageId[] {
		const rows = stmtSelectExpiredInFlight.all(now);
		for (const row of rows) stmtClearInFlight.run(row.id);
		return rows.map((r) => r.id);
	}

	function deleteExpired(now: IsoTimestamp): MessageId[] {
		const rows = stmtSelectExpired.all(now);
		for (const row of rows) stmtDeleteMessage.run(row.id);
		return rows.map((r) => r.id);
	}

	function createTopic(
		topicId: TopicId,
		createdBy: PeerId,
		now: IsoTimestamp,
	): void {
		const existing = stmtGetTopic.get(topicId);
		if (existing) throw new DbError("TOPIC_ALREADY_EXISTS");
		stmtInsertTopic.run(topicId, now, createdBy);
	}

	function listTopicSubscribers(topicId: TopicId): PeerId[] {
		return stmtListTopicSubscribers
			.all(topicId)
			.map((r) => r.subscriber_peer_id);
	}

	function subscribeTopic(
		topicId: TopicId,
		subscriberPeerId: PeerId,
		now: IsoTimestamp,
	): void {
		const topic = stmtGetTopic.get(topicId);
		if (!topic) throw new DbError("TOPIC_NOT_FOUND");
		try {
			stmtInsertTopicSubscription.run(topicId, subscriberPeerId, now);
		} catch (err: unknown) {
			if (
				err instanceof Error &&
				err.message.includes("UNIQUE constraint failed")
			) {
				throw new DbError("ALREADY_SUBSCRIBED");
			}
			throw err;
		}
	}

	const unsubscribeTopicTx = db.transaction(
		(topicId: TopicId, subscriberPeerId: PeerId): void => {
			const topic = stmtGetTopic.get(topicId);
			if (!topic) throw new DbError("TOPIC_NOT_FOUND");
			const result = stmtDeleteTopicSubscription.run(topicId, subscriberPeerId);
			if (result.changes === 0) throw new DbError("NOT_SUBSCRIBED");
		},
	);

	function fetchTopicHistory(
		topicId: TopicId,
		limit: number,
		beforeSentAt: IsoTimestamp | null,
	): TopicMessageRow[] {
		const topic = stmtGetTopic.get(topicId);
		if (!topic) throw new DbError("TOPIC_NOT_FOUND");
		return beforeSentAt === null
			? stmtListTopicHistoryAll.all(topicId, limit + 1)
			: stmtListTopicHistoryBefore.all(topicId, beforeSentAt, limit + 1);
	}

	function fetchTopicDetail(topicId: TopicId): TopicDetailRow {
		const topic = stmtGetTopic.get(topicId);
		if (!topic) throw new DbError("TOPIC_NOT_FOUND");
		const rows = stmtListTopicSubscribersWithStats.all(topicId);
		return {
			topicId: topic.id,
			createdBy: topic.created_by,
			createdAt: topic.created_at,
			subscribers: rows.map((r) => ({
				peerId: r.subscriber_peer_id,
				subscribedAt: r.subscribed_at,
				queueDepth: r.queue_depth,
				lastReadAt: r.last_read_at,
			})),
		};
	}

	function listTopicSummaries(): TopicSummaryDto[] {
		const rows = stmtListTopicSummaries.all();
		return rows.map((r) => ({
			topicId: r.id,
			createdBy: r.created_by,
			createdAt: r.created_at,
			subscriberCount: r.subscriber_count,
			lastPublishedAt: r.last_published_at,
		}));
	}

	const topicSendTx = db.transaction(
		(input: TopicSendInput): { deliveredTo: PeerId[] } => {
			const topic = stmtGetTopic.get(input.topicId);
			if (!topic) throw new DbError("TOPIC_NOT_FOUND");

			stmtInsertTopicMessage.run(
				input.topicMessageId,
				input.topicId,
				input.from,
				input.subject,
				input.body,
				input.sentAt,
			);

			const subscribers = stmtListTopicSubscribers
				.all(input.topicId)
				.map((r) => r.subscriber_peer_id)
				.filter((sid) => sid !== input.from);

			if (subscribers.length !== input.deliveryMessageIds.length) {
				throw new Error(
					`deliveryMessageIds count mismatch: expected ${subscribers.length}, got ${input.deliveryMessageIds.length}`,
				);
			}

			subscribers.forEach((subscriberId, i) => {
				stmtInsertMessageWithTopic.run(
					input.deliveryMessageIds[i] as string,
					input.from,
					subscriberId,
					input.subject,
					input.body,
					null,
					input.sentAt,
					input.expiresAt,
					input.topicMessageId,
				);
			});

			return { deliveredTo: subscribers };
		},
	);

	const deleteTopicTx = db.transaction(
		(topicId: TopicId): { deletedMessages: number; deletedSubs: number } => {
			const topic = stmtGetTopic.get(topicId);
			if (!topic) throw new DbError("TOPIC_NOT_FOUND");
			const deletedSubs = stmtCountTopicSubs.get(topicId)?.c ?? 0;
			const deletedMessages = stmtCountTopicFanoutMessages.get(topicId)?.c ?? 0;
			stmtDeleteTopic.run(topicId);
			return { deletedMessages, deletedSubs };
		},
	);

	// peer 삭제 정책: subscriptions 제거, inbox 전체 삭제 (peer 가 사라지면
	// 누구도 read 할 수 없으므로 보관해도 expiration 까지 누수). session 행 자체도 제거.
	const deletePeerTx = db.transaction(
		(peerId: PeerId): { deletedSubs: number; cancelledInflight: number } => {
			const subsResult = stmtDeleteSubsForPeer.run(peerId);
			const cancelledInflight = stmtCountPeerInflightInbox.get(peerId)?.c ?? 0;
			stmtDeletePeerInbox.run(peerId);
			stmtDeleteSession.run(peerId);
			return { deletedSubs: subsResult.changes, cancelledInflight };
		},
	);

	return {
		registerSession,
		unregisterSession: (peerId: PeerId, purge: boolean) =>
			unregisterTx(peerId, purge),
		markDisconnected,
		touchLastSeen,
		updateLastActivity,
		inspectLastActivityAt,
		getSession,
		listSessions,
		insertMessage,
		fetchDeliverable: (
			peerId: PeerId,
			max: number,
			now: IsoTimestamp,
			inFlightUntil: IsoTimestamp,
		) => fetchDeliverableTx(peerId, max, now, inFlightUntil),
		ackMessage,
		expireInFlight,
		deleteExpired,
		createTopic,
		subscribeTopic,
		unsubscribeTopic: (topicId: TopicId, subscriberPeerId: PeerId) =>
			unsubscribeTopicTx(topicId, subscriberPeerId),
		fetchTopicHistory,
		fetchTopicDetail,
		listTopicSubscribers,
		listTopicSummaries,
		topicSend: (input: TopicSendInput) => topicSendTx(input),
		deleteTopic: (topicId: TopicId) => deleteTopicTx(topicId),
		deletePeer: (peerId: PeerId) => deletePeerTx(peerId),
		close: (): void => {
			db.close();
		},
	};
}

export type CcDatabase = ReturnType<typeof openDatabase>;
