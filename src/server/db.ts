import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import Database from "better-sqlite3";
import type {
	IsoTimestamp,
	MessageDto,
	MessageId,
	PeerDto,
	TopicId,
} from "../protocol/http.js";
import { SessionStatus } from "../protocol/http.js";

export type DbErrorCode =
	| "TOPIC_ALREADY_REGISTERED"
	| "MESSAGE_NOT_FOUND"
	| "MESSAGE_NOT_IN_FLIGHT"
	| "CHANNEL_NOT_FOUND"
	| "CHANNEL_ALREADY_EXISTS"
	| "ALREADY_SUBSCRIBED"
	| "NOT_SUBSCRIBED";

export class DbError extends Error {
	constructor(public code: DbErrorCode) {
		super(code);
		this.name = "DbError";
	}
}

const MIGRATION = `
CREATE TABLE IF NOT EXISTS sessions (
	topic_id TEXT PRIMARY KEY,
	connected_at TEXT NOT NULL,
	last_seen_at TEXT NOT NULL,
	status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
	id TEXT PRIMARY KEY,
	from_topic TEXT NOT NULL,
	to_topic TEXT NOT NULL,
	subject TEXT NOT NULL,
	body TEXT NOT NULL,
	thread_id TEXT,
	sent_at TEXT NOT NULL,
	in_flight_until TEXT,
	acked_at TEXT,
	expires_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_delivery
	ON messages (to_topic, acked_at, in_flight_until);

CREATE INDEX IF NOT EXISTS idx_messages_expires
	ON messages (expires_at);

CREATE TABLE IF NOT EXISTS channels (
	id TEXT PRIMARY KEY,
	created_at TEXT NOT NULL,
	created_by TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS channel_subscriptions (
	channel_id TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
	subscriber_topic_id TEXT NOT NULL,
	subscribed_at TEXT NOT NULL,
	PRIMARY KEY (channel_id, subscriber_topic_id)
);

CREATE TABLE IF NOT EXISTS channel_messages (
	id TEXT PRIMARY KEY,
	channel_id TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
	from_topic_id TEXT NOT NULL,
	subject TEXT NOT NULL,
	body TEXT NOT NULL,
	sent_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_channel_messages_channel_sent
	ON channel_messages (channel_id, sent_at);

CREATE INDEX IF NOT EXISTS idx_channel_subscriptions_subscriber
	ON channel_subscriptions (subscriber_topic_id);
`;

interface SessionRow {
	topic_id: string;
	connected_at: string;
	last_seen_at: string;
	status: string;
}

interface MessageRow {
	id: string;
	from_topic: string;
	to_topic: string;
	subject: string;
	body: string;
	thread_id: string | null;
	sent_at: string;
	in_flight_until: string | null;
	acked_at: string | null;
	expires_at: string;
	channel_message_id: string | null;
}

interface ChannelRow {
	id: string;
	created_at: string;
	created_by: string;
}

export interface ChannelSendInput {
	channelMessageId: string;
	channelId: string;
	from: TopicId;
	subject: string;
	body: string;
	sentAt: IsoTimestamp;
	expiresAt: IsoTimestamp;
	deliveryMessageIds: string[];
}

function rowToPeer(row: SessionRow, queueLength: number): PeerDto {
	return {
		topicId: row.topic_id,
		status: row.status as PeerDto["status"],
		connectedAt: row.connected_at,
		lastSeenAt: row.last_seen_at,
		queueLength,
	};
}

function rowToMessage(row: MessageRow): MessageDto {
	return {
		id: row.id,
		from: row.from_topic,
		to: row.to_topic,
		subject: row.subject,
		body: row.body,
		threadId: row.thread_id,
		sentAt: row.sent_at,
		inFlightUntil: row.in_flight_until,
		ackedAt: row.acked_at,
		expiresAt: row.expires_at,
	};
}

export function openDatabase(dbPath: string) {
	mkdirSync(dirname(dbPath), { recursive: true });
	const db = new Database(dbPath);
	db.pragma("journal_mode = WAL");
	db.pragma("foreign_keys = ON");
	db.exec(MIGRATION);

	const messagesCols = db
		.prepare<[], { name: string }>("PRAGMA table_info(messages)")
		.all();
	const hasChannelMessageId = messagesCols.some(
		(c) => c.name === "channel_message_id",
	);
	if (!hasChannelMessageId) {
		db.exec(
			"ALTER TABLE messages ADD COLUMN channel_message_id TEXT REFERENCES channel_messages(id) ON DELETE SET NULL",
		);
	}

	const stmtGetSession = db.prepare<[string], SessionRow>(
		"SELECT * FROM sessions WHERE topic_id = ?",
	);
	const stmtInsertSession = db.prepare<[string, string, string, string]>(
		"INSERT INTO sessions (topic_id, connected_at, last_seen_at, status) VALUES (?, ?, ?, ?)",
	);
	const stmtReactivateSession = db.prepare<[string, string, string, string]>(
		"UPDATE sessions SET status = ?, connected_at = ?, last_seen_at = ? WHERE topic_id = ?",
	);
	const stmtDeleteSession = db.prepare<[string]>(
		"DELETE FROM sessions WHERE topic_id = ?",
	);
	const stmtPurgeQueue = db.prepare<[string]>(
		"DELETE FROM messages WHERE to_topic = ?",
	);
	const stmtMarkDisconnected = db.prepare<[string, string, string]>(
		"UPDATE sessions SET status = ?, last_seen_at = ? WHERE topic_id = ?",
	);
	const stmtTouchLastSeen = db.prepare<[string, string]>(
		"UPDATE sessions SET last_seen_at = ? WHERE topic_id = ?",
	);
	const stmtListSessionsWithQueue = db.prepare<
		[],
		SessionRow & { queue_length: number }
	>(
		`SELECT s.*, COALESCE(q.cnt, 0) AS queue_length
		 FROM sessions s
		 LEFT JOIN (
		   SELECT to_topic, COUNT(*) AS cnt
		   FROM messages
		   WHERE acked_at IS NULL
		   GROUP BY to_topic
		 ) q ON q.to_topic = s.topic_id
		 ORDER BY s.connected_at ASC`,
	);
	const stmtCountQueue = db.prepare<[string], { c: number }>(
		"SELECT COUNT(*) AS c FROM messages WHERE to_topic = ? AND acked_at IS NULL",
	);
	const stmtInsertMessage = db.prepare<
		[string, string, string, string, string, string | null, string, string]
	>(
		"INSERT INTO messages (id, from_topic, to_topic, subject, body, thread_id, sent_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
	);
	const stmtSelectDeliverable = db.prepare<
		[string, string, number],
		MessageRow
	>(
		`SELECT * FROM messages
		 WHERE to_topic = ?
		   AND acked_at IS NULL
		   AND (in_flight_until IS NULL OR in_flight_until <= ?)
		 ORDER BY sent_at ASC
		 LIMIT ?`,
	);
	const stmtSetInFlight = db.prepare<[string, string]>(
		"UPDATE messages SET in_flight_until = ? WHERE id = ?",
	);
	const stmtGetMessage = db.prepare<[string, string], MessageRow>(
		"SELECT * FROM messages WHERE id = ? AND to_topic = ?",
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
	const stmtInsertChannel = db.prepare<[string, string, string]>(
		"INSERT INTO channels (id, created_at, created_by) VALUES (?, ?, ?)",
	);
	const stmtGetChannel = db.prepare<[string], ChannelRow>(
		"SELECT * FROM channels WHERE id = ?",
	);
	const stmtInsertChannelSubscription = db.prepare<[string, string, string]>(
		"INSERT INTO channel_subscriptions (channel_id, subscriber_topic_id, subscribed_at) VALUES (?, ?, ?)",
	);
	const stmtListChannelSubscribers = db.prepare<
		[string],
		{ subscriber_topic_id: string }
	>(
		"SELECT subscriber_topic_id FROM channel_subscriptions WHERE channel_id = ?",
	);
	const stmtInsertChannelMessage = db.prepare<
		[string, string, string, string, string, string]
	>(
		"INSERT INTO channel_messages (id, channel_id, from_topic_id, subject, body, sent_at) VALUES (?, ?, ?, ?, ?, ?)",
	);
	const stmtInsertMessageWithChannel = db.prepare<
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
		"INSERT INTO messages (id, from_topic, to_topic, subject, body, thread_id, sent_at, expires_at, channel_message_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
	);

	function registerSession(topicId: TopicId, now: IsoTimestamp): PeerDto {
		const existing = stmtGetSession.get(topicId);
		if (existing) {
			if (existing.status === SessionStatus.CONNECTED) {
				throw new DbError("TOPIC_ALREADY_REGISTERED");
			}
			stmtReactivateSession.run(SessionStatus.CONNECTED, now, now, topicId);
		} else {
			stmtInsertSession.run(topicId, now, now, SessionStatus.CONNECTED);
		}
		return {
			topicId,
			status: SessionStatus.CONNECTED,
			connectedAt: now,
			lastSeenAt: now,
			queueLength: stmtCountQueue.get(topicId)?.c ?? 0,
		};
	}

	const unregisterTx = db.transaction(
		(topicId: TopicId, purge: boolean): { purged: boolean } => {
			stmtDeleteSession.run(topicId);
			if (purge) stmtPurgeQueue.run(topicId);
			return { purged: purge };
		},
	);

	function markDisconnected(topicId: TopicId, now: IsoTimestamp): void {
		stmtMarkDisconnected.run(SessionStatus.DISCONNECTED, now, topicId);
	}

	function touchLastSeen(topicId: TopicId, now: IsoTimestamp): void {
		stmtTouchLastSeen.run(now, topicId);
	}

	function getSession(topicId: TopicId): PeerDto | null {
		const row = stmtGetSession.get(topicId);
		if (!row) return null;
		return rowToPeer(row, stmtCountQueue.get(topicId)?.c ?? 0);
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
			topicId: TopicId,
			max: number,
			now: IsoTimestamp,
			inFlightUntil: IsoTimestamp,
		): MessageDto[] => {
			const rows = stmtSelectDeliverable.all(topicId, now, max);
			return rows.map((row) => {
				stmtSetInFlight.run(inFlightUntil, row.id);
				return { ...rowToMessage(row), inFlightUntil };
			});
		},
	);

	function ackMessage(
		topicId: TopicId,
		messageId: MessageId,
		now: IsoTimestamp,
	): IsoTimestamp {
		const row = stmtGetMessage.get(messageId, topicId);
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

	function createChannel(
		channelId: string,
		createdBy: TopicId,
		now: IsoTimestamp,
	): void {
		const existing = stmtGetChannel.get(channelId);
		if (existing) throw new DbError("CHANNEL_ALREADY_EXISTS");
		stmtInsertChannel.run(channelId, now, createdBy);
	}

	function listChannelSubscribers(channelId: string): TopicId[] {
		return stmtListChannelSubscribers
			.all(channelId)
			.map((r) => r.subscriber_topic_id);
	}

	function subscribeChannel(
		channelId: string,
		subscriberTopicId: TopicId,
		now: IsoTimestamp,
	): void {
		const channel = stmtGetChannel.get(channelId);
		if (!channel) throw new DbError("CHANNEL_NOT_FOUND");
		try {
			stmtInsertChannelSubscription.run(channelId, subscriberTopicId, now);
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

	const channelSendTx = db.transaction(
		(input: ChannelSendInput): { deliveredTo: TopicId[] } => {
			const channel = stmtGetChannel.get(input.channelId);
			if (!channel) throw new DbError("CHANNEL_NOT_FOUND");

			stmtInsertChannelMessage.run(
				input.channelMessageId,
				input.channelId,
				input.from,
				input.subject,
				input.body,
				input.sentAt,
			);

			const subscribers = stmtListChannelSubscribers
				.all(input.channelId)
				.map((r) => r.subscriber_topic_id)
				.filter((sid) => sid !== input.from);

			if (subscribers.length !== input.deliveryMessageIds.length) {
				throw new Error(
					`deliveryMessageIds count mismatch: expected ${subscribers.length}, got ${input.deliveryMessageIds.length}`,
				);
			}

			subscribers.forEach((subscriberId, i) => {
				stmtInsertMessageWithChannel.run(
					input.deliveryMessageIds[i] as string,
					input.from,
					subscriberId,
					input.subject,
					input.body,
					null,
					input.sentAt,
					input.expiresAt,
					input.channelMessageId,
				);
			});

			return { deliveredTo: subscribers };
		},
	);

	return {
		registerSession,
		unregisterSession: (topicId: TopicId, purge: boolean) =>
			unregisterTx(topicId, purge),
		markDisconnected,
		touchLastSeen,
		getSession,
		listSessions,
		insertMessage,
		fetchDeliverable: (
			topicId: TopicId,
			max: number,
			now: IsoTimestamp,
			inFlightUntil: IsoTimestamp,
		) => fetchDeliverableTx(topicId, max, now, inFlightUntil),
		ackMessage,
		expireInFlight,
		deleteExpired,
		createChannel,
		subscribeChannel,
		listChannelSubscribers,
		channelSend: (input: ChannelSendInput) => channelSendTx(input),
		close: (): void => {
			db.close();
		},
	};
}

export type CcDatabase = ReturnType<typeof openDatabase>;
