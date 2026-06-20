import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import type { ErrorCode } from "../protocol/errors.js";
import type {
	AckRequest,
	AckResponse,
	ChannelCreateRequest,
	ChannelCreateResponse,
	ChannelDto,
	ChannelHistoryRequest,
	ChannelHistoryResponse,
	ChannelMessageDto,
	ChannelMessageId,
	ChannelSendRequest,
	ChannelSendResponse,
	ChannelSubscribeRequest,
	ChannelSubscribeResponse,
	ChannelUnsubscribeRequest,
	ChannelUnsubscribeResponse,
	ListPeersResponse,
	MessageDto,
	MessageId,
	ReadRequest,
	ReadResponse,
	RegisterRequest,
	RegisterResponse,
	SendRequest,
	SendResponse,
	TopicId,
	UnregisterRequest,
	UnregisterResponse,
} from "../protocol/http.js";
import type { DashboardEvent } from "../protocol/sse.js";
import { type CcDatabase, DbError } from "./db.js";

export class BrokerError extends Error {
	constructor(
		public code: ErrorCode,
		message: string,
		public details?: unknown,
	) {
		super(message);
		this.name = "BrokerError";
	}
}

export interface BrokerOptions {
	visibilityTimeoutSec: number;
	ttlDays: number;
	dashboardUrl: string;
	// 테스트에서 monotonic timestamp 주입 hook. production 은 default 사용.
	clock?: () => string;
}

export interface Broker {
	events: EventEmitter;
	register: (req: RegisterRequest) => RegisterResponse;
	unregister: (topicId: TopicId, req: UnregisterRequest) => UnregisterResponse;
	send: (req: SendRequest) => SendResponse;
	read: (req: ReadRequest) => ReadResponse;
	ack: (req: AckRequest) => AckResponse;
	listPeers: () => ListPeersResponse;
	disconnect: (topicId: TopicId) => void;
	channelCreate: (req: ChannelCreateRequest) => ChannelCreateResponse;
	channelSubscribe: (req: ChannelSubscribeRequest) => ChannelSubscribeResponse;
	channelSend: (req: ChannelSendRequest) => ChannelSendResponse;
	channelUnsubscribe: (
		req: ChannelUnsubscribeRequest,
	) => ChannelUnsubscribeResponse;
	channelHistory: (req: ChannelHistoryRequest) => ChannelHistoryResponse;
}

const HISTORY_LIMIT_DEFAULT = 50;
export const HISTORY_LIMIT_MAX = 200;

const READ_MAX_DEFAULT = 50;
const MS_PER_DAY = 24 * 60 * 60 * 1000;
const MS_PER_SEC = 1000;

function plusSeconds(iso: string, sec: number): string {
	return new Date(new Date(iso).getTime() + sec * MS_PER_SEC).toISOString();
}

function plusDays(iso: string, days: number): string {
	return new Date(new Date(iso).getTime() + days * MS_PER_DAY).toISOString();
}

export function createBroker(db: CcDatabase, opts: BrokerOptions): Broker {
	const nowIso = opts.clock ?? ((): string => new Date().toISOString());
	const events = new EventEmitter();
	// dashboard /events 와 /tail 다중 연결 시 listener 가 빠르게 누적되어
	// 정상 동작인데도 MaxListenersExceededWarning 이 stderr 로 새는 것을 방지.
	events.setMaxListeners(0);
	events.on("error", () => {});

	function emit(event: DashboardEvent): void {
		try {
			events.emit(event.type, event);
		} catch {
			// listener threw — broker stays alive
		}
	}

	function register(req: RegisterRequest): RegisterResponse {
		const now = nowIso();
		let peer: ReturnType<typeof db.registerSession>;
		try {
			peer = db.registerSession(req.topicId, now);
		} catch (e) {
			if (e instanceof DbError && e.code === "TOPIC_ALREADY_REGISTERED") {
				throw new BrokerError(
					"TOPIC_ALREADY_REGISTERED",
					`topic '${req.topicId}' is already connected`,
				);
			}
			throw e;
		}
		emit({ type: "session_registered", peer });
		return {
			topicId: req.topicId,
			monitorCommand: `cc-messagebus tail ${req.topicId}`,
			dashboardUrl: opts.dashboardUrl,
		};
	}

	function unregister(
		topicId: string,
		req: UnregisterRequest,
	): UnregisterResponse {
		const existing = db.getSession(topicId);
		if (!existing) {
			throw new BrokerError(
				"TOPIC_NOT_FOUND",
				`topic '${topicId}' is not registered`,
			);
		}
		const result = db.unregisterSession(topicId, req.purgeQueue ?? false);
		emit({ type: "session_disconnected", topicId, at: nowIso() });
		return result;
	}

	function send(req: SendRequest): SendResponse {
		const sender = db.getSession(req.from);
		if (!sender) {
			throw new BrokerError(
				"TOPIC_NOT_FOUND",
				`sender topic '${req.from}' is not registered`,
			);
		}
		const target = db.getSession(req.to);
		if (!target) {
			throw new BrokerError(
				"PEER_NOT_FOUND",
				`target topic '${req.to}' is not registered`,
			);
		}
		const now = nowIso();
		const message: MessageDto = {
			id: randomUUID(),
			from: req.from,
			to: req.to,
			subject: req.subject,
			body: req.body,
			threadId: req.threadId ?? null,
			sentAt: now,
			inFlightUntil: null,
			ackedAt: null,
			expiresAt: plusDays(now, opts.ttlDays),
		};
		db.insertMessage(message);
		db.touchLastSeen(req.from, now);
		db.updateLastActivity(req.from, now);
		emit({ type: "message_sent", message });
		return { messageId: message.id, sentAt: now };
	}

	function read(req: ReadRequest): ReadResponse {
		const session = db.getSession(req.topicId);
		if (!session) {
			throw new BrokerError(
				"TOPIC_NOT_FOUND",
				`topic '${req.topicId}' is not registered`,
			);
		}
		const now = nowIso();
		const inFlightUntil = plusSeconds(now, opts.visibilityTimeoutSec);
		const max = req.max ?? READ_MAX_DEFAULT;
		const messages = db.fetchDeliverable(req.topicId, max, now, inFlightUntil);
		db.touchLastSeen(req.topicId, now);
		db.updateLastActivity(req.topicId, now);
		for (const m of messages) {
			emit({
				type: "message_read",
				messageId: m.id,
				topicId: req.topicId,
				at: now,
			});
		}
		return { messages };
	}

	function ack(req: AckRequest): AckResponse {
		const session = db.getSession(req.topicId);
		if (!session) {
			throw new BrokerError(
				"TOPIC_NOT_FOUND",
				`topic '${req.topicId}' is not registered`,
			);
		}
		const now = nowIso();
		let ackedAt: string;
		try {
			ackedAt = db.ackMessage(req.topicId, req.messageId, now);
		} catch (e) {
			if (e instanceof DbError) {
				if (e.code === "MESSAGE_NOT_FOUND") {
					throw new BrokerError(
						"MESSAGE_NOT_FOUND",
						`message '${req.messageId}' not found for topic '${req.topicId}'`,
					);
				}
				if (e.code === "MESSAGE_NOT_IN_FLIGHT") {
					throw new BrokerError(
						"MESSAGE_NOT_IN_FLIGHT",
						`message '${req.messageId}' is not in-flight (already acked or never read)`,
					);
				}
			}
			throw e;
		}
		db.touchLastSeen(req.topicId, now);
		db.updateLastActivity(req.topicId, now);
		emit({
			type: "message_acked",
			messageId: req.messageId,
			topicId: req.topicId,
			at: ackedAt,
		});
		return { ackedAt };
	}

	function listPeers(): ListPeersResponse {
		return { peers: db.listSessions() };
	}

	function disconnect(topicId: string): void {
		const session = db.getSession(topicId);
		if (!session) return;
		const now = nowIso();
		db.markDisconnected(topicId, now);
		emit({ type: "session_disconnected", topicId, at: now });
	}

	function channelCreate(req: ChannelCreateRequest): ChannelCreateResponse {
		const creator = db.getSession(req.createdBy);
		if (!creator) {
			throw new BrokerError(
				"TOPIC_NOT_FOUND",
				`creator topic '${req.createdBy}' is not registered`,
			);
		}
		const now = nowIso();
		try {
			db.createChannel(req.channelId, req.createdBy, now);
		} catch (e) {
			if (e instanceof DbError && e.code === "CHANNEL_ALREADY_EXISTS") {
				throw new BrokerError(
					"CHANNEL_ALREADY_EXISTS",
					`channel '${req.channelId}' already exists`,
				);
			}
			throw e;
		}
		const channel: ChannelDto = {
			channelId: req.channelId,
			createdBy: req.createdBy,
			createdAt: now,
		};
		db.touchLastSeen(req.createdBy, now);
		emit({ type: "channel_created", channel });
		return { channel };
	}

	function channelSubscribe(
		req: ChannelSubscribeRequest,
	): ChannelSubscribeResponse {
		const subscriber = db.getSession(req.topicId);
		if (!subscriber) {
			throw new BrokerError(
				"TOPIC_NOT_FOUND",
				`subscriber topic '${req.topicId}' is not registered`,
			);
		}
		const now = nowIso();
		try {
			db.subscribeChannel(req.channelId, req.topicId, now);
		} catch (e) {
			if (e instanceof DbError && e.code === "CHANNEL_NOT_FOUND") {
				throw new BrokerError(
					"CHANNEL_NOT_FOUND",
					`channel '${req.channelId}' not found`,
				);
			}
			if (e instanceof DbError && e.code === "ALREADY_SUBSCRIBED") {
				throw new BrokerError(
					"ALREADY_SUBSCRIBED",
					`topic '${req.topicId}' already subscribed to '${req.channelId}'`,
				);
			}
			throw e;
		}
		db.touchLastSeen(req.topicId, now);
		db.updateLastActivity(req.topicId, now);
		emit({
			type: "channel_subscribed",
			channelId: req.channelId,
			topicId: req.topicId,
			at: now,
		});
		return { subscribedAt: now };
	}

	function channelSend(req: ChannelSendRequest): ChannelSendResponse {
		const sender = db.getSession(req.from);
		if (!sender) {
			throw new BrokerError(
				"TOPIC_NOT_FOUND",
				`sender topic '${req.from}' is not registered`,
			);
		}
		const allSubs = db.listChannelSubscribers(req.channelId);
		const deliverTo = allSubs.filter((s) => s !== req.from);
		const now = nowIso();
		const expiresAt = plusDays(now, opts.ttlDays);
		const channelMessageId = randomUUID() as ChannelMessageId;
		const deliveryMessageIds: MessageId[] = deliverTo.map(
			() => randomUUID() as MessageId,
		);
		let result: { deliveredTo: TopicId[] };
		try {
			result = db.channelSend({
				channelMessageId,
				channelId: req.channelId,
				from: req.from,
				subject: req.subject,
				body: req.body,
				sentAt: now,
				expiresAt,
				deliveryMessageIds,
			});
		} catch (e) {
			if (e instanceof DbError && e.code === "CHANNEL_NOT_FOUND") {
				throw new BrokerError(
					"CHANNEL_NOT_FOUND",
					`channel '${req.channelId}' not found`,
				);
			}
			throw e;
		}
		db.touchLastSeen(req.from, now);
		db.updateLastActivity(req.from, now);
		result.deliveredTo.forEach((subscriberId, i) => {
			const fanoutMessage: MessageDto = {
				id: deliveryMessageIds[i] as MessageId,
				from: req.from,
				to: subscriberId,
				subject: req.subject,
				body: req.body,
				threadId: null,
				sentAt: now,
				inFlightUntil: null,
				ackedAt: null,
				expiresAt,
			};
			emit({ type: "message_sent", message: fanoutMessage });
		});
		emit({
			type: "channel_message_published",
			channelId: req.channelId,
			channelMessageId,
			from: req.from,
			deliveredTo: result.deliveredTo,
			sentAt: now,
		});
		return {
			channelMessageId,
			deliveredTo: result.deliveredTo,
			sentAt: now,
		};
	}

	function channelUnsubscribe(
		req: ChannelUnsubscribeRequest,
	): ChannelUnsubscribeResponse {
		const subscriber = db.getSession(req.topicId);
		if (!subscriber) {
			throw new BrokerError(
				"TOPIC_NOT_FOUND",
				`subscriber topic '${req.topicId}' is not registered`,
			);
		}
		const now = nowIso();
		try {
			db.unsubscribeChannel(req.channelId, req.topicId);
		} catch (e) {
			if (e instanceof DbError && e.code === "CHANNEL_NOT_FOUND") {
				throw new BrokerError(
					"CHANNEL_NOT_FOUND",
					`channel '${req.channelId}' not found`,
				);
			}
			if (e instanceof DbError && e.code === "NOT_SUBSCRIBED") {
				throw new BrokerError(
					"NOT_SUBSCRIBED",
					`topic '${req.topicId}' is not subscribed to '${req.channelId}'`,
				);
			}
			throw e;
		}
		db.touchLastSeen(req.topicId, now);
		emit({
			type: "channel_unsubscribed",
			channelId: req.channelId,
			topicId: req.topicId,
			at: now,
		});
		return { unsubscribedAt: now };
	}

	// PRD `channels.prd.md` "What We're NOT Building": ACL 없음 — 누구나 read 가능.
	// `requireTopicId` 없이 anonymous 호출 허용은 의도된 정책.
	function channelHistory(req: ChannelHistoryRequest): ChannelHistoryResponse {
		const requestedLimit = req.limit ?? HISTORY_LIMIT_DEFAULT;
		if (requestedLimit < 1 || requestedLimit > HISTORY_LIMIT_MAX) {
			throw new BrokerError(
				"VALIDATION_FAILED",
				`limit must be between 1 and ${HISTORY_LIMIT_MAX}`,
			);
		}
		let rows: ReturnType<typeof db.fetchChannelHistory>;
		try {
			rows = db.fetchChannelHistory(
				req.channelId,
				requestedLimit,
				req.beforeSentAt ?? null,
			);
		} catch (e) {
			if (e instanceof DbError && e.code === "CHANNEL_NOT_FOUND") {
				throw new BrokerError(
					"CHANNEL_NOT_FOUND",
					`channel '${req.channelId}' not found`,
				);
			}
			throw e;
		}
		const hasMore = rows.length > requestedLimit;
		const trimmed = hasMore ? rows.slice(0, requestedLimit) : rows;
		const messages: ChannelMessageDto[] = trimmed.map((row) => ({
			channelMessageId: row.id as ChannelMessageId,
			channelId: row.channel_id,
			from: row.from_topic_id,
			subject: row.subject,
			body: row.body,
			sentAt: row.sent_at,
			// expiresAt 은 `channel_messages` 컬럼이 아니라 sent_at + ttlDays 로 derive.
			// TTL 정책이 바뀌면 historic 메시지의 응답 expiresAt 도 함께 바뀐다 — 의도된 설계.
			expiresAt: plusDays(row.sent_at, opts.ttlDays),
		}));
		return { messages, hasMore };
	}

	return {
		events,
		register,
		unregister,
		send,
		read,
		ack,
		listPeers,
		disconnect,
		channelCreate,
		channelSubscribe,
		channelSend,
		channelUnsubscribe,
		channelHistory,
	};
}
