import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import type { ErrorCode } from "../protocol/errors.js";
import type {
	AckRequest,
	AckResponse,
	DiagnosticsResponse,
	IssueCreateRequest,
	IssueCreateResponse,
	ListPeersResponse,
	ListTopicsResponse,
	MessageDto,
	MessageId,
	PeerId,
	ReadRequest,
	ReadResponse,
	RecentErrorEntry,
	RecentRpcEntry,
	RegisterRequest,
	RegisterResponse,
	SendRequest,
	SendResponse,
	TopicCreateRequest,
	TopicCreateResponse,
	TopicDetailRequest,
	TopicDetailResponse,
	TopicDto,
	TopicHistoryRequest,
	TopicHistoryResponse,
	TopicMessageDto,
	TopicMessageId,
	TopicSendRequest,
	TopicSendResponse,
	TopicSubscribeRequest,
	TopicSubscribeResponse,
	TopicUnsubscribeRequest,
	TopicUnsubscribeResponse,
	UnregisterRequest,
	UnregisterResponse,
} from "../protocol/http.js";
import type { DashboardEvent } from "../protocol/sse.js";
import { type CcDatabase, DbError } from "./db.js";
import { type IssueClient, IssueClientError } from "./issue.js";

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
	version: string;
	getDbSizeByte: () => number;
	issueClient: IssueClient | null;
	// 테스트에서 monotonic timestamp 주입 hook. production 은 default 사용.
	clock?: () => string;
	// 테스트에서 uptime 고정용. production 은 Date.now() 기본.
	nowMs?: () => number;
}

export interface Broker {
	events: EventEmitter;
	register: (req: RegisterRequest) => RegisterResponse;
	unregister: (peerId: PeerId, req: UnregisterRequest) => UnregisterResponse;
	send: (req: SendRequest) => SendResponse;
	read: (req: ReadRequest) => ReadResponse;
	ack: (req: AckRequest) => AckResponse;
	listPeers: () => ListPeersResponse;
	listTopics: () => ListTopicsResponse;
	disconnect: (peerId: PeerId) => void;
	topicCreate: (req: TopicCreateRequest) => TopicCreateResponse;
	topicSubscribe: (req: TopicSubscribeRequest) => TopicSubscribeResponse;
	topicSend: (req: TopicSendRequest) => TopicSendResponse;
	topicUnsubscribe: (req: TopicUnsubscribeRequest) => TopicUnsubscribeResponse;
	topicHistory: (req: TopicHistoryRequest) => TopicHistoryResponse;
	topicDetail: (req: TopicDetailRequest) => TopicDetailResponse;
	diagnostics: () => DiagnosticsResponse;
	issueCreate: (req: IssueCreateRequest) => Promise<IssueCreateResponse>;
}

const HISTORY_LIMIT_DEFAULT = 50;
export const HISTORY_LIMIT_MAX = 200;
export const RING_BUFFER_CAPACITY = 50;

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
	const nowMs = opts.nowMs ?? ((): number => Date.now());
	const startedAtMs = nowMs();
	const recentRpcList: RecentRpcEntry[] = [];
	const recentErrorList: RecentErrorEntry[] = [];

	function pushRing<T>(buf: T[], entry: T): void {
		buf.push(entry);
		if (buf.length > RING_BUFFER_CAPACITY) buf.shift();
	}

	function errorMessage(e: unknown): string {
		return e instanceof Error ? e.message : String(e);
	}

	function recordRpc(
		method: string,
		startedMs: number,
		error: unknown,
	): void {
		const at = nowIso();
		pushRing(recentRpcList, {
			method,
			durationMs: nowMs() - startedMs,
			error: error === undefined ? null : errorMessage(error),
			at,
		});
		if (error !== undefined) {
			pushRing(recentErrorList, {
				message: errorMessage(error),
				stack: error instanceof Error ? (error.stack ?? null) : null,
				at,
			});
		}
	}

	function instrument<Args extends unknown[], R>(
		method: string,
		fn: (...args: Args) => R,
	): (...args: Args) => R {
		return (...args: Args): R => {
			const start = nowMs();
			try {
				const result = fn(...args);
				recordRpc(method, start, undefined);
				return result;
			} catch (e) {
				recordRpc(method, start, e);
				throw e;
			}
		};
	}

	function instrumentAsync<Args extends unknown[], R>(
		method: string,
		fn: (...args: Args) => Promise<R>,
	): (...args: Args) => Promise<R> {
		return async (...args: Args): Promise<R> => {
			const start = nowMs();
			try {
				const result = await fn(...args);
				recordRpc(method, start, undefined);
				return result;
			} catch (e) {
				recordRpc(method, start, e);
				throw e;
			}
		};
	}

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
			peer = db.registerSession(req.peerId, now);
		} catch (e) {
			if (e instanceof DbError && e.code === "PEER_ALREADY_REGISTERED") {
				throw new BrokerError(
					"PEER_ALREADY_REGISTERED",
					`peer '${req.peerId}' is already connected`,
				);
			}
			throw e;
		}
		emit({ type: "session_registered", peer });
		return {
			peerId: req.peerId,
			monitorCommand: `cc-messagebus tail ${req.peerId}`,
			dashboardUrl: opts.dashboardUrl,
		};
	}

	function unregister(
		peerId: PeerId,
		req: UnregisterRequest,
	): UnregisterResponse {
		const existing = db.getSession(peerId);
		if (!existing) {
			throw new BrokerError(
				"PEER_NOT_FOUND",
				`peer '${peerId}' is not registered`,
			);
		}
		const result = db.unregisterSession(peerId, req.purgeQueue ?? false);
		emit({ type: "session_disconnected", peerId, at: nowIso() });
		return result;
	}

	function send(req: SendRequest): SendResponse {
		const sender = db.getSession(req.from);
		if (!sender) {
			throw new BrokerError(
				"PEER_NOT_FOUND",
				`sender peer '${req.from}' is not registered`,
			);
		}
		const target = db.getSession(req.to);
		if (!target) {
			throw new BrokerError(
				"PEER_NOT_FOUND",
				`target peer '${req.to}' is not registered`,
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
		emit({ type: "message_sent", message, kind: "dm" });
		return { messageId: message.id, sentAt: now };
	}

	function read(req: ReadRequest): ReadResponse {
		const session = db.getSession(req.peerId);
		if (!session) {
			throw new BrokerError(
				"PEER_NOT_FOUND",
				`peer '${req.peerId}' is not registered`,
			);
		}
		const now = nowIso();
		const inFlightUntil = plusSeconds(now, opts.visibilityTimeoutSec);
		const max = req.max ?? READ_MAX_DEFAULT;
		const messages = db.fetchDeliverable(req.peerId, max, now, inFlightUntil);
		db.touchLastSeen(req.peerId, now);
		db.updateLastActivity(req.peerId, now);
		for (const m of messages) {
			emit({
				type: "message_read",
				messageId: m.id,
				peerId: req.peerId,
				at: now,
			});
		}
		return { messages };
	}

	function ack(req: AckRequest): AckResponse {
		const session = db.getSession(req.peerId);
		if (!session) {
			throw new BrokerError(
				"PEER_NOT_FOUND",
				`peer '${req.peerId}' is not registered`,
			);
		}
		const now = nowIso();
		let ackedAt: string;
		try {
			ackedAt = db.ackMessage(req.peerId, req.messageId, now);
		} catch (e) {
			if (e instanceof DbError) {
				if (e.code === "MESSAGE_NOT_FOUND") {
					throw new BrokerError(
						"MESSAGE_NOT_FOUND",
						`message '${req.messageId}' not found for peer '${req.peerId}'`,
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
		db.touchLastSeen(req.peerId, now);
		db.updateLastActivity(req.peerId, now);
		emit({
			type: "message_acked",
			messageId: req.messageId,
			peerId: req.peerId,
			at: ackedAt,
		});
		return { ackedAt };
	}

	function listPeers(): ListPeersResponse {
		return { peers: db.listSessions() };
	}

	function listTopics(): ListTopicsResponse {
		return { topics: db.listTopicSummaries() };
	}

	function disconnect(peerId: PeerId): void {
		const session = db.getSession(peerId);
		if (!session) return;
		const now = nowIso();
		db.markDisconnected(peerId, now);
		emit({ type: "session_disconnected", peerId, at: now });
	}

	function topicCreate(req: TopicCreateRequest): TopicCreateResponse {
		const creator = db.getSession(req.createdBy);
		if (!creator) {
			throw new BrokerError(
				"PEER_NOT_FOUND",
				`creator peer '${req.createdBy}' is not registered`,
			);
		}
		const now = nowIso();
		try {
			db.createTopic(req.topicId, req.createdBy, now);
		} catch (e) {
			if (e instanceof DbError && e.code === "TOPIC_ALREADY_EXISTS") {
				throw new BrokerError(
					"TOPIC_ALREADY_EXISTS",
					`topic '${req.topicId}' already exists`,
				);
			}
			throw e;
		}
		const topic: TopicDto = {
			topicId: req.topicId,
			createdBy: req.createdBy,
			createdAt: now,
		};
		db.touchLastSeen(req.createdBy, now);
		emit({ type: "topic_created", topic });
		return { topic };
	}

	function topicSubscribe(req: TopicSubscribeRequest): TopicSubscribeResponse {
		const subscriber = db.getSession(req.peerId);
		if (!subscriber) {
			throw new BrokerError(
				"PEER_NOT_FOUND",
				`subscriber peer '${req.peerId}' is not registered`,
			);
		}
		const now = nowIso();
		try {
			db.subscribeTopic(req.topicId, req.peerId, now);
		} catch (e) {
			if (e instanceof DbError && e.code === "TOPIC_NOT_FOUND") {
				throw new BrokerError(
					"TOPIC_NOT_FOUND",
					`topic '${req.topicId}' not found`,
				);
			}
			if (e instanceof DbError && e.code === "ALREADY_SUBSCRIBED") {
				throw new BrokerError(
					"ALREADY_SUBSCRIBED",
					`peer '${req.peerId}' already subscribed to '${req.topicId}'`,
				);
			}
			throw e;
		}
		db.touchLastSeen(req.peerId, now);
		db.updateLastActivity(req.peerId, now);
		emit({
			type: "topic_subscribed",
			topicId: req.topicId,
			peerId: req.peerId,
			at: now,
		});
		return { subscribedAt: now };
	}

	function topicSend(req: TopicSendRequest): TopicSendResponse {
		const sender = db.getSession(req.from);
		if (!sender) {
			throw new BrokerError(
				"PEER_NOT_FOUND",
				`sender peer '${req.from}' is not registered`,
			);
		}
		const allSubs = db.listTopicSubscribers(req.topicId);
		const deliverTo = allSubs.filter((s) => s !== req.from);
		const now = nowIso();
		const expiresAt = plusDays(now, opts.ttlDays);
		const topicMessageId = randomUUID() as TopicMessageId;
		const deliveryMessageIds: MessageId[] = deliverTo.map(
			() => randomUUID() as MessageId,
		);
		let result: { deliveredTo: PeerId[] };
		try {
			result = db.topicSend({
				topicMessageId,
				topicId: req.topicId,
				from: req.from,
				subject: req.subject,
				body: req.body,
				sentAt: now,
				expiresAt,
				deliveryMessageIds,
			});
		} catch (e) {
			if (e instanceof DbError && e.code === "TOPIC_NOT_FOUND") {
				throw new BrokerError(
					"TOPIC_NOT_FOUND",
					`topic '${req.topicId}' not found`,
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
			emit({
				type: "message_sent",
				message: fanoutMessage,
				kind: "topic",
				topicId: req.topicId,
			});
		});
		emit({
			type: "topic_message_published",
			topicId: req.topicId,
			topicMessageId,
			from: req.from,
			deliveredTo: result.deliveredTo,
			sentAt: now,
		});
		return {
			topicMessageId,
			deliveredTo: result.deliveredTo,
			sentAt: now,
		};
	}

	function topicUnsubscribe(
		req: TopicUnsubscribeRequest,
	): TopicUnsubscribeResponse {
		const subscriber = db.getSession(req.peerId);
		if (!subscriber) {
			throw new BrokerError(
				"PEER_NOT_FOUND",
				`subscriber peer '${req.peerId}' is not registered`,
			);
		}
		const now = nowIso();
		try {
			db.unsubscribeTopic(req.topicId, req.peerId);
		} catch (e) {
			if (e instanceof DbError && e.code === "TOPIC_NOT_FOUND") {
				throw new BrokerError(
					"TOPIC_NOT_FOUND",
					`topic '${req.topicId}' not found`,
				);
			}
			if (e instanceof DbError && e.code === "NOT_SUBSCRIBED") {
				throw new BrokerError(
					"NOT_SUBSCRIBED",
					`peer '${req.peerId}' is not subscribed to '${req.topicId}'`,
				);
			}
			throw e;
		}
		db.touchLastSeen(req.peerId, now);
		emit({
			type: "topic_unsubscribed",
			topicId: req.topicId,
			peerId: req.peerId,
			at: now,
		});
		return { unsubscribedAt: now };
	}

	// PRD `channels.prd.md` "What We're NOT Building": ACL 없음 — 누구나 read 가능.
	// `requirePeerId` 없이 anonymous 호출 허용은 의도된 정책.
	function topicHistory(req: TopicHistoryRequest): TopicHistoryResponse {
		const requestedLimit = req.limit ?? HISTORY_LIMIT_DEFAULT;
		if (requestedLimit < 1 || requestedLimit > HISTORY_LIMIT_MAX) {
			throw new BrokerError(
				"VALIDATION_FAILED",
				`limit must be between 1 and ${HISTORY_LIMIT_MAX}`,
			);
		}
		let rows: ReturnType<typeof db.fetchTopicHistory>;
		try {
			rows = db.fetchTopicHistory(
				req.topicId,
				requestedLimit,
				req.beforeSentAt ?? null,
			);
		} catch (e) {
			if (e instanceof DbError && e.code === "TOPIC_NOT_FOUND") {
				throw new BrokerError(
					"TOPIC_NOT_FOUND",
					`topic '${req.topicId}' not found`,
				);
			}
			throw e;
		}
		const hasMore = rows.length > requestedLimit;
		const trimmed = hasMore ? rows.slice(0, requestedLimit) : rows;
		const messages: TopicMessageDto[] = trimmed.map((row) => ({
			topicMessageId: row.id as TopicMessageId,
			topicId: row.topic_id,
			from: row.from_peer_id,
			subject: row.subject,
			body: row.body,
			sentAt: row.sent_at,
			// expiresAt 은 `topic_messages` 컬럼이 아니라 sent_at + ttlDays 로 derive.
			// TTL 정책이 바뀌면 historic 메시지의 응답 expiresAt 도 함께 바뀐다 — 의도된 설계.
			expiresAt: plusDays(row.sent_at, opts.ttlDays),
		}));
		return { messages, hasMore };
	}

	// PRD `channels.prd.md` "What We're NOT Building": ACL 없음 — 누구나 read 가능.
	// `requirePeerId` 없이 anonymous 호출 허용은 의도된 정책.
	function topicDetail(req: TopicDetailRequest): TopicDetailResponse {
		let detail: ReturnType<typeof db.fetchTopicDetail>;
		try {
			detail = db.fetchTopicDetail(req.topicId);
		} catch (e) {
			if (e instanceof DbError && e.code === "TOPIC_NOT_FOUND") {
				throw new BrokerError(
					"TOPIC_NOT_FOUND",
					`topic '${req.topicId}' not found`,
				);
			}
			throw e;
		}
		return { topic: detail };
	}

	function diagnostics(): DiagnosticsResponse {
		return {
			version: opts.version,
			uptimeSec: Math.floor((nowMs() - startedAtMs) / 1000),
			nodeVersion: process.version,
			topicCount: db.listTopicSummaries().length,
			peerCount: db.listSessions().length,
			dbSizeByte: opts.getDbSizeByte(),
			recentRpcList: [...recentRpcList],
			recentErrorList: [...recentErrorList],
		};
	}

	async function issueCreate(
		req: IssueCreateRequest,
	): Promise<IssueCreateResponse> {
		if (!opts.issueClient) {
			throw new BrokerError(
				"ISSUE_REPO_NOT_CONFIGURED",
				"issueRepo is not configured in ~/.cc-messagebus/config.json",
			);
		}
		try {
			const result = await opts.issueClient.create(req);
			return { issueNumber: result.issueNumber, url: result.url };
		} catch (e) {
			if (e instanceof IssueClientError) {
				throw new BrokerError("ISSUE_CLIENT_FAILED", e.message, e.details);
			}
			throw e;
		}
	}

	return {
		events,
		register: instrument("register", register),
		unregister: instrument("unregister", unregister),
		send: instrument("send", send),
		read: instrument("read", read),
		ack: instrument("ack", ack),
		listPeers: instrument("listPeers", listPeers),
		listTopics: instrument("listTopics", listTopics),
		// disconnect 는 SSE close 콜백 — RPC 가 아니므로 ring 기록 제외.
		disconnect,
		topicCreate: instrument("topicCreate", topicCreate),
		topicSubscribe: instrument("topicSubscribe", topicSubscribe),
		topicSend: instrument("topicSend", topicSend),
		topicUnsubscribe: instrument("topicUnsubscribe", topicUnsubscribe),
		topicHistory: instrument("topicHistory", topicHistory),
		topicDetail: instrument("topicDetail", topicDetail),
		// diagnostics 자체는 ring 기록 제외 — snapshot 호출이 자기 자신을 오염시키는 노이즈 방지.
		diagnostics,
		issueCreate: instrumentAsync("issueCreate", issueCreate),
	};
}
