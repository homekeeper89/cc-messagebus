import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import type { ErrorCode } from "../protocol/errors.js";
import type {
	AckRequest,
	AckResponse,
	ListPeersResponse,
	MessageDto,
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
}

const READ_MAX_DEFAULT = 50;
const MS_PER_DAY = 24 * 60 * 60 * 1000;
const MS_PER_SEC = 1000;

function nowIso(): string {
	return new Date().toISOString();
}

function plusSeconds(iso: string, sec: number): string {
	return new Date(new Date(iso).getTime() + sec * MS_PER_SEC).toISOString();
}

function plusDays(iso: string, days: number): string {
	return new Date(new Date(iso).getTime() + days * MS_PER_DAY).toISOString();
}

export function createBroker(db: CcDatabase, opts: BrokerOptions): Broker {
	const events = new EventEmitter();
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

	return {
		events,
		register,
		unregister,
		send,
		read,
		ack,
		listPeers,
		disconnect,
	};
}
