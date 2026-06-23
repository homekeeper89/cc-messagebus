import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Fastify, { type FastifyError, type FastifyInstance } from "fastify";
import { type ErrorCode, errorCodeToHttpStatus } from "../protocol/errors.js";
import {
	type AckRequest,
	HTTP_ENDPOINTS,
	type ReadRequest,
	type RegisterRequest,
	type SendRequest,
	type TopicCreateRequest,
	type TopicDetailRequest,
	type TopicHistoryRequest,
	type TopicSendRequest,
	type TopicSubscribeRequest,
	type TopicUnsubscribeRequest,
	type UnregisterRequest,
} from "../protocol/http.js";
import {
	DASHBOARD_EVENT_TYPES,
	type DashboardEvent,
	type MessageSentEvent,
	SSE_HEARTBEAT_INTERVAL_SEC,
	serializeSseEvent,
} from "../protocol/sse.js";
import {
	type Broker,
	BrokerError,
	createBroker,
	HISTORY_LIMIT_MAX,
} from "./broker.js";
import { type CleanupHandle, startCleanup } from "./cleanup.js";
import { type CcDatabase, openDatabase } from "./db.js";

const DEFAULT_HOST = "127.0.0.1";
const DEFAULT_PORT = 5959;
const DEFAULT_VISIBILITY_TIMEOUT_SEC = 30;
const DEFAULT_TTL_DAYS = 30;
const DEFAULT_CLEANUP_INTERVAL_SEC = 60;

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const DASHBOARD_HTML = readFileSync(
	join(__dirname, "../dashboard/index.html"),
	"utf8",
);

const PEER_ID_SCHEMA = { type: "string", minLength: 1, maxLength: 64 };
const SUBJECT_SCHEMA = { type: "string", minLength: 1, maxLength: 256 };
const BODY_SCHEMA = { type: "string", maxLength: 65536 };
const THREAD_ID_SCHEMA = { type: "string", maxLength: 64 };
const MESSAGE_ID_SCHEMA = { type: "string", minLength: 1, maxLength: 64 };
const TOPIC_ID_SCHEMA = { type: "string", minLength: 1, maxLength: 64 };

const REGISTER_BODY = {
	type: "object",
	properties: { peerId: PEER_ID_SCHEMA },
	required: ["peerId"],
	additionalProperties: false,
};
const UNREGISTER_BODY = {
	type: "object",
	properties: {
		peerId: PEER_ID_SCHEMA,
		purgeQueue: { type: "boolean" },
	},
	required: ["peerId"],
	additionalProperties: false,
};
const SEND_BODY = {
	type: "object",
	properties: {
		from: PEER_ID_SCHEMA,
		to: PEER_ID_SCHEMA,
		subject: SUBJECT_SCHEMA,
		body: BODY_SCHEMA,
		threadId: THREAD_ID_SCHEMA,
	},
	required: ["from", "to", "subject", "body"],
	additionalProperties: false,
};
const READ_BODY = {
	type: "object",
	properties: {
		peerId: PEER_ID_SCHEMA,
		max: { type: "integer", minimum: 1, maximum: 200 },
	},
	required: ["peerId"],
	additionalProperties: false,
};
const ACK_BODY = {
	type: "object",
	properties: {
		peerId: PEER_ID_SCHEMA,
		messageId: MESSAGE_ID_SCHEMA,
	},
	required: ["peerId", "messageId"],
	additionalProperties: false,
};
const LIST_PEERS_BODY = {
	type: "object",
	properties: {},
	additionalProperties: false,
};
const LIST_TOPICS_BODY = {
	type: "object",
	properties: {},
	additionalProperties: false,
};
const TOPIC_CREATE_BODY = {
	type: "object",
	properties: {
		topicId: TOPIC_ID_SCHEMA,
		createdBy: PEER_ID_SCHEMA,
	},
	required: ["topicId", "createdBy"],
	additionalProperties: false,
};
const TOPIC_SUBSCRIBE_BODY = {
	type: "object",
	properties: {
		topicId: TOPIC_ID_SCHEMA,
		peerId: PEER_ID_SCHEMA,
	},
	required: ["topicId", "peerId"],
	additionalProperties: false,
};
const TOPIC_SEND_BODY = {
	type: "object",
	properties: {
		topicId: TOPIC_ID_SCHEMA,
		from: PEER_ID_SCHEMA,
		subject: SUBJECT_SCHEMA,
		body: BODY_SCHEMA,
	},
	required: ["topicId", "from", "subject", "body"],
	additionalProperties: false,
};
const TOPIC_UNSUBSCRIBE_BODY = {
	type: "object",
	properties: {
		topicId: TOPIC_ID_SCHEMA,
		peerId: PEER_ID_SCHEMA,
	},
	required: ["topicId", "peerId"],
	additionalProperties: false,
};
const ISO_TIMESTAMP_SCHEMA = { type: "string", minLength: 1, maxLength: 64 };
const TOPIC_HISTORY_BODY = {
	type: "object",
	properties: {
		topicId: TOPIC_ID_SCHEMA,
		limit: { type: "integer", minimum: 1, maximum: HISTORY_LIMIT_MAX },
		beforeSentAt: ISO_TIMESTAMP_SCHEMA,
	},
	required: ["topicId"],
	additionalProperties: false,
};
const TOPIC_DETAIL_BODY = {
	type: "object",
	properties: { topicId: TOPIC_ID_SCHEMA },
	required: ["topicId"],
	additionalProperties: false,
};

export interface ServerOptions {
	host?: string;
	port?: number;
	dbPath: string;
	visibilityTimeoutSec?: number;
	ttlDays?: number;
	cleanupIntervalSec?: number;
	dashboardUrl?: string;
	logger?: boolean;
}

export interface Server {
	app: FastifyInstance;
	broker: Broker;
	db: CcDatabase;
	start: () => Promise<string>;
	stop: () => Promise<void>;
}

export function createServer(opts: ServerOptions): Server {
	const host = opts.host ?? DEFAULT_HOST;
	const port = opts.port ?? DEFAULT_PORT;
	const visibilityTimeoutSec =
		opts.visibilityTimeoutSec ?? DEFAULT_VISIBILITY_TIMEOUT_SEC;
	const ttlDays = opts.ttlDays ?? DEFAULT_TTL_DAYS;
	const cleanupIntervalSec =
		opts.cleanupIntervalSec ?? DEFAULT_CLEANUP_INTERVAL_SEC;
	const dashboardUrl = opts.dashboardUrl ?? `http://${host}:${port}`;

	const db = openDatabase(opts.dbPath);
	const broker = createBroker(db, {
		visibilityTimeoutSec,
		ttlDays,
		dashboardUrl,
	});
	let cleanup: CleanupHandle | null = null;

	const app = Fastify({
		logger: opts.logger ?? false,
		// SSE long-poll 이 graceful shutdown 을 무한정 막지 않도록 keep-alive 강제 종료
		forceCloseConnections: true,
	});

	app.post<{ Body: RegisterRequest }>(
		HTTP_ENDPOINTS.register.path,
		{ schema: { body: REGISTER_BODY } },
		async (req) => ({ ok: true, ...broker.register(req.body) }),
	);

	app.post<{ Body: UnregisterRequest }>(
		HTTP_ENDPOINTS.unregister.path,
		{ schema: { body: UNREGISTER_BODY } },
		async (req) => ({
			ok: true,
			...broker.unregister(req.body.peerId, req.body),
		}),
	);

	app.post<{ Body: SendRequest }>(
		HTTP_ENDPOINTS.send.path,
		{ schema: { body: SEND_BODY } },
		async (req) => ({ ok: true, ...broker.send(req.body) }),
	);

	app.post<{ Body: ReadRequest }>(
		HTTP_ENDPOINTS.read.path,
		{ schema: { body: READ_BODY } },
		async (req) => ({ ok: true, ...broker.read(req.body) }),
	);

	app.post<{ Body: AckRequest }>(
		HTTP_ENDPOINTS.ack.path,
		{ schema: { body: ACK_BODY } },
		async (req) => ({ ok: true, ...broker.ack(req.body) }),
	);

	app.post(
		HTTP_ENDPOINTS.listPeers.path,
		{ schema: { body: LIST_PEERS_BODY } },
		async () => ({ ok: true, ...broker.listPeers() }),
	);

	app.post(
		HTTP_ENDPOINTS.listTopics.path,
		{ schema: { body: LIST_TOPICS_BODY } },
		async () => ({ ok: true, ...broker.listTopics() }),
	);

	app.post<{ Body: TopicCreateRequest }>(
		HTTP_ENDPOINTS.topicCreate.path,
		{ schema: { body: TOPIC_CREATE_BODY } },
		async (req) => ({ ok: true, ...broker.topicCreate(req.body) }),
	);

	app.post<{ Body: TopicSubscribeRequest }>(
		HTTP_ENDPOINTS.topicSubscribe.path,
		{ schema: { body: TOPIC_SUBSCRIBE_BODY } },
		async (req) => ({ ok: true, ...broker.topicSubscribe(req.body) }),
	);

	app.post<{ Body: TopicSendRequest }>(
		HTTP_ENDPOINTS.topicSend.path,
		{ schema: { body: TOPIC_SEND_BODY } },
		async (req) => ({ ok: true, ...broker.topicSend(req.body) }),
	);

	app.post<{ Body: TopicUnsubscribeRequest }>(
		HTTP_ENDPOINTS.topicUnsubscribe.path,
		{ schema: { body: TOPIC_UNSUBSCRIBE_BODY } },
		async (req) => ({ ok: true, ...broker.topicUnsubscribe(req.body) }),
	);

	app.post<{ Body: TopicHistoryRequest }>(
		HTTP_ENDPOINTS.topicHistory.path,
		{ schema: { body: TOPIC_HISTORY_BODY } },
		async (req) => ({ ok: true, ...broker.topicHistory(req.body) }),
	);

	app.post<{ Body: TopicDetailRequest }>(
		HTTP_ENDPOINTS.topicDetail.path,
		{ schema: { body: TOPIC_DETAIL_BODY } },
		async (req) => ({ ok: true, ...broker.topicDetail(req.body) }),
	);

	app.get<{ Params: { peerId: string } }>(
		"/tail/:peerId",
		async (req, reply) => {
			const { peerId } = req.params;
			const session = db.getSession(peerId);
			if (!session) {
				const code: ErrorCode = "PEER_NOT_FOUND";
				reply.code(errorCodeToHttpStatus[code]).send({
					ok: false,
					error: { code, message: `peer '${peerId}' is not registered` },
				});
				return;
			}

			reply.hijack();
			const raw = reply.raw;
			raw.writeHead(200, {
				"Content-Type": "text/event-stream",
				"Cache-Control": "no-cache, no-transform",
				Connection: "keep-alive",
				"X-Accel-Buffering": "no",
			});

			let closed = false;
			const cleanup = (): void => {
				if (closed) return;
				closed = true;
				clearInterval(heartbeat);
				broker.events.off("message_sent", listener);
			};
			// EPIPE 등 write 실패 시 cleanup 만 수행하고 throw 막음
			const safeWrite = (chunk: string): void => {
				try {
					raw.write(chunk);
				} catch {
					cleanup();
				}
			};

			safeWrite(
				serializeSseEvent({
					type: "heartbeat",
					at: new Date().toISOString(),
				}),
			);

			const listener = (event: MessageSentEvent): void => {
				if (event.message.to !== peerId) return;
				safeWrite(
					serializeSseEvent({
						type: "message_delivered",
						message: event.message,
					}),
				);
			};
			broker.events.on("message_sent", listener);

			const heartbeat = setInterval(() => {
				safeWrite(
					serializeSseEvent({
						type: "heartbeat",
						at: new Date().toISOString(),
					}),
				);
			}, SSE_HEARTBEAT_INTERVAL_SEC * 1000);
			heartbeat.unref();

			raw.on("close", () => {
				cleanup();
				try {
					broker.disconnect(peerId);
				} catch (e) {
					// 서버 shutdown 후 socket close 가 늦게 도착하면 db 가 닫혀있는 race 만 무시.
					// 그 외 예외는 진짜 버그이므로 로깅하지 않고 다시 던져서 에러 핸들러에 노출.
					const msg = e instanceof Error ? e.message : String(e);
					if (!msg.includes("database connection is not open")) throw e;
				}
			});
		},
	);

	app.get("/dashboard", async (_req, reply) => {
		reply.type("text/html; charset=utf-8").send(DASHBOARD_HTML);
	});

	app.get("/events", (_req, reply) => {
		reply.hijack();
		const raw = reply.raw;
		raw.writeHead(200, {
			"Content-Type": "text/event-stream",
			"Cache-Control": "no-cache, no-transform",
			Connection: "keep-alive",
			"X-Accel-Buffering": "no",
		});

		const listened = Object.values(DASHBOARD_EVENT_TYPES).filter(
			(t) => t !== DASHBOARD_EVENT_TYPES.sessionSnapshot,
		);

		let closed = false;
		const cleanup = (): void => {
			if (closed) return;
			closed = true;
			clearInterval(heartbeat);
			for (const t of listened) broker.events.off(t, onEvent);
		};
		const safeWrite = (chunk: string): void => {
			try {
				raw.write(chunk);
			} catch {
				cleanup();
			}
		};

		const onEvent = (event: DashboardEvent): void => {
			safeWrite(serializeSseEvent(event));
		};

		safeWrite(
			serializeSseEvent({
				type: "session_snapshot",
				peers: broker.listPeers().peers,
				at: new Date().toISOString(),
			}),
		);
		safeWrite(
			serializeSseEvent({
				type: "heartbeat",
				at: new Date().toISOString(),
			}),
		);

		for (const t of listened) broker.events.on(t, onEvent);

		const heartbeat = setInterval(() => {
			safeWrite(
				serializeSseEvent({
					type: "heartbeat",
					at: new Date().toISOString(),
				}),
			);
		}, SSE_HEARTBEAT_INTERVAL_SEC * 1000);
		heartbeat.unref();

		raw.on("close", () => {
			cleanup();
		});
	});

	app.setErrorHandler((err: FastifyError, _req, reply) => {
		if (err instanceof BrokerError) {
			const status = errorCodeToHttpStatus[err.code];
			reply.code(status).send({
				ok: false,
				error: { code: err.code, message: err.message, details: err.details },
			});
			return;
		}
		if (
			err.validation ||
			err.code === "FST_ERR_VALIDATION" ||
			err.statusCode === 400
		) {
			const code: ErrorCode = "VALIDATION_FAILED";
			reply.code(errorCodeToHttpStatus[code]).send({
				ok: false,
				error: { code, message: err.message, details: err.validation },
			});
			return;
		}
		const code: ErrorCode = "INTERNAL_ERROR";
		reply.code(errorCodeToHttpStatus[code]).send({
			ok: false,
			error: { code, message: "internal server error" },
		});
	});

	return {
		app,
		broker,
		db,
		start: async (): Promise<string> => {
			const address = await app.listen({ host, port });
			cleanup = startCleanup(broker, db, { intervalSec: cleanupIntervalSec });
			return address;
		},
		stop: async (): Promise<void> => {
			cleanup?.stop();
			cleanup = null;
			await app.close();
			db.close();
		},
	};
}
