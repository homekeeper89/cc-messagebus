import Fastify, { type FastifyError, type FastifyInstance } from "fastify";
import { type ErrorCode, errorCodeToHttpStatus } from "../protocol/errors.js";
import { HTTP_ENDPOINTS } from "../protocol/http.js";
import { type Broker, BrokerError, createBroker } from "./broker.js";
import { type CleanupHandle, startCleanup } from "./cleanup.js";
import { type CcDatabase, openDatabase } from "./db.js";

const DEFAULT_HOST = "127.0.0.1";
const DEFAULT_PORT = 5959;
const DEFAULT_VISIBILITY_TIMEOUT_SEC = 30;
const DEFAULT_TTL_DAYS = 30;
const DEFAULT_CLEANUP_INTERVAL_SEC = 60;

const TOPIC_ID_SCHEMA = { type: "string", minLength: 1, maxLength: 64 };
const SUBJECT_SCHEMA = { type: "string", minLength: 1, maxLength: 256 };
const BODY_SCHEMA = { type: "string", maxLength: 65536 };
const THREAD_ID_SCHEMA = { type: "string", maxLength: 64 };
const MESSAGE_ID_SCHEMA = { type: "string", minLength: 1, maxLength: 64 };

const REGISTER_BODY = {
	type: "object",
	properties: { topicId: TOPIC_ID_SCHEMA },
	required: ["topicId"],
	additionalProperties: false,
};
const UNREGISTER_BODY = {
	type: "object",
	properties: {
		topicId: TOPIC_ID_SCHEMA,
		purgeQueue: { type: "boolean" },
	},
	required: ["topicId"],
	additionalProperties: false,
};
const SEND_BODY = {
	type: "object",
	properties: {
		from: TOPIC_ID_SCHEMA,
		to: TOPIC_ID_SCHEMA,
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
		topicId: TOPIC_ID_SCHEMA,
		max: { type: "integer", minimum: 1, maximum: 200 },
	},
	required: ["topicId"],
	additionalProperties: false,
};
const ACK_BODY = {
	type: "object",
	properties: {
		topicId: TOPIC_ID_SCHEMA,
		messageId: MESSAGE_ID_SCHEMA,
	},
	required: ["topicId", "messageId"],
	additionalProperties: false,
};
const LIST_PEERS_BODY = {
	type: "object",
	properties: {},
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

	const app = Fastify({ logger: opts.logger ?? false });

	app.post(
		HTTP_ENDPOINTS.register.path,
		{ schema: { body: REGISTER_BODY } },
		async (req) => ({ ok: true, ...broker.register(req.body as never) }),
	);

	app.post(
		HTTP_ENDPOINTS.unregister.path,
		{ schema: { body: UNREGISTER_BODY } },
		async (req) => {
			const body = req.body as { topicId: string; purgeQueue?: boolean };
			return { ok: true, ...broker.unregister(body.topicId, body) };
		},
	);

	app.post(
		HTTP_ENDPOINTS.send.path,
		{ schema: { body: SEND_BODY } },
		async (req) => ({ ok: true, ...broker.send(req.body as never) }),
	);

	app.post(
		HTTP_ENDPOINTS.read.path,
		{ schema: { body: READ_BODY } },
		async (req) => ({ ok: true, ...broker.read(req.body as never) }),
	);

	app.post(
		HTTP_ENDPOINTS.ack.path,
		{ schema: { body: ACK_BODY } },
		async (req) => ({ ok: true, ...broker.ack(req.body as never) }),
	);

	app.post(
		HTTP_ENDPOINTS.listPeers.path,
		{ schema: { body: LIST_PEERS_BODY } },
		async () => ({ ok: true, ...broker.listPeers() }),
	);

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
