import type {
	AckResponse,
	ListPeersResponse,
	MessageId,
	ReadResponse,
	RegisterResponse,
	SendResponse,
	ThreadId,
	TopicId,
	UnregisterResponse,
} from "./http.js";

export const MCP_TOOL_NAMES = {
	register: "register",
	unregister: "unregister",
	send: "send",
	read: "read",
	ack: "ack",
	listPeers: "list_peers",
} as const;

export type McpToolKey = keyof typeof MCP_TOOL_NAMES;

// MCP tool inputs OMIT the caller's topicId where applicable. The MCP
// adapter is stateful (stores topicId at register time) and injects it
// when forwarding to HTTP RPC. The HTTP layer itself stays stateless.

export interface RegisterToolInput {
	topicId: TopicId;
}
export type RegisterToolOutput = RegisterResponse;

export interface UnregisterToolInput {
	purgeQueue?: boolean;
}
export type UnregisterToolOutput = UnregisterResponse;

export interface SendToolInput {
	to: TopicId;
	subject: string;
	body: string;
	threadId?: ThreadId;
}
export type SendToolOutput = SendResponse;

export interface ReadToolInput {
	max?: number;
}
export type ReadToolOutput = ReadResponse;

export interface AckToolInput {
	messageId: MessageId;
}
export type AckToolOutput = AckResponse;

export type ListPeersToolInput = Record<string, never>;
export type ListPeersToolOutput = ListPeersResponse;

// WARNING: The `register` description below is wire-critical. Phase 6
// relies on Claude reading it and auto-invoking the Monitor tool with
// the returned monitorCommand. Do not paraphrase or translate without
// retesting model behavior.
export const MCP_TOOL_DESCRIPTIONS: Record<McpToolKey, string> = {
	register:
		"Register this Claude session under a topicId on the cc-messagebus broker. After register succeeds, you MUST invoke the Monitor tool with the returned `monitorCommand` so that incoming messages are delivered to this session.",
	unregister:
		"Unregister the current session from the broker. By default the message queue is preserved; pass purgeQueue=true to delete it.",
	send: "Send a message to another registered topic. Target may be offline — the broker queues until delivery.",
	read: "Fetch unacked messages for this session. Each returned message enters in-flight state and must be ack-ed within the visibility timeout (default 30s) or it will be redelivered.",
	ack: "Acknowledge a previously read message by id. Until ack, the message stays in-flight and may be redelivered.",
	listPeers: "List all registered topics and their connection status.",
};

export type JsonSchema = Record<string, unknown>;

const TOPIC_ID_SCHEMA = { type: "string", minLength: 1, maxLength: 64 };
const SUBJECT_SCHEMA = { type: "string", minLength: 1, maxLength: 256 };
const BODY_SCHEMA = { type: "string", maxLength: 65536 };
const THREAD_ID_SCHEMA = { type: "string", maxLength: 64 };
const MESSAGE_ID_SCHEMA = { type: "string", minLength: 1, maxLength: 64 };

export const MCP_INPUT_SCHEMAS: Record<McpToolKey, JsonSchema> = {
	register: {
		type: "object",
		properties: { topicId: TOPIC_ID_SCHEMA },
		required: ["topicId"],
		additionalProperties: false,
	},
	unregister: {
		type: "object",
		properties: { purgeQueue: { type: "boolean" } },
		additionalProperties: false,
	},
	send: {
		type: "object",
		properties: {
			to: TOPIC_ID_SCHEMA,
			subject: SUBJECT_SCHEMA,
			body: BODY_SCHEMA,
			threadId: THREAD_ID_SCHEMA,
		},
		required: ["to", "subject", "body"],
		additionalProperties: false,
	},
	read: {
		type: "object",
		properties: { max: { type: "integer", minimum: 1, maximum: 200 } },
		additionalProperties: false,
	},
	ack: {
		type: "object",
		properties: { messageId: MESSAGE_ID_SCHEMA },
		required: ["messageId"],
		additionalProperties: false,
	},
	listPeers: { type: "object", properties: {}, additionalProperties: false },
};
