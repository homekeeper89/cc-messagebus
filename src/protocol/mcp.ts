import type {
	AckResponse,
	IsoTimestamp,
	ListPeersResponse,
	ListTopicsResponse,
	MessageId,
	PeerId,
	ReadResponse,
	RegisterResponse,
	TopicCreateResponse,
	TopicDetailResponse,
	TopicHistoryResponse,
	TopicId,
	TopicMonitorResponse,
	TopicSendResponse,
	TopicSubscribeResponse,
	TopicUnsubscribeResponse,
	UnregisterResponse,
} from "./http.js";

// NOTE: `send` (1:1 DM) tool 은 0.3.0 PR-D 부터 MCP 노출에서 제외.
// agent 가 1:1 DM 으로 보내는 문제 차단이 목적. HTTP /send RPC,
// read/ack inbox 인프라는 유지 (topic delivery 가 의존하고 운영자/디버깅용).
export const MCP_TOOL_NAMES = {
	register: "register",
	unregister: "unregister",
	read: "read",
	ack: "ack",
	listPeers: "list_peers",
	listTopics: "list_topics",
	topicCreate: "topic_create",
	topicSubscribe: "topic_subscribe",
	topicSend: "topic_send",
	topicUnsubscribe: "topic_unsubscribe",
	topicHistory: "topic_history",
	topicDetail: "topic_detail",
	topicMonitor: "topic_monitor",
} as const;

export type McpToolKey = keyof typeof MCP_TOOL_NAMES;

// MCP tool inputs OMIT the caller's peerId where applicable. The MCP
// adapter is stateful (stores peerId at register time) and injects it
// when forwarding to HTTP RPC. The HTTP layer itself stays stateless.

export interface RegisterToolInput {
	peerId: PeerId;
}
export type RegisterToolOutput = RegisterResponse;

export interface UnregisterToolInput {
	purgeQueue?: boolean;
}
export type UnregisterToolOutput = UnregisterResponse;

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

export type ListTopicsToolInput = Record<string, never>;
export type ListTopicsToolOutput = ListTopicsResponse;

export interface TopicCreateToolInput {
	topicId: TopicId;
}
export type TopicCreateToolOutput = TopicCreateResponse;

export interface TopicSubscribeToolInput {
	topicId: TopicId;
}
export type TopicSubscribeToolOutput = TopicSubscribeResponse;

export interface TopicSendToolInput {
	topicId: TopicId;
	subject: string;
	body: string;
}
export type TopicSendToolOutput = TopicSendResponse;

export interface TopicUnsubscribeToolInput {
	topicId: TopicId;
}
export type TopicUnsubscribeToolOutput = TopicUnsubscribeResponse;

export interface TopicHistoryToolInput {
	topicId: TopicId;
	limit?: number;
	beforeSentAt?: IsoTimestamp;
}
export type TopicHistoryToolOutput = TopicHistoryResponse;

export interface TopicDetailToolInput {
	topicId: TopicId;
}
export type TopicDetailToolOutput = TopicDetailResponse;

export interface TopicMonitorToolInput {
	topicId: TopicId;
	max?: number;
}
export type TopicMonitorToolOutput = TopicMonitorResponse;

// WARNING: The `register` description AND the `topic_subscribe`
// description below are wire-critical. The register flow relies on
// Claude auto-invoking Monitor; the topic_subscribe flow relies on
// Claude understanding that no replay happens and that history must be
// pulled via topic_history. Do not paraphrase or translate without
// retesting model behavior.
export const MCP_TOOL_DESCRIPTIONS: Record<McpToolKey, string> = {
	register:
		"Register this Claude session under a peerId on the cc-messagebus broker. After register succeeds, you MUST invoke the Monitor tool with the returned `monitorCommand` so that incoming messages are delivered to this session.",
	unregister:
		"Unregister the current session from the broker. By default the message queue is preserved; pass purgeQueue=true to delete it.",
	read: "Fetch unacked messages for this session. Each returned message enters in-flight state and must be ack-ed within the visibility timeout (default 30s) or it will be redelivered.",
	ack: "Acknowledge a previously read message by id. Until ack, the message stays in-flight and may be redelivered.",
	listPeers: "List all registered peers and their connection status.",
	listTopics:
		"List all pub-sub topics with subscriber count and last published timestamp. Sorted by most-recently-active first; topics with no messages appear last (creation time as tie-breaker).",
	topicCreate:
		"Create a new pub-sub topic. The current session's peerId is recorded as createdBy. Returns TOPIC_ALREADY_EXISTS if the topicId is taken.",
	topicSubscribe:
		"Subscribe the current session's peerId to a topic. After subscribe succeeds, future topic_send messages will arrive in this session's inbox via the same read/ack flow used internally — the existing Monitor process keeps delivering them. Does NOT replay past messages; use topic_history for that. Returns ALREADY_SUBSCRIBED on duplicate.",
	topicSend:
		"Publish a message to all current subscribers of the topic. The broker performs an atomic fan-out: 1 canonical row plus N-1 inbox copies (the publisher itself is excluded from delivery). Returns the canonical topicMessageId and the list of recipient peerIds.",
	topicUnsubscribe:
		"Unsubscribe the current session's peerId from a topic. Already-delivered messages in the inbox are preserved (still ackable). Returns NOT_SUBSCRIBED if no active subscription.",
	topicHistory:
		"Pull past canonical messages of a topic for late-joining context. Returns up to `limit` messages (default broker-decided) ordered by sentAt desc. Use `beforeSentAt` as a cursor for pagination.",
	topicDetail:
		"Inspect a topic's subscribers with per-subscriber queue stats (queueDepth, lastReadAt). No ACL — any session can read. Returns TOPIC_NOT_FOUND if the topicId does not exist.",
	topicMonitor:
		"Fetch unread topic messages since this session's last cursor and advance the cursor atomically. Returns up to `max` messages (default broker-decided) ordered by sentAt asc, plus the new cursor. Requires an active subscription (NOT_SUBSCRIBED otherwise). Unlike topic_history (which is read-only browsing), topic_monitor mutates per-subscriber cursor state — call it repeatedly to drain the topic.",
};

export type JsonSchema = Record<string, unknown>;

const PEER_ID_SCHEMA = { type: "string", minLength: 1, maxLength: 64 };
const SUBJECT_SCHEMA = { type: "string", minLength: 1, maxLength: 256 };
const BODY_SCHEMA = { type: "string", maxLength: 65536 };
const MESSAGE_ID_SCHEMA = { type: "string", minLength: 1, maxLength: 64 };
const TOPIC_ID_SCHEMA = { type: "string", minLength: 1, maxLength: 64 };

export const MCP_INPUT_SCHEMAS: Record<McpToolKey, JsonSchema> = {
	register: {
		type: "object",
		properties: { peerId: PEER_ID_SCHEMA },
		required: ["peerId"],
		additionalProperties: false,
	},
	unregister: {
		type: "object",
		properties: { purgeQueue: { type: "boolean" } },
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
	listTopics: {
		type: "object",
		properties: {},
		additionalProperties: false,
	},
	topicCreate: {
		type: "object",
		properties: { topicId: TOPIC_ID_SCHEMA },
		required: ["topicId"],
		additionalProperties: false,
	},
	topicSubscribe: {
		type: "object",
		properties: { topicId: TOPIC_ID_SCHEMA },
		required: ["topicId"],
		additionalProperties: false,
	},
	topicSend: {
		type: "object",
		properties: {
			topicId: TOPIC_ID_SCHEMA,
			subject: SUBJECT_SCHEMA,
			body: BODY_SCHEMA,
		},
		required: ["topicId", "subject", "body"],
		additionalProperties: false,
	},
	topicUnsubscribe: {
		type: "object",
		properties: { topicId: TOPIC_ID_SCHEMA },
		required: ["topicId"],
		additionalProperties: false,
	},
	topicHistory: {
		type: "object",
		properties: {
			topicId: TOPIC_ID_SCHEMA,
			limit: { type: "integer", minimum: 1, maximum: 200 },
			beforeSentAt: { type: "string", minLength: 1, maxLength: 64 },
		},
		required: ["topicId"],
		additionalProperties: false,
	},
	topicDetail: {
		type: "object",
		properties: { topicId: TOPIC_ID_SCHEMA },
		required: ["topicId"],
		additionalProperties: false,
	},
	topicMonitor: {
		type: "object",
		properties: {
			topicId: TOPIC_ID_SCHEMA,
			max: { type: "integer", minimum: 1, maximum: 200 },
		},
		required: ["topicId"],
		additionalProperties: false,
	},
};
