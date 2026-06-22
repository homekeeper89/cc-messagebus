import type {
	AckResponse,
	ChannelCreateResponse,
	ChannelDetailResponse,
	ChannelHistoryResponse,
	ChannelId,
	ChannelSendResponse,
	ChannelSubscribeResponse,
	ChannelUnsubscribeResponse,
	IsoTimestamp,
	ListChannelsResponse,
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
	listChannels: "list_channels",
	channelCreate: "channel_create",
	channelSubscribe: "channel_subscribe",
	channelSend: "channel_send",
	channelUnsubscribe: "channel_unsubscribe",
	channelHistory: "channel_history",
	channelDetail: "channel_detail",
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

export type ListChannelsToolInput = Record<string, never>;
export type ListChannelsToolOutput = ListChannelsResponse;

export interface ChannelCreateToolInput {
	channelId: ChannelId;
}
export type ChannelCreateToolOutput = ChannelCreateResponse;

export interface ChannelSubscribeToolInput {
	channelId: ChannelId;
}
export type ChannelSubscribeToolOutput = ChannelSubscribeResponse;

export interface ChannelSendToolInput {
	channelId: ChannelId;
	subject: string;
	body: string;
}
export type ChannelSendToolOutput = ChannelSendResponse;

export interface ChannelUnsubscribeToolInput {
	channelId: ChannelId;
}
export type ChannelUnsubscribeToolOutput = ChannelUnsubscribeResponse;

export interface ChannelHistoryToolInput {
	channelId: ChannelId;
	limit?: number;
	beforeSentAt?: IsoTimestamp;
}
export type ChannelHistoryToolOutput = ChannelHistoryResponse;

export interface ChannelDetailToolInput {
	channelId: ChannelId;
}
export type ChannelDetailToolOutput = ChannelDetailResponse;

// WARNING: The `register` description AND the `channel_subscribe`
// description below are wire-critical. The register flow relies on
// Claude auto-invoking Monitor; the channel_subscribe flow relies on
// Claude understanding that no replay happens and that history must be
// pulled via channel_history. Do not paraphrase or translate without
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
	listChannels:
		"List all pub-sub channels with subscriber count and last published timestamp. Sorted by most-recently-active first; channels with no messages appear last (creation time as tie-breaker).",
	channelCreate:
		"Create a new pub-sub channel. The current session's topicId is recorded as createdBy. Returns CHANNEL_ALREADY_EXISTS if the channelId is taken.",
	channelSubscribe:
		"Subscribe the current session's topicId to a channel. After subscribe succeeds, future channel_send messages will arrive in this session's inbox via the same read/ack flow as 1:1 messages — the existing Monitor process keeps delivering them. Does NOT replay past messages; use channel_history for that. Returns ALREADY_SUBSCRIBED on duplicate.",
	channelSend:
		"Publish a message to all current subscribers of the channel. The broker performs an atomic fan-out: 1 canonical row plus N-1 inbox copies (the publisher itself is excluded from delivery). Returns the canonical channelMessageId and the list of recipient topicIds.",
	channelUnsubscribe:
		"Unsubscribe the current session's topicId from a channel. Already-delivered messages in the inbox are preserved (still ackable). Returns NOT_SUBSCRIBED if no active subscription.",
	channelHistory:
		"Pull past canonical messages of a channel for late-joining context. Returns up to `limit` messages (default broker-decided) ordered by sentAt desc. Use `beforeSentAt` as a cursor for pagination.",
	channelDetail:
		"Inspect a channel's subscribers with per-subscriber queue stats (queueDepth, lastReadAt). No ACL — any session can read. Returns CHANNEL_NOT_FOUND if the channelId does not exist.",
};

export type JsonSchema = Record<string, unknown>;

const TOPIC_ID_SCHEMA = { type: "string", minLength: 1, maxLength: 64 };
const SUBJECT_SCHEMA = { type: "string", minLength: 1, maxLength: 256 };
const BODY_SCHEMA = { type: "string", maxLength: 65536 };
const THREAD_ID_SCHEMA = { type: "string", maxLength: 64 };
const MESSAGE_ID_SCHEMA = { type: "string", minLength: 1, maxLength: 64 };
const CHANNEL_ID_SCHEMA = { type: "string", minLength: 1, maxLength: 64 };

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
	listChannels: {
		type: "object",
		properties: {},
		additionalProperties: false,
	},
	channelCreate: {
		type: "object",
		properties: { channelId: CHANNEL_ID_SCHEMA },
		required: ["channelId"],
		additionalProperties: false,
	},
	channelSubscribe: {
		type: "object",
		properties: { channelId: CHANNEL_ID_SCHEMA },
		required: ["channelId"],
		additionalProperties: false,
	},
	channelSend: {
		type: "object",
		properties: {
			channelId: CHANNEL_ID_SCHEMA,
			subject: SUBJECT_SCHEMA,
			body: BODY_SCHEMA,
		},
		required: ["channelId", "subject", "body"],
		additionalProperties: false,
	},
	channelUnsubscribe: {
		type: "object",
		properties: { channelId: CHANNEL_ID_SCHEMA },
		required: ["channelId"],
		additionalProperties: false,
	},
	channelHistory: {
		type: "object",
		properties: {
			channelId: CHANNEL_ID_SCHEMA,
			limit: { type: "integer", minimum: 1, maximum: 200 },
			beforeSentAt: { type: "string", minLength: 1, maxLength: 64 },
		},
		required: ["channelId"],
		additionalProperties: false,
	},
	channelDetail: {
		type: "object",
		properties: { channelId: CHANNEL_ID_SCHEMA },
		required: ["channelId"],
		additionalProperties: false,
	},
};
