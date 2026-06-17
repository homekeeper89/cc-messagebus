import type { ApiResponse } from "./errors.js";

export type IsoTimestamp = string;
export type TopicId = string;
export type MessageId = string;
export type ThreadId = string;

export const SessionStatus = {
	CONNECTED: "connected",
	DISCONNECTED: "disconnected",
} as const;
export type SessionStatus = (typeof SessionStatus)[keyof typeof SessionStatus];

export interface MessageDto {
	id: MessageId;
	from: TopicId;
	to: TopicId;
	subject: string;
	body: string;
	threadId: ThreadId | null;
	sentAt: IsoTimestamp;
	inFlightUntil: IsoTimestamp | null;
	ackedAt: IsoTimestamp | null;
	expiresAt: IsoTimestamp;
}

export interface PeerDto {
	topicId: TopicId;
	status: SessionStatus;
	connectedAt: IsoTimestamp;
	lastSeenAt: IsoTimestamp;
	queueLength: number;
}

export const HTTP_ENDPOINTS = {
	register: { method: "POST", path: "/api/register" },
	unregister: { method: "POST", path: "/api/unregister" },
	send: { method: "POST", path: "/api/send" },
	read: { method: "POST", path: "/api/read" },
	ack: { method: "POST", path: "/api/ack" },
	listPeers: { method: "POST", path: "/api/list_peers" },
} as const;

export interface RegisterRequest {
	topicId: TopicId;
}
export interface RegisterResponse {
	topicId: TopicId;
	monitorCommand: string;
	dashboardUrl: string;
}

export interface UnregisterRequest {
	topicId: TopicId;
	purgeQueue?: boolean;
}
export interface UnregisterResponse {
	purged: boolean;
}

export interface SendRequest {
	from: TopicId;
	to: TopicId;
	subject: string;
	body: string;
	threadId?: ThreadId;
}
export interface SendResponse {
	messageId: MessageId;
	sentAt: IsoTimestamp;
}

export interface ReadRequest {
	topicId: TopicId;
	max?: number;
}
export interface ReadResponse {
	messages: MessageDto[];
}

export interface AckRequest {
	topicId: TopicId;
	messageId: MessageId;
}
export interface AckResponse {
	ackedAt: IsoTimestamp;
}

export type ListPeersRequest = Record<string, never>;
export interface ListPeersResponse {
	peers: PeerDto[];
}

export type RegisterApiResponse = ApiResponse<RegisterResponse>;
export type UnregisterApiResponse = ApiResponse<UnregisterResponse>;
export type SendApiResponse = ApiResponse<SendResponse>;
export type ReadApiResponse = ApiResponse<ReadResponse>;
export type AckApiResponse = ApiResponse<AckResponse>;
export type ListPeersApiResponse = ApiResponse<ListPeersResponse>;
