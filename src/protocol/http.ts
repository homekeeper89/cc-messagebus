import type { ApiResponse } from "./errors.js";

export type IsoTimestamp = string;
export type PeerId = string;
export type MessageId = string;
export type ThreadId = string;
export type TopicId = string;
export type TopicMessageId = string;

export const SessionStatus = {
	CONNECTED: "connected",
	DISCONNECTED: "disconnected",
} as const;
export type SessionStatus = (typeof SessionStatus)[keyof typeof SessionStatus];

export interface MessageDto {
	id: MessageId;
	from: PeerId;
	to: PeerId;
	subject: string;
	body: string;
	threadId: ThreadId | null;
	sentAt: IsoTimestamp;
	inFlightUntil: IsoTimestamp | null;
	ackedAt: IsoTimestamp | null;
	expiresAt: IsoTimestamp;
}

export interface PeerDto {
	peerId: PeerId;
	status: SessionStatus;
	connectedAt: IsoTimestamp;
	lastSeenAt: IsoTimestamp;
	lastActivityAt: IsoTimestamp | null;
	queueLength: number;
}

export interface TopicDto {
	topicId: TopicId;
	createdBy: PeerId;
	createdAt: IsoTimestamp;
}

export interface TopicSummaryDto {
	topicId: TopicId;
	createdBy: PeerId;
	createdAt: IsoTimestamp;
	subscriberCount: number;
	lastPublishedAt: IsoTimestamp | null;
}

export interface SubscriberDto {
	peerId: PeerId;
	subscribedAt: IsoTimestamp;
	queueDepth: number;
	lastReadAt: IsoTimestamp | null;
}

export interface TopicDetailDto {
	topicId: TopicId;
	createdBy: PeerId;
	createdAt: IsoTimestamp;
	subscribers: SubscriberDto[];
}

export interface TopicMessageDto {
	topicMessageId: TopicMessageId;
	topicId: TopicId;
	from: PeerId;
	subject: string;
	body: string;
	sentAt: IsoTimestamp;
	expiresAt: IsoTimestamp;
}

export const HTTP_ENDPOINTS = {
	register: { method: "POST", path: "/api/register" },
	unregister: { method: "POST", path: "/api/unregister" },
	send: { method: "POST", path: "/api/send" },
	read: { method: "POST", path: "/api/read" },
	ack: { method: "POST", path: "/api/ack" },
	listPeers: { method: "POST", path: "/api/list_peers" },
	listTopics: { method: "POST", path: "/api/list_topics" },
	topicCreate: { method: "POST", path: "/api/topic_create" },
	topicSubscribe: { method: "POST", path: "/api/topic_subscribe" },
	topicSend: { method: "POST", path: "/api/topic_send" },
	topicUnsubscribe: { method: "POST", path: "/api/topic_unsubscribe" },
	topicHistory: { method: "POST", path: "/api/topic_history" },
	topicDetail: { method: "POST", path: "/api/topic_detail" },
	diagnostics: { method: "POST", path: "/api/diagnostics" },
	issueCreate: { method: "POST", path: "/api/issue_create" },
} as const;

export interface RegisterRequest {
	peerId: PeerId;
}
export interface RegisterResponse {
	peerId: PeerId;
	monitorCommand: string;
	dashboardUrl: string;
}

export interface UnregisterRequest {
	peerId: PeerId;
	purgeQueue?: boolean;
}
export interface UnregisterResponse {
	purged: boolean;
}

export interface SendRequest {
	from: PeerId;
	to: PeerId;
	subject: string;
	body: string;
	threadId?: ThreadId;
}
export interface SendResponse {
	messageId: MessageId;
	sentAt: IsoTimestamp;
}

export interface ReadRequest {
	peerId: PeerId;
	max?: number;
}
export interface ReadResponse {
	messages: MessageDto[];
}

export interface AckRequest {
	peerId: PeerId;
	messageId: MessageId;
}
export interface AckResponse {
	ackedAt: IsoTimestamp;
}

export type ListPeersRequest = Record<string, never>;
export interface ListPeersResponse {
	peers: PeerDto[];
}

export type ListTopicsRequest = Record<string, never>;
export interface ListTopicsResponse {
	topics: TopicSummaryDto[];
}

export interface TopicCreateRequest {
	topicId: TopicId;
	createdBy: PeerId;
}
export interface TopicCreateResponse {
	topic: TopicDto;
}

export interface TopicSubscribeRequest {
	topicId: TopicId;
	peerId: PeerId;
}
export interface TopicSubscribeResponse {
	subscribedAt: IsoTimestamp;
}

export interface TopicSendRequest {
	topicId: TopicId;
	from: PeerId;
	subject: string;
	body: string;
}
export interface TopicSendResponse {
	topicMessageId: TopicMessageId;
	deliveredTo: PeerId[];
	sentAt: IsoTimestamp;
}

export interface TopicUnsubscribeRequest {
	topicId: TopicId;
	peerId: PeerId;
}
export interface TopicUnsubscribeResponse {
	unsubscribedAt: IsoTimestamp;
}

export interface TopicHistoryRequest {
	topicId: TopicId;
	limit?: number;
	beforeSentAt?: IsoTimestamp;
}
export interface TopicHistoryResponse {
	messages: TopicMessageDto[];
	hasMore: boolean;
}

export interface TopicDetailRequest {
	topicId: TopicId;
}
export interface TopicDetailResponse {
	topic: TopicDetailDto;
}

export interface RecentRpcEntry {
	method: string;
	durationMs: number;
	error: string | null;
	at: IsoTimestamp;
}

export interface RecentErrorEntry {
	message: string;
	stack: string | null;
	at: IsoTimestamp;
}

export type DiagnosticsRequest = Record<string, never>;
export interface DiagnosticsResponse {
	version: string;
	uptimeSec: number;
	nodeVersion: string;
	topicCount: number;
	peerCount: number;
	dbSizeByte: number;
	recentRpcList: RecentRpcEntry[];
	recentErrorList: RecentErrorEntry[];
}

export type IssueType = "bug" | "feature" | "note";

export interface IssueCreateRequest {
	type: IssueType;
	title: string;
	body: string;
}
export interface IssueCreateResponse {
	issueNumber: number;
	url: string;
}

export type RegisterApiResponse = ApiResponse<RegisterResponse>;
export type UnregisterApiResponse = ApiResponse<UnregisterResponse>;
export type SendApiResponse = ApiResponse<SendResponse>;
export type ReadApiResponse = ApiResponse<ReadResponse>;
export type AckApiResponse = ApiResponse<AckResponse>;
export type ListPeersApiResponse = ApiResponse<ListPeersResponse>;
export type ListTopicsApiResponse = ApiResponse<ListTopicsResponse>;
export type TopicCreateApiResponse = ApiResponse<TopicCreateResponse>;
export type TopicSubscribeApiResponse = ApiResponse<TopicSubscribeResponse>;
export type TopicSendApiResponse = ApiResponse<TopicSendResponse>;
export type TopicUnsubscribeApiResponse = ApiResponse<TopicUnsubscribeResponse>;
export type TopicHistoryApiResponse = ApiResponse<TopicHistoryResponse>;
export type TopicDetailApiResponse = ApiResponse<TopicDetailResponse>;
export type DiagnosticsApiResponse = ApiResponse<DiagnosticsResponse>;
export type IssueCreateApiResponse = ApiResponse<IssueCreateResponse>;
