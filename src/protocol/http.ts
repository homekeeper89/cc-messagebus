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
	pid: number | null;
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
	archivedAt: IsoTimestamp | null;
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
	archivedAt: IsoTimestamp | null;
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

export interface DmConversationDto {
	peerA: PeerId;
	peerB: PeerId;
	lastFrom: PeerId;
	lastTo: PeerId;
	lastSubject: string;
	lastSentAt: IsoTimestamp;
	messageCount: number;
}

export const HTTP_ENDPOINTS = {
	register: { method: "POST", path: "/api/register" },
	unregister: { method: "POST", path: "/api/unregister" },
	send: { method: "POST", path: "/api/send" },
	read: { method: "POST", path: "/api/read" },
	ack: { method: "POST", path: "/api/ack" },
	listPeers: { method: "POST", path: "/api/list_peers" },
	listTopics: { method: "POST", path: "/api/list_topics" },
	listDmConversations: {
		method: "POST",
		path: "/api/list_dm_conversations",
	},
	topicCreate: { method: "POST", path: "/api/topic_create" },
	topicSubscribe: { method: "POST", path: "/api/topic_subscribe" },
	topicSend: { method: "POST", path: "/api/topic_send" },
	topicUnsubscribe: { method: "POST", path: "/api/topic_unsubscribe" },
	topicHistory: { method: "POST", path: "/api/topic_history" },
	topicDetail: { method: "POST", path: "/api/topic_detail" },
	topicMonitor: { method: "POST", path: "/api/topic_monitor" },
	diagnostics: { method: "POST", path: "/api/diagnostics" },
	serverInfo: { method: "POST", path: "/api/server_info" },
	channelBroadcast: { method: "POST", path: "/api/channel_broadcast" },
	channelDelete: { method: "POST", path: "/api/channel_delete" },
	topicArchive: { method: "POST", path: "/api/topic_archive" },
	topicUnarchive: { method: "POST", path: "/api/topic_unarchive" },
	peerDelete: { method: "POST", path: "/api/peer_delete" },
	peersClean: { method: "POST", path: "/api/peers_clean" },
} as const;

export interface RegisterRequest {
	peerId: PeerId;
	pid?: number;
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

export type ListDmConversationsRequest = Record<string, never>;
export interface ListDmConversationsResponse {
	conversations: DmConversationDto[];
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

export interface TopicMonitorRequest {
	topicId: TopicId;
	peerId: PeerId;
	max?: number;
}
export interface TopicMonitorResponse {
	messages: TopicMessageDto[];
	cursor: TopicMessageId | null;
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

export interface ServerInfoResponse {
	issueRepo: string | null;
	version: string;
}

export interface ChannelBroadcastRequest {
	topicId: TopicId;
	from: PeerId;
	subject: string;
	body: string;
}
export interface ChannelBroadcastResponse {
	topicMessageId: TopicMessageId;
	deliveredTo: PeerId[];
	sentAt: IsoTimestamp;
}

export interface ChannelDeleteRequest {
	topicId: TopicId;
}
export interface ChannelDeleteResponse {
	deletedMessages: number;
	deletedSubs: number;
}

export interface TopicArchiveRequest {
	topicId: TopicId;
}
export interface TopicArchiveResponse {
	archivedAt: IsoTimestamp;
}

export interface TopicUnarchiveRequest {
	topicId: TopicId;
}
export interface TopicUnarchiveResponse {
	unarchivedAt: IsoTimestamp;
}

export interface PeerDeleteRequest {
	peerId: PeerId;
}
export interface PeerDeleteResponse {
	deletedSubs: number;
	cancelledInflight: number;
}

export interface CleanedPeerDto {
	peerId: PeerId;
	pid: number;
}
export type PeersCleanRequest = Record<string, never>;
export interface PeersCleanResponse {
	cleaned: CleanedPeerDto[];
}

export type RegisterApiResponse = ApiResponse<RegisterResponse>;
export type UnregisterApiResponse = ApiResponse<UnregisterResponse>;
export type SendApiResponse = ApiResponse<SendResponse>;
export type ReadApiResponse = ApiResponse<ReadResponse>;
export type AckApiResponse = ApiResponse<AckResponse>;
export type ListPeersApiResponse = ApiResponse<ListPeersResponse>;
export type ListTopicsApiResponse = ApiResponse<ListTopicsResponse>;
export type ListDmConversationsApiResponse =
	ApiResponse<ListDmConversationsResponse>;
export type TopicCreateApiResponse = ApiResponse<TopicCreateResponse>;
export type TopicSubscribeApiResponse = ApiResponse<TopicSubscribeResponse>;
export type TopicSendApiResponse = ApiResponse<TopicSendResponse>;
export type TopicUnsubscribeApiResponse = ApiResponse<TopicUnsubscribeResponse>;
export type TopicHistoryApiResponse = ApiResponse<TopicHistoryResponse>;
export type TopicDetailApiResponse = ApiResponse<TopicDetailResponse>;
export type TopicMonitorApiResponse = ApiResponse<TopicMonitorResponse>;
export type DiagnosticsApiResponse = ApiResponse<DiagnosticsResponse>;
export type ServerInfoApiResponse = ApiResponse<ServerInfoResponse>;
export type ChannelBroadcastApiResponse = ApiResponse<ChannelBroadcastResponse>;
export type ChannelDeleteApiResponse = ApiResponse<ChannelDeleteResponse>;
export type TopicArchiveApiResponse = ApiResponse<TopicArchiveResponse>;
export type TopicUnarchiveApiResponse = ApiResponse<TopicUnarchiveResponse>;
export type PeerDeleteApiResponse = ApiResponse<PeerDeleteResponse>;
export type PeersCleanApiResponse = ApiResponse<PeersCleanResponse>;
