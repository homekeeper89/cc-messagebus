import type {
	IsoTimestamp,
	MessageDto,
	MessageId,
	PeerDto,
	PeerId,
	TopicDto,
	TopicId,
	TopicMessageId,
	TopicSummaryDto,
} from "./http.js";

export const DASHBOARD_EVENT_TYPES = {
	sessionSnapshot: "session_snapshot",
	sessionRegistered: "session_registered",
	sessionDisconnected: "session_disconnected",
	messageSent: "message_sent",
	messageRead: "message_read",
	messageAcked: "message_acked",
	messageRedelivered: "message_redelivered",
	messageExpired: "message_expired",
	topicCreated: "topic_created",
	topicSubscribed: "topic_subscribed",
	topicUnsubscribed: "topic_unsubscribed",
	topicMessagePublished: "topic_message_published",
	topicDeleted: "topic_deleted",
	peerDeleted: "peer_deleted",
} as const;

export interface SessionSnapshotEvent {
	type: "session_snapshot";
	peers: PeerDto[];
	topics: TopicSummaryDto[];
	at: IsoTimestamp;
}

export interface SessionRegisteredEvent {
	type: "session_registered";
	peer: PeerDto;
}
export interface SessionDisconnectedEvent {
	type: "session_disconnected";
	peerId: PeerId;
	at: IsoTimestamp;
}
export interface MessageSentEvent {
	type: "message_sent";
	message: MessageDto;
	// "dm" = 1:1 send() 결과, "topic" = topicSend() fanout 결과.
	kind: "dm" | "topic";
	// kind === "topic" 일 때만 set. fanout 출처 topic 식별용.
	topicId?: TopicId;
}
export interface MessageReadEvent {
	type: "message_read";
	messageId: MessageId;
	peerId: PeerId;
	at: IsoTimestamp;
}
export interface MessageAckedEvent {
	type: "message_acked";
	messageId: MessageId;
	peerId: PeerId;
	at: IsoTimestamp;
}
export interface MessageRedeliveredEvent {
	type: "message_redelivered";
	messageId: MessageId;
	at: IsoTimestamp;
}
export interface MessageExpiredEvent {
	type: "message_expired";
	messageId: MessageId;
	at: IsoTimestamp;
}

export interface TopicCreatedEvent {
	type: "topic_created";
	topic: TopicDto;
}
export interface TopicSubscribedEvent {
	type: "topic_subscribed";
	topicId: TopicId;
	peerId: PeerId;
	at: IsoTimestamp;
}
export interface TopicUnsubscribedEvent {
	type: "topic_unsubscribed";
	topicId: TopicId;
	peerId: PeerId;
	at: IsoTimestamp;
}
export interface TopicMessagePublishedEvent {
	type: "topic_message_published";
	topicId: TopicId;
	topicMessageId: TopicMessageId;
	from: PeerId;
	deliveredTo: PeerId[];
	sentAt: IsoTimestamp;
}
export interface TopicDeletedEvent {
	type: "topic_deleted";
	topicId: TopicId;
	deletedMessages: number;
	deletedSubs: number;
	at: IsoTimestamp;
}
export interface PeerDeletedEvent {
	type: "peer_deleted";
	peerId: PeerId;
	deletedSubs: number;
	cancelledInflight: number;
	at: IsoTimestamp;
}

export type DashboardEvent =
	| SessionSnapshotEvent
	| SessionRegisteredEvent
	| SessionDisconnectedEvent
	| MessageSentEvent
	| MessageReadEvent
	| MessageAckedEvent
	| MessageRedeliveredEvent
	| MessageExpiredEvent
	| TopicCreatedEvent
	| TopicSubscribedEvent
	| TopicUnsubscribedEvent
	| TopicMessagePublishedEvent
	| TopicDeletedEvent
	| PeerDeletedEvent;

export function serializeSseEvent<T extends { type: string }>(
	event: T,
	id?: string,
): string {
	const lines = [`event: ${event.type}`, `data: ${JSON.stringify(event)}`];
	if (id !== undefined) lines.push(`id: ${id}`);
	return `${lines.join("\n")}\n\n`;
}

export interface ParseSseChunksResult {
	events: DashboardEvent[];
	rest: string;
}

export function parseSseChunks(buffer: string): ParseSseChunksResult {
	const events: DashboardEvent[] = [];
	let cursor = 0;
	while (true) {
		const boundary = buffer.indexOf("\n\n", cursor);
		if (boundary === -1) break;
		const block = buffer.slice(cursor, boundary);
		cursor = boundary + 2;
		for (const line of block.split("\n")) {
			if (line.startsWith(":")) continue;
			if (!line.startsWith("data:")) continue;
			const payload = line.slice(5).trimStart();
			if (!payload) continue;
			try {
				events.push(JSON.parse(payload) as DashboardEvent);
			} catch {
				// ill-formed payload silently dropped; broker emits valid JSON only
			}
		}
	}
	return { events, rest: buffer.slice(cursor) };
}
