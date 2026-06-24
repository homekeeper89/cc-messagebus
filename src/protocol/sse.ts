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

export const TAIL_EVENT_TYPES = {
	messageDelivered: "message_delivered",
} as const;

export interface MessageDeliveredEvent {
	type: "message_delivered";
	message: MessageDto;
}
export type TailEvent = MessageDeliveredEvent;

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
	| TopicMessagePublishedEvent;

export function serializeSseEvent<T extends { type: string }>(
	event: T,
	id?: string,
): string {
	const lines = [`event: ${event.type}`, `data: ${JSON.stringify(event)}`];
	if (id !== undefined) lines.push(`id: ${id}`);
	return `${lines.join("\n")}\n\n`;
}
