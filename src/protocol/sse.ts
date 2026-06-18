import type {
	IsoTimestamp,
	MessageDto,
	MessageId,
	PeerDto,
	TopicId,
} from "./http.js";

export const TAIL_EVENT_TYPES = {
	messageDelivered: "message_delivered",
	heartbeat: "heartbeat",
} as const;

export interface MessageDeliveredEvent {
	type: "message_delivered";
	message: MessageDto;
}
export interface TailHeartbeatEvent {
	type: "heartbeat";
	at: IsoTimestamp;
}
export type TailEvent = MessageDeliveredEvent | TailHeartbeatEvent;

export const DASHBOARD_EVENT_TYPES = {
	sessionSnapshot: "session_snapshot",
	sessionRegistered: "session_registered",
	sessionDisconnected: "session_disconnected",
	messageSent: "message_sent",
	messageRead: "message_read",
	messageAcked: "message_acked",
	messageRedelivered: "message_redelivered",
	messageExpired: "message_expired",
	heartbeat: "heartbeat",
} as const;

export interface SessionSnapshotEvent {
	type: "session_snapshot";
	peers: PeerDto[];
	at: IsoTimestamp;
}

export interface SessionRegisteredEvent {
	type: "session_registered";
	peer: PeerDto;
}
export interface SessionDisconnectedEvent {
	type: "session_disconnected";
	topicId: TopicId;
	at: IsoTimestamp;
}
export interface MessageSentEvent {
	type: "message_sent";
	message: MessageDto;
}
export interface MessageReadEvent {
	type: "message_read";
	messageId: MessageId;
	topicId: TopicId;
	at: IsoTimestamp;
}
export interface MessageAckedEvent {
	type: "message_acked";
	messageId: MessageId;
	topicId: TopicId;
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
export interface DashboardHeartbeatEvent {
	type: "heartbeat";
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
	| DashboardHeartbeatEvent;

export const SSE_HEARTBEAT_INTERVAL_SEC = 15;

export function serializeSseEvent<T extends { type: string }>(
	event: T,
	id?: string,
): string {
	const lines = [`event: ${event.type}`, `data: ${JSON.stringify(event)}`];
	if (id !== undefined) lines.push(`id: ${id}`);
	return `${lines.join("\n")}\n\n`;
}
