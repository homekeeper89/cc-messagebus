import type { TopicId } from "../protocol/http.js";
import { McpClientError } from "./broker-client.js";

let currentTopicId: TopicId | null = null;

export function setTopicId(id: TopicId): void {
	if (currentTopicId !== null) {
		throw new McpClientError(
			"TOPIC_ALREADY_REGISTERED",
			`adapter already registered as '${currentTopicId}'`,
		);
	}
	currentTopicId = id;
}

export function clearTopicId(): void {
	currentTopicId = null;
}

export function requireTopicId(): TopicId {
	if (currentTopicId === null) {
		throw new McpClientError(
			"TOPIC_NOT_REGISTERED",
			"this session has not called register yet",
		);
	}
	return currentTopicId;
}

export function peekTopicId(): TopicId | null {
	return currentTopicId;
}
