import type { PeerId } from "../protocol/http.js";
import { McpClientError } from "./broker-client.js";

let currentPeerId: PeerId | null = null;

export function setPeerId(id: PeerId): void {
	if (currentPeerId !== null) {
		throw new McpClientError(
			"PEER_ALREADY_REGISTERED",
			`adapter already registered as '${currentPeerId}'`,
		);
	}
	currentPeerId = id;
}

export function clearPeerId(): void {
	currentPeerId = null;
}

export function requirePeerId(): PeerId {
	if (currentPeerId === null) {
		throw new McpClientError(
			"PEER_NOT_REGISTERED",
			"this session has not called register yet",
		);
	}
	return currentPeerId;
}

export function peekPeerId(): PeerId | null {
	return currentPeerId;
}
