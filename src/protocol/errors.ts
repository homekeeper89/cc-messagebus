export type ErrorCode =
	| "TOPIC_ALREADY_REGISTERED"
	| "TOPIC_NOT_FOUND"
	| "PEER_NOT_FOUND"
	| "MESSAGE_NOT_FOUND"
	| "MESSAGE_NOT_IN_FLIGHT"
	| "VALIDATION_FAILED"
	| "INTERNAL_ERROR"
	| "CHANNEL_NOT_FOUND"
	| "CHANNEL_ALREADY_EXISTS"
	| "ALREADY_SUBSCRIBED"
	| "NOT_SUBSCRIBED";

export const errorCodeToHttpStatus = {
	TOPIC_ALREADY_REGISTERED: 409,
	TOPIC_NOT_FOUND: 404,
	PEER_NOT_FOUND: 404,
	MESSAGE_NOT_FOUND: 404,
	MESSAGE_NOT_IN_FLIGHT: 409,
	VALIDATION_FAILED: 400,
	INTERNAL_ERROR: 500,
	CHANNEL_NOT_FOUND: 404,
	CHANNEL_ALREADY_EXISTS: 409,
	ALREADY_SUBSCRIBED: 409,
	NOT_SUBSCRIBED: 404,
} as const satisfies Record<ErrorCode, number>;

export interface ApiError {
	code: ErrorCode;
	message: string;
	details?: unknown;
}

export type ApiResponse<T> =
	| ({ ok: true } & T)
	| { ok: false; error: ApiError };
