import type { ApiResponse } from "../protocol/errors.js";
import {
	type AckRequest,
	type AckResponse,
	type ChannelCreateRequest,
	type ChannelCreateResponse,
	type ChannelHistoryRequest,
	type ChannelHistoryResponse,
	type ChannelSendRequest,
	type ChannelSendResponse,
	type ChannelSubscribeRequest,
	type ChannelSubscribeResponse,
	type ChannelUnsubscribeRequest,
	type ChannelUnsubscribeResponse,
	HTTP_ENDPOINTS,
	type ListChannelsResponse,
	type ListPeersResponse,
	type ReadRequest,
	type ReadResponse,
	type RegisterRequest,
	type RegisterResponse,
	type SendRequest,
	type SendResponse,
	type UnregisterRequest,
	type UnregisterResponse,
} from "../protocol/http.js";

export class McpClientError extends Error {
	readonly code: string;
	readonly details?: unknown;
	constructor(code: string, message: string, details?: unknown) {
		super(message);
		this.name = "McpClientError";
		this.code = code;
		this.details = details;
	}
}

export interface BrokerClient {
	register: (req: RegisterRequest) => Promise<RegisterResponse>;
	unregister: (req: UnregisterRequest) => Promise<UnregisterResponse>;
	send: (req: SendRequest) => Promise<SendResponse>;
	read: (req: ReadRequest) => Promise<ReadResponse>;
	ack: (req: AckRequest) => Promise<AckResponse>;
	listPeers: () => Promise<ListPeersResponse>;
	listChannels: () => Promise<ListChannelsResponse>;
	channelCreate: (req: ChannelCreateRequest) => Promise<ChannelCreateResponse>;
	channelSubscribe: (
		req: ChannelSubscribeRequest,
	) => Promise<ChannelSubscribeResponse>;
	channelSend: (req: ChannelSendRequest) => Promise<ChannelSendResponse>;
	channelUnsubscribe: (
		req: ChannelUnsubscribeRequest,
	) => Promise<ChannelUnsubscribeResponse>;
	channelHistory: (
		req: ChannelHistoryRequest,
	) => Promise<ChannelHistoryResponse>;
}

export function createBrokerClient(baseUrl: string): BrokerClient {
	const base = baseUrl.replace(/\/+$/, "");

	async function call<TReq, TRes>(path: string, body: TReq): Promise<TRes> {
		let res: Response;
		try {
			res = await fetch(`${base}${path}`, {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify(body),
			});
		} catch (e) {
			// fetch failure (ECONNREFUSED/ENOTFOUND/etc.) — caller may auto-spawn broker
			const msg = e instanceof Error ? e.message : String(e);
			throw new McpClientError("BROKER_UNREACHABLE", msg);
		}
		let json: ApiResponse<TRes>;
		try {
			json = (await res.json()) as ApiResponse<TRes>;
		} catch (e) {
			const msg = e instanceof Error ? e.message : String(e);
			throw new McpClientError(
				"INVALID_RESPONSE",
				`broker returned non-JSON (status ${res.status}): ${msg}`,
			);
		}
		if (json.ok === false) {
			throw new McpClientError(
				json.error.code,
				json.error.message,
				json.error.details,
			);
		}
		const { ok: _ok, ...rest } = json;
		return rest as TRes;
	}

	return {
		register: (req) =>
			call<RegisterRequest, RegisterResponse>(
				HTTP_ENDPOINTS.register.path,
				req,
			),
		unregister: (req) =>
			call<UnregisterRequest, UnregisterResponse>(
				HTTP_ENDPOINTS.unregister.path,
				req,
			),
		send: (req) =>
			call<SendRequest, SendResponse>(HTTP_ENDPOINTS.send.path, req),
		read: (req) =>
			call<ReadRequest, ReadResponse>(HTTP_ENDPOINTS.read.path, req),
		ack: (req) => call<AckRequest, AckResponse>(HTTP_ENDPOINTS.ack.path, req),
		listPeers: () =>
			call<Record<string, never>, ListPeersResponse>(
				HTTP_ENDPOINTS.listPeers.path,
				{},
			),
		listChannels: () =>
			call<Record<string, never>, ListChannelsResponse>(
				HTTP_ENDPOINTS.listChannels.path,
				{},
			),
		channelCreate: (req) =>
			call<ChannelCreateRequest, ChannelCreateResponse>(
				HTTP_ENDPOINTS.channelCreate.path,
				req,
			),
		channelSubscribe: (req) =>
			call<ChannelSubscribeRequest, ChannelSubscribeResponse>(
				HTTP_ENDPOINTS.channelSubscribe.path,
				req,
			),
		channelSend: (req) =>
			call<ChannelSendRequest, ChannelSendResponse>(
				HTTP_ENDPOINTS.channelSend.path,
				req,
			),
		channelUnsubscribe: (req) =>
			call<ChannelUnsubscribeRequest, ChannelUnsubscribeResponse>(
				HTTP_ENDPOINTS.channelUnsubscribe.path,
				req,
			),
		channelHistory: (req) =>
			call<ChannelHistoryRequest, ChannelHistoryResponse>(
				HTTP_ENDPOINTS.channelHistory.path,
				req,
			),
	};
}
