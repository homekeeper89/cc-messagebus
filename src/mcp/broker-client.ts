import type { ApiResponse } from "../protocol/errors.js";
import {
	type AckRequest,
	type AckResponse,
	HTTP_ENDPOINTS,
	type ListPeersResponse,
	type ListTopicsResponse,
	type ReadRequest,
	type ReadResponse,
	type RegisterRequest,
	type RegisterResponse,
	type SendRequest,
	type SendResponse,
	type TopicCreateRequest,
	type TopicCreateResponse,
	type TopicDetailRequest,
	type TopicDetailResponse,
	type TopicHistoryRequest,
	type TopicHistoryResponse,
	type TopicMonitorRequest,
	type TopicMonitorResponse,
	type TopicSendRequest,
	type TopicSendResponse,
	type TopicSubscribeRequest,
	type TopicSubscribeResponse,
	type TopicUnsubscribeRequest,
	type TopicUnsubscribeResponse,
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
	listTopics: () => Promise<ListTopicsResponse>;
	topicCreate: (req: TopicCreateRequest) => Promise<TopicCreateResponse>;
	topicSubscribe: (
		req: TopicSubscribeRequest,
	) => Promise<TopicSubscribeResponse>;
	topicSend: (req: TopicSendRequest) => Promise<TopicSendResponse>;
	topicUnsubscribe: (
		req: TopicUnsubscribeRequest,
	) => Promise<TopicUnsubscribeResponse>;
	topicHistory: (req: TopicHistoryRequest) => Promise<TopicHistoryResponse>;
	topicDetail: (req: TopicDetailRequest) => Promise<TopicDetailResponse>;
	topicMonitor: (req: TopicMonitorRequest) => Promise<TopicMonitorResponse>;
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
		listTopics: () =>
			call<Record<string, never>, ListTopicsResponse>(
				HTTP_ENDPOINTS.listTopics.path,
				{},
			),
		topicCreate: (req) =>
			call<TopicCreateRequest, TopicCreateResponse>(
				HTTP_ENDPOINTS.topicCreate.path,
				req,
			),
		topicSubscribe: (req) =>
			call<TopicSubscribeRequest, TopicSubscribeResponse>(
				HTTP_ENDPOINTS.topicSubscribe.path,
				req,
			),
		topicSend: (req) =>
			call<TopicSendRequest, TopicSendResponse>(
				HTTP_ENDPOINTS.topicSend.path,
				req,
			),
		topicUnsubscribe: (req) =>
			call<TopicUnsubscribeRequest, TopicUnsubscribeResponse>(
				HTTP_ENDPOINTS.topicUnsubscribe.path,
				req,
			),
		topicHistory: (req) =>
			call<TopicHistoryRequest, TopicHistoryResponse>(
				HTTP_ENDPOINTS.topicHistory.path,
				req,
			),
		topicDetail: (req) =>
			call<TopicDetailRequest, TopicDetailResponse>(
				HTTP_ENDPOINTS.topicDetail.path,
				req,
			),
		topicMonitor: (req) =>
			call<TopicMonitorRequest, TopicMonitorResponse>(
				HTTP_ENDPOINTS.topicMonitor.path,
				req,
			),
	};
}
