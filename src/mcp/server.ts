import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
	CallToolRequestSchema,
	ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import {
	MCP_INPUT_SCHEMAS,
	MCP_TOOL_DESCRIPTIONS,
	MCP_TOOL_NAMES,
	type McpToolKey,
} from "../protocol/mcp.js";
import {
	type BrokerClient,
	createBrokerClient,
	McpClientError,
} from "./broker-client.js";
import { ensureBrokerRunning, type SpawnCmd } from "./spawn.js";
import { clearTopicId, requireTopicId, setTopicId } from "./state.js";

const DEFAULT_BROKER_URL = "http://127.0.0.1:5959";

export interface RunMcpOptions {
	baseUrl?: string;
	client?: BrokerClient;
	spawnCmd?: SpawnCmd;
}

function defaultSpawnCmd(): SpawnCmd {
	return {
		exe: process.execPath,
		args: [process.argv[1], "serve"],
	};
}

export function buildToolList(): {
	name: string;
	description: string;
	inputSchema: Record<string, unknown>;
}[] {
	return (Object.keys(MCP_TOOL_NAMES) as McpToolKey[]).map((key) => ({
		name: MCP_TOOL_NAMES[key],
		description: MCP_TOOL_DESCRIPTIONS[key],
		inputSchema: MCP_INPUT_SCHEMAS[key],
	}));
}

export async function dispatch(
	client: BrokerClient,
	spawnCmd: SpawnCmd,
	name: string,
	args: Record<string, unknown>,
): Promise<unknown> {
	switch (name) {
		case MCP_TOOL_NAMES.register: {
			await ensureBrokerRunning(client, spawnCmd);
			const topicId = args.topicId as string;
			const res = await client.register({ topicId });
			setTopicId(topicId);
			return res;
		}
		case MCP_TOOL_NAMES.unregister: {
			const topicId = requireTopicId();
			const res = await client.unregister({
				topicId,
				purgeQueue: args.purgeQueue as boolean | undefined,
			});
			clearTopicId();
			return res;
		}
		case MCP_TOOL_NAMES.send: {
			const from = requireTopicId();
			return client.send({
				from,
				to: args.to as string,
				subject: args.subject as string,
				body: args.body as string,
				threadId: args.threadId as string | undefined,
			});
		}
		case MCP_TOOL_NAMES.read: {
			const topicId = requireTopicId();
			return client.read({
				topicId,
				max: args.max as number | undefined,
			});
		}
		case MCP_TOOL_NAMES.ack: {
			const topicId = requireTopicId();
			return client.ack({
				topicId,
				messageId: args.messageId as string,
			});
		}
		case MCP_TOOL_NAMES.listPeers:
			return client.listPeers();
		case MCP_TOOL_NAMES.channelCreate: {
			const createdBy = requireTopicId();
			return client.channelCreate({
				channelId: args.channelId as string,
				createdBy,
			});
		}
		case MCP_TOOL_NAMES.channelSubscribe: {
			const topicId = requireTopicId();
			return client.channelSubscribe({
				channelId: args.channelId as string,
				topicId,
			});
		}
		case MCP_TOOL_NAMES.channelSend: {
			const from = requireTopicId();
			return client.channelSend({
				channelId: args.channelId as string,
				from,
				subject: args.subject as string,
				body: args.body as string,
			});
		}
		case MCP_TOOL_NAMES.channelUnsubscribe: {
			const topicId = requireTopicId();
			return client.channelUnsubscribe({
				channelId: args.channelId as string,
				topicId,
			});
		}
		case MCP_TOOL_NAMES.channelHistory: {
			// PRD: ACL 없음 — 누구나 read 가능. requireTopicId 의도적으로 호출 안 함.
			return client.channelHistory({
				channelId: args.channelId as string,
				limit: args.limit as number | undefined,
				beforeSentAt: args.beforeSentAt as string | undefined,
			});
		}
		default:
			throw new McpClientError("UNKNOWN_TOOL", `unknown tool: ${name}`);
	}
}

export async function runMcp(opts: RunMcpOptions = {}): Promise<void> {
	const baseUrl =
		opts.baseUrl ?? process.env.CC_MESSAGEBUS_URL ?? DEFAULT_BROKER_URL;
	const client = opts.client ?? createBrokerClient(baseUrl);
	const spawnCmd = opts.spawnCmd ?? defaultSpawnCmd();

	const server = new Server(
		{ name: "cc-messagebus", version: "0.0.1" },
		{ capabilities: { tools: {} } },
	);

	server.setRequestHandler(ListToolsRequestSchema, async () => ({
		tools: buildToolList(),
	}));

	server.setRequestHandler(CallToolRequestSchema, async (req) => {
		const { name, arguments: args = {} } = req.params;
		try {
			const result = await dispatch(client, spawnCmd, name, args);
			return {
				content: [{ type: "text", text: JSON.stringify(result) }],
			};
		} catch (e) {
			const code = e instanceof McpClientError ? e.code : "INTERNAL_ERROR";
			const message = e instanceof Error ? e.message : String(e);
			const details = e instanceof McpClientError ? e.details : undefined;
			return {
				content: [
					{
						type: "text",
						text: JSON.stringify({
							ok: false,
							error: { code, message, details },
						}),
					},
				],
				isError: true,
			};
		}
	});

	await server.connect(new StdioServerTransport());
}
