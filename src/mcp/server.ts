import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
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
import { clearPeerId, requirePeerId, setPeerId } from "./state.js";

const DEFAULT_BROKER_URL = "http://127.0.0.1:5959";

const PKG_VERSION: string = (() => {
	const here = dirname(fileURLToPath(import.meta.url));
	const pkgPath = join(here, "..", "..", "package.json");
	const raw = JSON.parse(readFileSync(pkgPath, "utf8")) as { version: string };
	return raw.version;
})();

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
			const peerId = args.peerId as string;
			const res = await client.register({ peerId });
			setPeerId(peerId);
			return res;
		}
		case MCP_TOOL_NAMES.unregister: {
			const peerId = requirePeerId();
			const res = await client.unregister({
				peerId,
				purgeQueue: args.purgeQueue as boolean | undefined,
			});
			clearPeerId();
			return res;
		}
		case MCP_TOOL_NAMES.read: {
			const peerId = requirePeerId();
			return client.read({
				peerId,
				max: args.max as number | undefined,
			});
		}
		case MCP_TOOL_NAMES.ack: {
			const peerId = requirePeerId();
			return client.ack({
				peerId,
				messageId: args.messageId as string,
			});
		}
		case MCP_TOOL_NAMES.listPeers:
			return client.listPeers();
		case MCP_TOOL_NAMES.listTopics:
			// PRD: ACL 없음 — 누구나 list 가능. requirePeerId 의도적으로 호출 안 함.
			return client.listTopics();
		case MCP_TOOL_NAMES.topicCreate: {
			const createdBy = requirePeerId();
			return client.topicCreate({
				topicId: args.topicId as string,
				createdBy,
			});
		}
		case MCP_TOOL_NAMES.topicSubscribe: {
			const peerId = requirePeerId();
			return client.topicSubscribe({
				topicId: args.topicId as string,
				peerId,
			});
		}
		case MCP_TOOL_NAMES.topicSend: {
			const from = requirePeerId();
			return client.topicSend({
				topicId: args.topicId as string,
				from,
				subject: args.subject as string,
				body: args.body as string,
			});
		}
		case MCP_TOOL_NAMES.topicUnsubscribe: {
			const peerId = requirePeerId();
			return client.topicUnsubscribe({
				topicId: args.topicId as string,
				peerId,
			});
		}
		case MCP_TOOL_NAMES.topicHistory: {
			// PRD: ACL 없음 — 누구나 read 가능. requirePeerId 의도적으로 호출 안 함.
			return client.topicHistory({
				topicId: args.topicId as string,
				limit: args.limit as number | undefined,
				beforeSentAt: args.beforeSentAt as string | undefined,
			});
		}
		case MCP_TOOL_NAMES.topicDetail: {
			// PRD: ACL 없음 — 누구나 read 가능. requirePeerId 의도적으로 호출 안 함.
			return client.topicDetail({
				topicId: args.topicId as string,
			});
		}
		case MCP_TOOL_NAMES.topicMonitor: {
			const peerId = requirePeerId();
			return client.topicMonitor({
				topicId: args.topicId as string,
				peerId,
				max: args.max as number | undefined,
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
		{ name: "cc-messagebus", version: PKG_VERSION },
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
