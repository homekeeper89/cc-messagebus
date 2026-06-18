import assert from "node:assert/strict";
import { beforeEach, describe, it } from "node:test";
import {
	type BrokerClient,
	McpClientError,
} from "../../src/mcp/broker-client.js";
import { buildToolList, dispatch } from "../../src/mcp/server.js";
import type { SpawnCmd } from "../../src/mcp/spawn.js";
import { clearTopicId, peekTopicId } from "../../src/mcp/state.js";
import {
	MCP_INPUT_SCHEMAS,
	MCP_TOOL_DESCRIPTIONS,
	MCP_TOOL_NAMES,
	type McpToolKey,
} from "../../src/protocol/mcp.js";

interface CallLog {
	register: { topicId: string }[];
	unregister: { topicId: string; purgeQueue?: boolean }[];
	send: { from: string; to: string; subject: string; body: string }[];
	read: { topicId: string; max?: number }[];
	ack: { topicId: string; messageId: string }[];
	listPeers: number;
}

function makeMockClient(overrides: Partial<BrokerClient> = {}): {
	client: BrokerClient;
	calls: CallLog;
} {
	const calls: CallLog = {
		register: [],
		unregister: [],
		send: [],
		read: [],
		ack: [],
		listPeers: 0,
	};
	const client: BrokerClient = {
		register: async (req) => {
			calls.register.push(req);
			return {
				topicId: req.topicId,
				monitorCommand: `cc-messagebus tail ${req.topicId}`,
				dashboardUrl: "http://127.0.0.1:5959/dashboard",
			};
		},
		unregister: async (req) => {
			calls.unregister.push(req);
			return { purged: req.purgeQueue === true };
		},
		send: async (req) => {
			calls.send.push(req);
			return { messageId: "msg-1", sentAt: "2026-06-18T00:00:00.000Z" };
		},
		read: async (req) => {
			calls.read.push(req);
			return { messages: [] };
		},
		ack: async (req) => {
			calls.ack.push(req);
			return { ackedAt: "2026-06-18T00:00:00.000Z" };
		},
		listPeers: async () => {
			calls.listPeers += 1;
			return { peers: [] };
		},
		...overrides,
	};
	return { client, calls };
}

const noopSpawn: SpawnCmd = { exe: "/bin/true", args: [] };

describe("mcp/server.buildToolList", () => {
	it("returns all 6 tools matching protocol/mcp.ts", () => {
		const tools = buildToolList();
		assert.equal(tools.length, 6);
		const keys = Object.keys(MCP_TOOL_NAMES) as McpToolKey[];
		for (const key of keys) {
			const found = tools.find((t) => t.name === MCP_TOOL_NAMES[key]);
			assert.ok(found, `tool ${key} missing`);
			assert.equal(found.description, MCP_TOOL_DESCRIPTIONS[key]);
			assert.deepEqual(found.inputSchema, MCP_INPUT_SCHEMAS[key]);
		}
	});
});

describe("mcp/server.dispatch", () => {
	beforeEach(() => {
		clearTopicId();
	});

	it("register stores topicId and returns monitorCommand", async () => {
		const { client, calls } = makeMockClient();
		const res = (await dispatch(client, noopSpawn, "register", {
			topicId: "alice",
		})) as { topicId: string; monitorCommand: string };
		assert.equal(calls.register.length, 1);
		assert.equal(calls.register[0].topicId, "alice");
		assert.equal(res.topicId, "alice");
		assert.equal(res.monitorCommand, "cc-messagebus tail alice");
		assert.equal(peekTopicId(), "alice");
	});

	it("send before register throws TOPIC_NOT_REGISTERED", async () => {
		const { client } = makeMockClient();
		await assert.rejects(
			() =>
				dispatch(client, noopSpawn, "send", {
					to: "bob",
					subject: "hi",
					body: "hello",
				}),
			(e: unknown) =>
				e instanceof McpClientError && e.code === "TOPIC_NOT_REGISTERED",
		);
	});

	it("unregister clears state so subsequent send rejects", async () => {
		const { client } = makeMockClient();
		await dispatch(client, noopSpawn, "register", { topicId: "alice" });
		await dispatch(client, noopSpawn, "unregister", {});
		assert.equal(peekTopicId(), null);
		await assert.rejects(
			() =>
				dispatch(client, noopSpawn, "send", {
					to: "bob",
					subject: "hi",
					body: "hello",
				}),
			(e: unknown) =>
				e instanceof McpClientError && e.code === "TOPIC_NOT_REGISTERED",
		);
	});

	it("propagates broker McpClientError unchanged", async () => {
		const { client } = makeMockClient({
			register: async () => {
				throw new McpClientError(
					"TOPIC_ALREADY_REGISTERED",
					"topic 'alice' is already connected",
				);
			},
		});
		await assert.rejects(
			() => dispatch(client, noopSpawn, "register", { topicId: "alice" }),
			(e: unknown) =>
				e instanceof McpClientError && e.code === "TOPIC_ALREADY_REGISTERED",
		);
		assert.equal(peekTopicId(), null);
	});

	it("send forwards args and injects from", async () => {
		const { client, calls } = makeMockClient();
		await dispatch(client, noopSpawn, "register", { topicId: "alice" });
		await dispatch(client, noopSpawn, "send", {
			to: "bob",
			subject: "hi",
			body: "hello",
			threadId: "t-1",
		});
		assert.equal(calls.send.length, 1);
		assert.deepEqual(calls.send[0], {
			from: "alice",
			to: "bob",
			subject: "hi",
			body: "hello",
			threadId: "t-1",
		});
	});

	it("list_peers does not require register", async () => {
		const { client, calls } = makeMockClient();
		const res = (await dispatch(client, noopSpawn, "list_peers", {})) as {
			peers: unknown[];
		};
		assert.equal(calls.listPeers, 1);
		assert.deepEqual(res.peers, []);
	});

	it("unknown tool throws UNKNOWN_TOOL", async () => {
		const { client } = makeMockClient();
		await assert.rejects(
			() => dispatch(client, noopSpawn, "bogus", {}),
			(e: unknown) => e instanceof McpClientError && e.code === "UNKNOWN_TOOL",
		);
	});
});
