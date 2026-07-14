import assert from "node:assert/strict";
import { beforeEach, describe, it } from "node:test";
import {
	type BrokerClient,
	McpClientError,
} from "../../src/mcp/broker-client.js";
import { buildToolList, dispatch } from "../../src/mcp/server.js";
import type { SpawnCmd } from "../../src/mcp/spawn.js";
import { clearPeerId, peekPeerId } from "../../src/mcp/state.js";
import {
	MCP_INPUT_SCHEMAS,
	MCP_TOOL_DESCRIPTIONS,
	MCP_TOOL_NAMES,
	type McpToolKey,
} from "../../src/protocol/mcp.js";

interface CallLog {
	register: { peerId: string }[];
	unregister: { peerId: string; purgeQueue?: boolean }[];
	send: {
		from: string;
		to: string;
		subject: string;
		body: string;
		threadId?: string;
	}[];
	read: { peerId: string; max?: number }[];
	ack: { peerId: string; messageId: string }[];
	listPeers: number;
	listTopics: number;
	topicCreate: { topicId: string; createdBy: string }[];
	topicSubscribe: { topicId: string; peerId: string }[];
	topicSend: {
		topicId: string;
		from: string;
		subject: string;
		body: string;
	}[];
	topicUnsubscribe: { topicId: string; peerId: string }[];
	topicHistory: {
		topicId: string;
		limit?: number;
		beforeSentAt?: string;
	}[];
	topicDetail: { topicId: string }[];
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
		listTopics: 0,
		topicCreate: [],
		topicSubscribe: [],
		topicSend: [],
		topicUnsubscribe: [],
		topicHistory: [],
		topicDetail: [],
	};
	const client: BrokerClient = {
		register: async (req) => {
			calls.register.push(req);
			return {
				peerId: req.peerId,
				monitorCommand: `cc-messagebus tail ${req.peerId}`,
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
		listTopics: async () => {
			calls.listTopics += 1;
			return { topics: [] };
		},
		topicCreate: async (req) => {
			calls.topicCreate.push(req);
			return {
				topic: {
					topicId: req.topicId,
					createdBy: req.createdBy,
					createdAt: "2026-06-18T00:00:00.000Z",
				},
			};
		},
		topicSubscribe: async (req) => {
			calls.topicSubscribe.push(req);
			return { subscribedAt: "2026-06-18T00:00:00.000Z" };
		},
		topicSend: async (req) => {
			calls.topicSend.push(req);
			return {
				topicMessageId: "ch-msg-1",
				deliveredTo: [],
				sentAt: "2026-06-18T00:00:00.000Z",
			};
		},
		topicUnsubscribe: async (req) => {
			calls.topicUnsubscribe.push(req);
			return { unsubscribedAt: "2026-06-18T00:00:00.000Z" };
		},
		topicHistory: async (req) => {
			calls.topicHistory.push(req);
			return { messages: [], hasMore: false };
		},
		topicDetail: async (req) => {
			calls.topicDetail.push(req);
			return {
				topic: {
					topicId: req.topicId,
					createdBy: "alice",
					createdAt: "2026-06-18T00:00:00.000Z",
					subscribers: [],
				},
			};
		},
		...overrides,
	};
	return { client, calls };
}

const noopSpawn: SpawnCmd = { exe: "/bin/true", args: [] };

describe("mcp/server.buildToolList", () => {
	it("returns all tools matching protocol/mcp.ts", () => {
		const tools = buildToolList();
		assert.equal(tools.length, 14);
		const keys = Object.keys(MCP_TOOL_NAMES) as McpToolKey[];
		for (const key of keys) {
			const found = tools.find((t) => t.name === MCP_TOOL_NAMES[key]);
			assert.ok(found, `tool ${key} missing`);
			assert.equal(found.description, MCP_TOOL_DESCRIPTIONS[key]);
			assert.deepEqual(found.inputSchema, MCP_INPUT_SCHEMAS[key]);
		}
	});

	it("exposes `send` (DM) tool", () => {
		const tools = buildToolList();
		const send = tools.find((t) => t.name === "send");
		assert.ok(send, "send tool must be exposed on MCP surface");
	});
});

describe("mcp/server.dispatch", () => {
	beforeEach(() => {
		clearPeerId();
	});

	it("register stores peerId and returns monitorCommand", async () => {
		const { client, calls } = makeMockClient();
		const res = (await dispatch(client, noopSpawn, "register", {
			peerId: "alice",
		})) as { peerId: string; monitorCommand: string };
		assert.equal(calls.register.length, 1);
		assert.equal(calls.register[0].peerId, "alice");
		assert.equal(res.peerId, "alice");
		assert.equal(res.monitorCommand, "cc-messagebus tail alice");
		assert.equal(peekPeerId(), "alice");
	});

	it("dispatch('send', ...) injects from and forwards args to client.send", async () => {
		const { client, calls } = makeMockClient();
		await dispatch(client, noopSpawn, "register", { peerId: "alice" });
		await dispatch(client, noopSpawn, "send", {
			to: "bob",
			subject: "hi",
			body: "hello",
		});
		assert.equal(calls.send.length, 1);
		assert.deepEqual(calls.send[0], {
			from: "alice",
			to: "bob",
			subject: "hi",
			body: "hello",
			threadId: undefined,
		});
	});

	it("dispatch('send', ...) before register throws PEER_NOT_REGISTERED", async () => {
		const { client } = makeMockClient();
		await assert.rejects(
			() =>
				dispatch(client, noopSpawn, "send", {
					to: "bob",
					subject: "hi",
					body: "hello",
				}),
			(e: unknown) =>
				e instanceof McpClientError && e.code === "PEER_NOT_REGISTERED",
		);
	});

	it("unregister clears state so subsequent peer-required tool rejects", async () => {
		const { client } = makeMockClient();
		await dispatch(client, noopSpawn, "register", { peerId: "alice" });
		await dispatch(client, noopSpawn, "unregister", {});
		assert.equal(peekPeerId(), null);
		await assert.rejects(
			() =>
				dispatch(client, noopSpawn, "topic_create", {
					topicId: "general",
				}),
			(e: unknown) =>
				e instanceof McpClientError && e.code === "PEER_NOT_REGISTERED",
		);
	});

	it("propagates broker McpClientError unchanged", async () => {
		const { client } = makeMockClient({
			register: async () => {
				throw new McpClientError(
					"PEER_ALREADY_REGISTERED",
					"topic 'alice' is already connected",
				);
			},
		});
		await assert.rejects(
			() => dispatch(client, noopSpawn, "register", { peerId: "alice" }),
			(e: unknown) =>
				e instanceof McpClientError && e.code === "PEER_ALREADY_REGISTERED",
		);
		assert.equal(peekPeerId(), null);
	});

	it("list_peers does not require register", async () => {
		const { client, calls } = makeMockClient();
		const res = (await dispatch(client, noopSpawn, "list_peers", {})) as {
			peers: unknown[];
		};
		assert.equal(calls.listPeers, 1);
		assert.deepEqual(res.peers, []);
	});

	it("list_topics does not require register", async () => {
		const { client, calls } = makeMockClient();
		const res = (await dispatch(client, noopSpawn, "list_topics", {})) as {
			topics: unknown[];
		};
		assert.equal(calls.listTopics, 1);
		assert.deepEqual(res.topics, []);
	});

	it("topic_create injects createdBy from session", async () => {
		const { client, calls } = makeMockClient();
		await dispatch(client, noopSpawn, "register", { peerId: "alice" });
		const res = (await dispatch(client, noopSpawn, "topic_create", {
			topicId: "general",
		})) as { topic: { topicId: string; createdBy: string } };
		assert.equal(calls.topicCreate.length, 1);
		assert.deepEqual(calls.topicCreate[0], {
			topicId: "general",
			createdBy: "alice",
		});
		assert.equal(res.topic.createdBy, "alice");
	});

	it("topic_create before register throws PEER_NOT_REGISTERED", async () => {
		const { client } = makeMockClient();
		await assert.rejects(
			() => dispatch(client, noopSpawn, "topic_create", { topicId: "general" }),
			(e: unknown) =>
				e instanceof McpClientError && e.code === "PEER_NOT_REGISTERED",
		);
	});

	it("topic_subscribe injects peerId from session", async () => {
		const { client, calls } = makeMockClient();
		await dispatch(client, noopSpawn, "register", { peerId: "bob" });
		await dispatch(client, noopSpawn, "topic_subscribe", {
			topicId: "general",
		});
		assert.equal(calls.topicSubscribe.length, 1);
		assert.deepEqual(calls.topicSubscribe[0], {
			topicId: "general",
			peerId: "bob",
		});
	});

	it("topic_send injects from from session", async () => {
		const { client, calls } = makeMockClient();
		await dispatch(client, noopSpawn, "register", { peerId: "alice" });
		await dispatch(client, noopSpawn, "topic_send", {
			topicId: "general",
			subject: "hi",
			body: "hello",
		});
		assert.equal(calls.topicSend.length, 1);
		assert.deepEqual(calls.topicSend[0], {
			topicId: "general",
			from: "alice",
			subject: "hi",
			body: "hello",
		});
	});

	it("topic_unsubscribe injects peerId from session", async () => {
		const { client, calls } = makeMockClient();
		await dispatch(client, noopSpawn, "register", { peerId: "bob" });
		await dispatch(client, noopSpawn, "topic_unsubscribe", {
			topicId: "general",
		});
		assert.equal(calls.topicUnsubscribe.length, 1);
		assert.deepEqual(calls.topicUnsubscribe[0], {
			topicId: "general",
			peerId: "bob",
		});
	});

	it("topic_unsubscribe before register throws PEER_NOT_REGISTERED", async () => {
		const { client } = makeMockClient();
		await assert.rejects(
			() =>
				dispatch(client, noopSpawn, "topic_unsubscribe", {
					topicId: "general",
				}),
			(e: unknown) =>
				e instanceof McpClientError && e.code === "PEER_NOT_REGISTERED",
		);
	});

	it("topic_history does not require register and forwards args", async () => {
		const { client, calls } = makeMockClient();
		await dispatch(client, noopSpawn, "topic_history", {
			topicId: "general",
			limit: 10,
			beforeSentAt: "2026-06-19T00:00:00.000Z",
		});
		assert.equal(calls.topicHistory.length, 1);
		assert.deepEqual(calls.topicHistory[0], {
			topicId: "general",
			limit: 10,
			beforeSentAt: "2026-06-19T00:00:00.000Z",
		});
	});

	it("topic_detail does not require register and forwards topicId", async () => {
		const { client, calls } = makeMockClient();
		const res = (await dispatch(client, noopSpawn, "topic_detail", {
			topicId: "general",
		})) as { topic: { topicId: string; subscribers: unknown[] } };
		assert.equal(calls.topicDetail.length, 1);
		assert.deepEqual(calls.topicDetail[0], { topicId: "general" });
		assert.equal(res.topic.topicId, "general");
	});

	it("unknown tool throws UNKNOWN_TOOL", async () => {
		const { client } = makeMockClient();
		await assert.rejects(
			() => dispatch(client, noopSpawn, "bogus", {}),
			(e: unknown) => e instanceof McpClientError && e.code === "UNKNOWN_TOOL",
		);
	});
});
