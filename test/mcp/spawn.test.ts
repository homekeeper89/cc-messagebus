import assert from "node:assert/strict";
import type { ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, describe, it } from "node:test";
import { fileURLToPath } from "node:url";
import {
	type BrokerClient,
	createBrokerClient,
	McpClientError,
} from "../../src/mcp/broker-client.js";
import { ensureBrokerRunning, type SpawnCmd } from "../../src/mcp/spawn.js";

const BASE_URL = "http://127.0.0.1:5959";
const NOOP_SPAWN: SpawnCmd = { exe: "/bin/true", args: [] };

// 버전 협상 분기를 실제 프로세스 없이 검증하기 위한 fake broker client.
// alive 플래그로 listPeers 응답(살아있음/BROKER_UNREACHABLE)을 제어한다.
function makeFakeClient(opts: {
	initiallyAlive: boolean;
	version?: string;
	serverInfoThrows?: boolean;
}): {
	client: BrokerClient;
	calls: { serverInfo: number; stop: number };
	state: { alive: boolean };
} {
	const state = { alive: opts.initiallyAlive };
	const calls = {
		serverInfo: 0,
		stop: 0,
	};
	const unreachable = () =>
		new McpClientError("BROKER_UNREACHABLE", "fake: not running");
	const base = {
		listPeers: async () => {
			if (!state.alive) throw unreachable();
			return { peers: [] };
		},
		serverInfo: async () => {
			calls.serverInfo += 1;
			if (!state.alive) throw unreachable();
			if (opts.serverInfoThrows) throw new McpClientError("X", "boom");
			return { issueRepo: null, version: opts.version ?? "0.0.0" };
		},
		stop: async () => {
			calls.stop += 1;
			if (!state.alive) throw unreachable();
			state.alive = false; // stop 즉시 포트가 비워지는 것으로 시뮬레이션
			return { stopping: true, version: opts.version ?? "0.0.0" };
		},
	} as unknown as BrokerClient;
	return { client: base, calls, state };
}

async function isPortFree(): Promise<boolean> {
	try {
		await fetch(`${BASE_URL}/api/list_peers`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: "{}",
		});
		return false;
	} catch {
		return true;
	}
}

describe("mcp/spawn.ensureBrokerRunning", () => {
	let tmpDir: string;
	let dbPath: string;
	let spawned: ChildProcess | null = null;
	let portWasFree = false;

	before(async () => {
		portWasFree = await isPortFree();
		tmpDir = mkdtempSync(join(tmpdir(), "ccmb-spawn-"));
		dbPath = join(tmpDir, "data.db");
	});

	after(async () => {
		if (spawned?.pid) {
			try {
				process.kill(spawned.pid, "SIGTERM");
			} catch {
				// already gone
			}
			await new Promise((r) => setTimeout(r, 200));
		}
		rmSync(tmpDir, { recursive: true, force: true });
	});

	it("spawns broker and waits for ready when port is free", async (t) => {
		if (!portWasFree) {
			t.skip("port 5959 already in use — skipping integration test");
			return;
		}
		const client = createBrokerClient(BASE_URL);
		const cliPath = fileURLToPath(new URL("../../src/cli.ts", import.meta.url));
		const spawnCmd: SpawnCmd = {
			exe: process.execPath,
			args: ["--import", "tsx", cliPath, "serve"],
			env: {
				...process.env,
				CC_MESSAGEBUS_DB: dbPath,
			},
		};
		await ensureBrokerRunning(client, spawnCmd, {
			timeoutMs: 10_000,
			onSpawn: (child) => {
				spawned = child;
			},
		});
		const res = await client.listPeers();
		if (res.peers.length !== 0) {
			throw new Error(
				`expected 0 peers from fresh db, got ${res.peers.length}`,
			);
		}
	});
});

describe("mcp/spawn version negotiation (fake client)", () => {
	it("reuses running broker when version matches expectedVersion", async () => {
		const { client, calls } = makeFakeClient({
			initiallyAlive: true,
			version: "0.6.5",
		});
		let spawnCalled = false;
		await ensureBrokerRunning(client, NOOP_SPAWN, {
			expectedVersion: "0.6.5",
			spawnImpl: () => {
				spawnCalled = true;
			},
		});
		assert.equal(calls.serverInfo, 1);
		assert.equal(calls.stop, 0, "must not stop a matching-version broker");
		assert.equal(spawnCalled, false, "must not respawn when version matches");
	});

	it("stops and respawns when running broker version differs", async () => {
		const { client, calls, state } = makeFakeClient({
			initiallyAlive: true,
			version: "0.6.2", // 옛 버전
		});
		let spawnCalled = false;
		await ensureBrokerRunning(client, NOOP_SPAWN, {
			expectedVersion: "0.6.5",
			timeoutMs: 2000,
			spawnImpl: () => {
				spawnCalled = true;
				// 새 broker 가 떠서 ready 가 된 것으로 시뮬레이션
				state.alive = true;
			},
		});
		assert.equal(calls.stop, 1, "must stop the stale broker");
		assert.equal(spawnCalled, true, "must respawn a new broker");
	});

	it("stops and respawns when serverInfo is unavailable (old broker)", async () => {
		const { client, calls, state } = makeFakeClient({
			initiallyAlive: true,
			serverInfoThrows: true,
		});
		let spawnCalled = false;
		await ensureBrokerRunning(client, NOOP_SPAWN, {
			expectedVersion: "0.6.5",
			timeoutMs: 2000,
			spawnImpl: () => {
				spawnCalled = true;
				state.alive = true;
			},
		});
		assert.equal(calls.stop, 1);
		assert.equal(spawnCalled, true);
	});

	it("spawns directly when no broker is running (no version check)", async () => {
		const { client, calls, state } = makeFakeClient({ initiallyAlive: false });
		let spawnCalled = false;
		await ensureBrokerRunning(client, NOOP_SPAWN, {
			expectedVersion: "0.6.5",
			timeoutMs: 2000,
			spawnImpl: () => {
				spawnCalled = true;
				state.alive = true;
			},
		});
		assert.equal(calls.serverInfo, 0, "no live broker → no version check");
		assert.equal(calls.stop, 0);
		assert.equal(spawnCalled, true);
	});
});
