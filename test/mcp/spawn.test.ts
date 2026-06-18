import type { ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, describe, it } from "node:test";
import { fileURLToPath } from "node:url";
import { createBrokerClient } from "../../src/mcp/broker-client.js";
import { ensureBrokerRunning, type SpawnCmd } from "../../src/mcp/spawn.js";

const BASE_URL = "http://127.0.0.1:5959";

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
