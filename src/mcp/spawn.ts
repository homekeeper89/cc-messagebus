import { type ChildProcess, spawn } from "node:child_process";
import { type BrokerClient, McpClientError } from "./broker-client.js";

const READY_TIMEOUT_MS = 5000;
const POLL_INTERVAL_MS = 100;

export interface SpawnCmd {
	exe: string;
	args: string[];
	env?: NodeJS.ProcessEnv;
}

export interface EnsureBrokerOptions {
	timeoutMs?: number;
	onSpawn?: (child: ChildProcess) => void;
}

export async function ensureBrokerRunning(
	client: BrokerClient,
	spawnCmd: SpawnCmd,
	opts: EnsureBrokerOptions = {},
): Promise<void> {
	try {
		await client.listPeers();
		return;
	} catch (e) {
		if (!(e instanceof McpClientError && e.code === "BROKER_UNREACHABLE")) {
			throw e;
		}
	}

	// detached + ignore stdio: broker outlives the adapter process; its stdout
	// must not collide with the MCP JSON-RPC stream on stdin/stdout.
	const child = spawn(spawnCmd.exe, spawnCmd.args, {
		detached: true,
		stdio: "ignore",
		env: spawnCmd.env,
	});
	child.unref();
	opts.onSpawn?.(child);

	const timeoutMs = opts.timeoutMs ?? READY_TIMEOUT_MS;
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		try {
			await client.listPeers();
			return;
		} catch {
			await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
		}
	}
	throw new McpClientError(
		"BROKER_SPAWN_TIMEOUT",
		`broker did not become ready within ${timeoutMs}ms`,
	);
}
