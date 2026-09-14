import { type ChildProcess, spawn } from "node:child_process";
import { type BrokerClient, McpClientError } from "./broker-client.js";

const READY_TIMEOUT_MS = 5000;
const POLL_INTERVAL_MS = 100;
const STOP_TIMEOUT_MS = 5000;

export interface SpawnCmd {
	exe: string;
	args: string[];
	env?: NodeJS.ProcessEnv;
}

export interface EnsureBrokerOptions {
	timeoutMs?: number;
	onSpawn?: (child: ChildProcess) => void;
	// 주어지면 살아있는 broker 의 버전과 대조 → 불일치 시 stop 후 respawn (자동 업그레이드).
	// 미주입 시 버전 무시하고 살아있으면 그대로 재사용 (기존 동작).
	expectedVersion?: string;
	// 테스트 전용: 실제 자식 프로세스 생성을 대체. 미주입 시 실제 detached spawn.
	spawnImpl?: (spawnCmd: SpawnCmd) => void;
}

function spawnDetached(
	spawnCmd: SpawnCmd,
	onSpawn?: (child: ChildProcess) => void,
): void {
	// detached + ignore stdio: broker outlives the adapter process; its stdout
	// must not collide with the MCP JSON-RPC stream on stdin/stdout.
	const child = spawn(spawnCmd.exe, spawnCmd.args, {
		detached: true,
		stdio: "ignore",
		env: spawnCmd.env,
	});
	child.unref();
	onSpawn?.(child);
}

async function waitForReady(
	client: BrokerClient,
	timeoutMs: number,
): Promise<void> {
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

// 살아있는 broker 를 stop 시키고 포트가 실제로 비워질 때까지(= listPeers 실패) 대기.
async function stopAndWaitDown(
	client: BrokerClient,
	timeoutMs: number,
): Promise<void> {
	try {
		await client.stop();
	} catch (e) {
		// 이미 죽어가는 중이면 연결이 끊겨 BROKER_UNREACHABLE 이 날 수 있다 — 정상 경로로 취급.
		if (!(e instanceof McpClientError && e.code === "BROKER_UNREACHABLE")) {
			throw e;
		}
	}
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		try {
			await client.listPeers();
			// 아직 응답함 → 종료 진행 중, 계속 대기
			await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
		} catch (e) {
			if (e instanceof McpClientError && e.code === "BROKER_UNREACHABLE") {
				return; // 포트가 비워짐
			}
			throw e;
		}
	}
	throw new McpClientError(
		"BROKER_STOP_TIMEOUT",
		`broker did not stop within ${timeoutMs}ms`,
	);
}

export async function ensureBrokerRunning(
	client: BrokerClient,
	spawnCmd: SpawnCmd,
	opts: EnsureBrokerOptions = {},
): Promise<void> {
	const timeoutMs = opts.timeoutMs ?? READY_TIMEOUT_MS;

	// 1) 살아있는 broker 가 있는지 확인
	let alive = false;
	try {
		await client.listPeers();
		alive = true;
	} catch (e) {
		if (!(e instanceof McpClientError && e.code === "BROKER_UNREACHABLE")) {
			throw e;
		}
	}

	if (alive) {
		// 버전 협상: expectedVersion 미주입이면 그대로 재사용 (기존 동작 유지)
		if (opts.expectedVersion == null) return;
		let runningVersion: string | null = null;
		try {
			runningVersion = (await client.serverInfo()).version;
		} catch {
			// 구버전 broker 가 server_info 를 모르거나 응답 실패 → 안전하게 교체 시도
			runningVersion = null;
		}
		if (runningVersion === opts.expectedVersion) return;
		// 버전 불일치(또는 확인 불가) → stop 후 아래에서 respawn
		await stopAndWaitDown(client, STOP_TIMEOUT_MS);
	}

	// 2) 새 broker 기동 후 ready 대기
	if (opts.spawnImpl) {
		opts.spawnImpl(spawnCmd);
	} else {
		spawnDetached(spawnCmd, opts.onSpawn);
	}
	await waitForReady(client, timeoutMs);
}
