import { spawn } from "node:child_process";
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { runDashboard } from "./client/dashboard.js";
import { runTail } from "./client/tail.js";
import { createBrokerClient, McpClientError } from "./mcp/broker-client.js";
import { runMcp } from "./mcp/server.js";
import { createServer } from "./server/index.js";

const DEFAULT_DB_PATH = join(homedir(), ".cc-messagebus", "data.db");
const PID_PATH = join(homedir(), ".cc-messagebus", "broker.pid");
const DEFAULT_BROKER_URL = "http://127.0.0.1:5959";

function writePidFile(): void {
	try {
		mkdirSync(dirname(PID_PATH), { recursive: true });
		writeFileSync(PID_PATH, String(process.pid), "utf8");
	} catch (e) {
		// pidfile 은 best-effort (restart fallback 용). 실패해도 serve 는 계속 진행.
		process.stderr.write(`warning: failed to write pidfile: ${String(e)}\n`);
	}
}

function removePidFile(): void {
	try {
		rmSync(PID_PATH, { force: true });
	} catch {
		// 무시
	}
}

function readPidFile(): number | null {
	try {
		const raw = readFileSync(PID_PATH, "utf8").trim();
		const pid = Number.parseInt(raw, 10);
		return Number.isInteger(pid) && pid > 0 ? pid : null;
	} catch {
		return null;
	}
}

async function runServe(): Promise<void> {
	const server = createServer({
		dbPath: process.env.CC_MESSAGEBUS_DB ?? DEFAULT_DB_PATH,
		logger: true,
		onStopRequest: () => {
			process.stdout.write("received /api/stop, shutting down...\n");
			void gracefulStop("stop-endpoint");
		},
	});

	let stopping = false;
	const gracefulStop = async (reason: string): Promise<void> => {
		if (stopping) return;
		stopping = true;
		try {
			await server.stop();
			removePidFile();
			process.stdout.write(`stopped (${reason})\n`);
			process.exit(0);
		} catch (e) {
			process.stderr.write(`shutdown error: ${String(e)}\n`);
			process.exit(1);
		}
	};

	let address: string;
	try {
		address = await server.start();
	} catch (e) {
		const msg = e instanceof Error ? e.message : String(e);
		// EADDRINUSE 같은 운영 진입점 실패를 사용자에게 명확히 알림
		process.stderr.write(`failed to start cc-messagebus: ${msg}\n`);
		process.exit(1);
	}
	writePidFile();
	process.stdout.write(`cc-messagebus listening on ${address}\n`);

	const onSignal = (signal: NodeJS.Signals): void => {
		process.stdout.write(`\nreceived ${signal}, shutting down...\n`);
		void gracefulStop(signal);
	};

	// once: 재진입 시 server.stop() 두 번 호출되어 close 된 db 에서 throw 방지
	process.once("SIGINT", onSignal);
	process.once("SIGTERM", onSignal);
}

// 살아있는 broker 를 graceful 하게 내리고 포트가 비워질 때까지 대기.
// 1순위: POST /api/stop (안전 종료 — server.stop() → db.close()).
// fallback: pidfile 의 PID 로 SIGTERM (HTTP 가 먹통일 때).
async function stopRunningBroker(baseUrl: string): Promise<boolean> {
	const client = createBrokerClient(baseUrl);
	let stopRequested = false;
	try {
		const res = await client.stop();
		stopRequested = res.stopping;
	} catch (e) {
		if (!(e instanceof McpClientError && e.code === "BROKER_UNREACHABLE")) {
			throw e;
		}
		// broker 가 안 떠 있음 → 내릴 것도 없음
		return false;
	}

	if (!stopRequested) {
		// server 가 onStopRequest 없이 응답만 한 경우(이론상 daemon 에선 발생 안 함) → pidfile fallback
		const pid = readPidFile();
		if (pid != null) {
			try {
				process.kill(pid, "SIGTERM");
			} catch {
				// 이미 죽었을 수 있음
			}
		}
	}

	// 포트가 실제로 비워질 때까지 대기 (listPeers 가 BROKER_UNREACHABLE 낼 때까지)
	const deadline = Date.now() + 5000;
	while (Date.now() < deadline) {
		try {
			await client.listPeers();
			await new Promise((resolve) => setTimeout(resolve, 100));
		} catch (e) {
			if (e instanceof McpClientError && e.code === "BROKER_UNREACHABLE") {
				removePidFile();
				return true;
			}
			throw e;
		}
	}
	process.stderr.write("warning: broker did not stop within 5s\n");
	return false;
}

async function runRestart(): Promise<void> {
	const baseUrl = process.env.CC_MESSAGEBUS_URL ?? DEFAULT_BROKER_URL;
	process.stdout.write("stopping running broker (if any)...\n");
	try {
		const stopped = await stopRunningBroker(baseUrl);
		process.stdout.write(
			stopped ? "old broker stopped\n" : "no running broker to stop\n",
		);
	} catch (e) {
		process.stderr.write(`failed to stop broker: ${String(e)}\n`);
		process.exit(1);
	}

	// 새 broker 를 detached 로 기동해 이 프로세스가 끝나도 살아남게 함.
	const child = spawn(process.execPath, [process.argv[1], "serve"], {
		detached: true,
		stdio: "ignore",
		env: process.env,
	});
	child.unref();
	process.stdout.write("new broker started (detached)\n");
	process.exit(0);
}

function printUsage(): void {
	process.stdout.write(
		[
			"cc-messagebus — cross-session message bus for Claude Code",
			"",
			"Usage:",
			"  cc-messagebus serve              start the broker daemon",
			"  cc-messagebus restart            stop running broker and start a fresh one",
			"  cc-messagebus mcp                MCP stdio adapter (Phase 6)",
			"  cc-messagebus tail <peerId>      poll inbox and print messages",
			"  cc-messagebus status             show broker status (Phase 5)",
			"  cc-messagebus dashboard          open dashboard (Phase 7)",
			"",
			"Env:",
			"  CC_MESSAGEBUS_DB                 sqlite path (default ~/.cc-messagebus/data.db)",
			"  CC_MESSAGEBUS_URL                broker base url for tail (default http://127.0.0.1:5959)",
			"",
		].join("\n"),
	);
}

const subcommand = process.argv[2];
switch (subcommand) {
	case "serve":
		await runServe();
		break;
	case "restart":
		await runRestart();
		break;
	case "tail": {
		const peerId = process.argv[3];
		if (!peerId) {
			process.stderr.write("usage: cc-messagebus tail <peerId>\n");
			process.exit(1);
		}
		await runTail(peerId, { baseUrl: process.env.CC_MESSAGEBUS_URL });
		break;
	}
	case "mcp":
		await runMcp();
		break;
	case "dashboard":
		runDashboard({ baseUrl: process.env.CC_MESSAGEBUS_URL });
		break;
	case "status":
		process.stderr.write(`'${subcommand}' is not implemented yet — Phase 7\n`);
		process.exit(1);
		break;
	default:
		printUsage();
		process.exit(subcommand ? 1 : 0);
}
