import { homedir } from "node:os";
import { join } from "node:path";
import { runTail } from "./client/tail.js";
import { runMcp } from "./mcp/server.js";
import { createServer } from "./server/index.js";

const DEFAULT_DB_PATH = join(homedir(), ".cc-messagebus", "data.db");

async function runServe(): Promise<void> {
	const server = createServer({
		dbPath: process.env.CC_MESSAGEBUS_DB ?? DEFAULT_DB_PATH,
		logger: true,
	});
	let address: string;
	try {
		address = await server.start();
	} catch (e) {
		const msg = e instanceof Error ? e.message : String(e);
		// EADDRINUSE 같은 운영 진입점 실패를 사용자에게 명확히 알림
		process.stderr.write(`failed to start cc-messagebus: ${msg}\n`);
		process.exit(1);
	}
	process.stdout.write(`cc-messagebus listening on ${address}\n`);

	const shutdown = async (signal: NodeJS.Signals): Promise<void> => {
		process.stdout.write(`\nreceived ${signal}, shutting down...\n`);
		try {
			await server.stop();
			process.exit(0);
		} catch (e) {
			process.stderr.write(`shutdown error: ${String(e)}\n`);
			process.exit(1);
		}
	};

	// once: 재진입 시 server.stop() 두 번 호출되어 close 된 db 에서 throw 방지
	process.once("SIGINT", shutdown);
	process.once("SIGTERM", shutdown);
}

function printUsage(): void {
	process.stdout.write(
		[
			"cc-messagebus — cross-session message bus for Claude Code",
			"",
			"Usage:",
			"  cc-messagebus serve              start the broker daemon",
			"  cc-messagebus mcp                MCP stdio adapter (Phase 6)",
			"  cc-messagebus tail <topicId>     subscribe to a topic via SSE",
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
	case "tail": {
		const topicId = process.argv[3];
		if (!topicId) {
			process.stderr.write("usage: cc-messagebus tail <topicId>\n");
			process.exit(1);
		}
		await runTail(topicId, { baseUrl: process.env.CC_MESSAGEBUS_URL });
		break;
	}
	case "mcp":
		await runMcp();
		break;
	case "status":
	case "dashboard":
	case "stop":
		process.stderr.write(`'${subcommand}' is not implemented yet — Phase 7\n`);
		process.exit(1);
		break;
	default:
		printUsage();
		process.exit(subcommand ? 1 : 0);
}
