import { homedir } from "node:os";
import { join } from "node:path";
import { createServer } from "./server/index.js";

const DEFAULT_DB_PATH = join(homedir(), ".cc-messagebus", "data.db");

async function runServe(): Promise<void> {
	const server = createServer({
		dbPath: process.env.CC_MESSAGEBUS_DB ?? DEFAULT_DB_PATH,
		logger: true,
	});
	const address = await server.start();
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

	process.on("SIGINT", shutdown);
	process.on("SIGTERM", shutdown);
}

function printUsage(): void {
	process.stdout.write(
		[
			"cc-messagebus — cross-session message bus for Claude Code",
			"",
			"Usage:",
			"  cc-messagebus serve              start the broker daemon",
			"  cc-messagebus mcp                MCP stdio adapter (Phase 6)",
			"  cc-messagebus tail <topicId>     tail messages (Phase 5)",
			"  cc-messagebus status             show broker status (Phase 5)",
			"  cc-messagebus dashboard          open dashboard (Phase 7)",
			"",
			"Env:",
			"  CC_MESSAGEBUS_DB                 sqlite path (default ~/.cc-messagebus/data.db)",
			"",
		].join("\n"),
	);
}

const subcommand = process.argv[2];
switch (subcommand) {
	case "serve":
		await runServe();
		break;
	case "mcp":
	case "tail":
	case "status":
	case "dashboard":
	case "stop":
		process.stderr.write(
			`'${subcommand}' is not implemented yet — Phase 5/6/7\n`,
		);
		process.exit(1);
		break;
	default:
		printUsage();
		process.exit(subcommand ? 1 : 0);
}
