import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

export interface ServerConfig {
	issueRepo: string | null;
}

const DEFAULT_CONFIG: ServerConfig = { issueRepo: null };

export function loadConfig(home: string = homedir()): ServerConfig {
	const path = join(home, ".cc-messagebus", "config.json");
	let raw: string;
	try {
		raw = readFileSync(path, "utf8");
	} catch (e) {
		if ((e as NodeJS.ErrnoException).code === "ENOENT") return DEFAULT_CONFIG;
		throw e;
	}
	const parsed = JSON.parse(raw) as Partial<ServerConfig>;
	return {
		issueRepo:
			typeof parsed.issueRepo === "string" && parsed.issueRepo.length > 0
				? parsed.issueRepo
				: null,
	};
}
