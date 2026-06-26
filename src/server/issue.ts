import { spawn } from "node:child_process";
import type { IssueCreateRequest } from "../protocol/http.js";

export interface IssueClientResult {
	issueNumber: number;
	url: string;
}

export interface IssueClient {
	create(req: IssueCreateRequest): Promise<IssueClientResult>;
}

export class IssueClientError extends Error {
	constructor(
		message: string,
		public details?: unknown,
	) {
		super(message);
		this.name = "IssueClientError";
	}
}

const ISSUE_URL_REGEX = /^https?:\/\/[^\s]+\/issues\/(\d+)\s*$/m;

function parseIssueUrl(stdout: string): IssueClientResult {
	const match = stdout.match(ISSUE_URL_REGEX);
	if (!match) {
		throw new IssueClientError(
			"gh issue create succeeded but URL was not parseable",
			{ stdout },
		);
	}
	const url = match[0].trim();
	const issueNumber = Number.parseInt(match[1] as string, 10);
	return { issueNumber, url };
}

export interface GhCliOptions {
	repo: string;
	command?: string;
}

export function createGhCliIssueClient(opts: GhCliOptions): IssueClient {
	const command = opts.command ?? "gh";
	return {
		create(req) {
			return new Promise<IssueClientResult>((resolve, reject) => {
				const child = spawn(
					command,
					[
						"issue",
						"create",
						"--repo",
						opts.repo,
						"--title",
						req.title,
						"--body-file",
						"-",
					],
					{ stdio: ["pipe", "pipe", "pipe"] },
				);
				let stdout = "";
				let stderr = "";
				child.stdout.on("data", (chunk: Buffer) => {
					stdout += chunk.toString("utf8");
				});
				child.stderr.on("data", (chunk: Buffer) => {
					stderr += chunk.toString("utf8");
				});
				child.on("error", (err) => {
					const code = (err as NodeJS.ErrnoException).code;
					if (code === "ENOENT") {
						reject(
							new IssueClientError(
								`'${command}' command not found — install GitHub CLI (gh)`,
							),
						);
						return;
					}
					reject(new IssueClientError(err.message, { code }));
				});
				child.on("close", (exitCode) => {
					if (exitCode !== 0) {
						reject(
							new IssueClientError(
								`gh issue create exited with code ${exitCode}`,
								{ stderr: stderr.trim() },
							),
						);
						return;
					}
					try {
						resolve(parseIssueUrl(stdout));
					} catch (e) {
						reject(e);
					}
				});
				child.stdin.end(req.body);
			});
		},
	};
}
