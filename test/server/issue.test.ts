import { strict as assert } from "node:assert";
import {
	chmodSync,
	mkdtempSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, test } from "node:test";
import {
	createGhCliIssueClient,
	IssueClientError,
} from "../../src/server/issue.js";

describe("createGhCliIssueClient", () => {
	let tmpDir: string;

	beforeEach(() => {
		tmpDir = mkdtempSync(join(tmpdir(), "cc-mb-issue-"));
	});

	afterEach(() => {
		rmSync(tmpDir, { recursive: true, force: true });
	});

	function writeFakeGh(script: string): string {
		const path = join(tmpDir, "fake-gh.cjs");
		writeFileSync(path, `#!/usr/bin/env node\n${script}\n`, "utf8");
		chmodSync(path, 0o755);
		return path;
	}

	test("parses issue URL from stdout on success", async () => {
		const fake = writeFakeGh(
			`process.stdout.write("https://github.com/owner/repo/issues/42\\n");`,
		);
		const client = createGhCliIssueClient({
			repo: "owner/repo",
			command: fake,
		});
		const result = await client.create({
			type: "bug",
			title: "t",
			body: "b",
		});
		assert.equal(result.issueNumber, 42);
		assert.equal(result.url, "https://github.com/owner/repo/issues/42");
	});

	test("throws IssueClientError on non-zero exit and captures stderr", async () => {
		const fake = writeFakeGh(
			`process.stderr.write("auth required\\n"); process.exit(1);`,
		);
		const client = createGhCliIssueClient({
			repo: "owner/repo",
			command: fake,
		});
		await assert.rejects(
			() => client.create({ type: "bug", title: "t", body: "b" }),
			(e: unknown) => {
				assert.ok(e instanceof IssueClientError);
				assert.match(e.message, /exited with code 1/);
				assert.deepEqual(e.details, { stderr: "auth required" });
				return true;
			},
		);
	});

	test("throws IssueClientError with not-found message when binary missing", async () => {
		const client = createGhCliIssueClient({
			repo: "owner/repo",
			command: join(tmpDir, "definitely-not-a-real-binary"),
		});
		await assert.rejects(
			() => client.create({ type: "bug", title: "t", body: "b" }),
			(e: unknown) => {
				assert.ok(e instanceof IssueClientError);
				assert.match(e.message, /not found/);
				return true;
			},
		);
	});

	test("throws when stdout has no parseable URL", async () => {
		const fake = writeFakeGh(`process.stdout.write("no url here\\n");`);
		const client = createGhCliIssueClient({
			repo: "owner/repo",
			command: fake,
		});
		await assert.rejects(
			() => client.create({ type: "bug", title: "t", body: "b" }),
			(e: unknown) => {
				assert.ok(e instanceof IssueClientError);
				assert.match(e.message, /not parseable/);
				return true;
			},
		);
	});

	test("body is piped to stdin (gh --body-file -)", async () => {
		const stdinFile = join(tmpDir, "stdin-capture.txt");
		const script = `
            const data = [];
            process.stdin.on("data", c => data.push(c));
            process.stdin.on("end", () => {
                require("fs").writeFileSync(${JSON.stringify(stdinFile)}, Buffer.concat(data));
                process.stdout.write("https://github.com/owner/repo/issues/7\\n");
            });
        `;
		const fake = writeFakeGh(script);
		const client = createGhCliIssueClient({
			repo: "owner/repo",
			command: fake,
		});
		await client.create({
			type: "feature",
			title: "t",
			body: "hello body content",
		});
		const captured = readFileSync(stdinFile, "utf8");
		assert.equal(captured, "hello body content");
	});

	test("forwards --repo and --title args to gh", async () => {
		const argsFile = join(tmpDir, "args-capture.txt");
		const script = `
            require("fs").writeFileSync(${JSON.stringify(argsFile)}, JSON.stringify(process.argv.slice(2)));
            process.stdin.on("data", () => {});
            process.stdin.on("end", () => {
                process.stdout.write("https://github.com/owner/repo/issues/1\\n");
            });
        `;
		const fake = writeFakeGh(script);
		const client = createGhCliIssueClient({
			repo: "owner/repo",
			command: fake,
		});
		await client.create({
			type: "bug",
			title: "my title",
			body: "x",
		});
		const args = JSON.parse(readFileSync(argsFile, "utf8")) as string[];
		assert.deepEqual(args, [
			"issue",
			"create",
			"--repo",
			"owner/repo",
			"--title",
			"my title",
			"--body-file",
			"-",
		]);
	});
});
