import { strict as assert } from "node:assert";
import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, test } from "node:test";
import {
	type Broker,
	BrokerError,
	type BrokerOptions,
	createBroker,
	RING_BUFFER_CAPACITY,
} from "../../src/server/broker.js";
import { type CcDatabase, openDatabase } from "../../src/server/db.js";
import { IssueClientError } from "../../src/server/issue.js";

function uniqueDbPath(): string {
	return join(
		tmpdir(),
		`cc-mb-diag-${Date.now()}-${Math.random().toString(36).slice(2)}.db`,
	);
}

describe("broker.diagnostics + ring buffer", () => {
	let dbPath: string;
	let db: CcDatabase;

	beforeEach(() => {
		dbPath = uniqueDbPath();
		db = openDatabase(dbPath);
	});

	afterEach(() => {
		db.close();
		rmSync(dbPath, { force: true });
	});

	function makeBroker(
		overrides: Partial<
			Pick<BrokerOptions, "issueClient" | "getDbSizeByte" | "version">
		> = {},
	): Broker {
		return createBroker(db, {
			visibilityTimeoutSec: 30,
			ttlDays: 30,
			dashboardUrl: "http://localhost:5959",
			version: overrides.version ?? "9.9.9",
			getDbSizeByte: overrides.getDbSizeByte ?? ((): number => 0),
			issueClient: overrides.issueClient ?? null,
		});
	}

	test("snapshot exposes version, node, uptime, counts, dbSize", () => {
		const broker = makeBroker({ getDbSizeByte: () => 4096 });
		broker.register({ peerId: "p1" });
		broker.register({ peerId: "p2" });
		broker.topicCreate({ topicId: "t1", createdBy: "p1" });

		const snap = broker.diagnostics();
		assert.equal(snap.version, "9.9.9");
		assert.equal(snap.nodeVersion, process.version);
		assert.equal(snap.dbSizeByte, 4096);
		assert.equal(snap.peerCount, 2);
		assert.equal(snap.topicCount, 1);
		assert.ok(snap.uptimeSec >= 0);
		assert.ok(Array.isArray(snap.recentRpcList));
		assert.ok(Array.isArray(snap.recentErrorList));
	});

	test("ring buffer caps at RING_BUFFER_CAPACITY (50) entries", () => {
		const broker = makeBroker();
		broker.register({ peerId: "p1" });
		for (let i = 0; i < RING_BUFFER_CAPACITY + 10; i++) {
			broker.listPeers();
		}
		const snap = broker.diagnostics();
		assert.equal(snap.recentRpcList.length, RING_BUFFER_CAPACITY);
		const newest = snap.recentRpcList[snap.recentRpcList.length - 1];
		assert.equal(newest?.method, "listPeers");
		assert.equal(newest?.error, null);
		assert.ok(typeof newest?.durationMs === "number" && newest.durationMs >= 0);
	});

	test("errored RPC is recorded in both rpc and error rings", () => {
		const broker = makeBroker();
		try {
			broker.ack({ peerId: "ghost", messageId: "missing" });
		} catch {
			// expected
		}
		const snap = broker.diagnostics();
		const ackEntry = snap.recentRpcList.find((r) => r.method === "ack");
		assert.ok(ackEntry, "ack rpc must be recorded");
		assert.ok(ackEntry.error, "ack rpc must carry error string");
		assert.ok(
			snap.recentErrorList.length >= 1,
			"error ring must capture the failure",
		);
	});

	test("diagnostics itself is not recorded in rpc ring (no self-noise)", () => {
		const broker = makeBroker();
		broker.diagnostics();
		broker.diagnostics();
		const snap = broker.diagnostics();
		assert.ok(
			snap.recentRpcList.every((r) => r.method !== "diagnostics"),
			"diagnostics calls must not pollute the ring",
		);
	});

	test("issueCreate without issueClient throws ISSUE_REPO_NOT_CONFIGURED", async () => {
		const broker = makeBroker({ issueClient: null });
		await assert.rejects(
			() => broker.issueCreate({ type: "bug", title: "t", body: "b" }),
			(e: unknown) => {
				assert.ok(e instanceof BrokerError);
				assert.equal(e.code, "ISSUE_REPO_NOT_CONFIGURED");
				return true;
			},
		);
	});

	test("issueCreate forwards to issueClient and returns issueNumber + url", async () => {
		const broker = makeBroker({
			issueClient: {
				create: async () => ({
					issueNumber: 7,
					url: "https://github.com/x/y/issues/7",
				}),
			},
		});
		const result = await broker.issueCreate({
			type: "feature",
			title: "t",
			body: "b",
		});
		assert.equal(result.issueNumber, 7);
		assert.equal(result.url, "https://github.com/x/y/issues/7");
	});

	test("issueCreate wraps IssueClientError as ISSUE_CLIENT_FAILED with details", async () => {
		const broker = makeBroker({
			issueClient: {
				create: async () => {
					throw new IssueClientError("gh not found", { code: "ENOENT" });
				},
			},
		});
		await assert.rejects(
			() => broker.issueCreate({ type: "bug", title: "t", body: "b" }),
			(e: unknown) => {
				assert.ok(e instanceof BrokerError);
				assert.equal(e.code, "ISSUE_CLIENT_FAILED");
				assert.deepEqual(e.details, { code: "ENOENT" });
				return true;
			},
		);
	});
});
